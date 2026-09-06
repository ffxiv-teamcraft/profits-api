import {Agent} from 'https';
import axios, {AxiosError, AxiosInstance} from 'axios';

/**
 * Simultaneous connections to Universalis, and target throughput in requests/second.
 * Actual throughput is driven by the token bucket below, so it does not depend on
 * latency the way a plain delay() paired with a fixed concurrency does.
 */
const CONCURRENCY = Number(process.env.UNIVERSALIS_CONCURRENCY || 8);
const RATE_PER_SEC = Number(process.env.UNIVERSALIS_RPS || 20);
const REQUEST_TIMEOUT = 20000;
const MAX_ATTEMPTS = 5;
const TOTAL_BUDGET_MS = 90000;

/**
 * A flat interface rather than a discriminated union: the project compiles with
 * strictNullChecks off, where narrowing on `ok` does not work.
 */
export interface UniversalisResult<T> {
    ok: boolean;
    data?: T;
    status?: number | null;
    reason?: string;
}

export interface ErrorSink {
    next: (error: { source: string, message: string }) => void;
}

const http: AxiosInstance = axios.create({
    timeout: REQUEST_TIMEOUT,
    decompress: true,
    headers: {'User-Agent': 'FFXIV Teamcraft Profits Helper'},
    // keep-alive: avoids one TLS handshake per request (~40k requests per cycle)
    httpsAgent: new Agent({keepAlive: true, maxSockets: CONCURRENCY, keepAliveMsecs: 30000})
});

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Token bucket: smooths throughput over time, with a small burst allowance.
 */
class RateLimiter {
    private tokens: number;
    private last = Date.now();

    constructor(private rate: number, private readonly burst = rate) {
        this.tokens = burst;
    }

    get currentRate(): number {
        return this.rate;
    }

    setRate(rate: number): void {
        this.rate = Math.max(1, rate);
    }

    async take(): Promise<void> {
        for (; ;) {
            const now = Date.now();
            this.tokens = Math.min(this.burst, this.tokens + ((now - this.last) / 1000) * this.rate);
            this.last = now;
            if (this.tokens >= 1) {
                this.tokens -= 1;
                return;
            }
            await sleep(Math.ceil(((1 - this.tokens) / this.rate) * 1000));
        }
    }
}

/**
 * Bounds the number of in-flight requests. A slow request can no longer hold a slot
 * indefinitely: the axios timeout and the total budget take care of that.
 */
class Semaphore {
    private active = 0;
    private readonly waiting: (() => void)[] = [];

    constructor(private readonly limit: number) {
    }

    get inFlight(): number {
        return this.active;
    }

    get queued(): number {
        return this.waiting.length;
    }

    async acquire(): Promise<void> {
        if (this.active < this.limit) {
            this.active++;
            return;
        }
        await new Promise<void>(resolve => this.waiting.push(resolve));
        this.active++;
    }

    release(): void {
        this.active--;
        const next = this.waiting.shift();
        if (next) {
            next();
        }
    }
}

const limiter = new RateLimiter(RATE_PER_SEC);
const gate = new Semaphore(CONCURRENCY);

/**
 * Counters for the current cycle. They feed the Discord report, which is what removes
 * the need to open the logs to know whether a cycle is going well.
 *
 * Failed attempts (noise: a 503 retried successfully needs no action) are kept separate
 * from definitive abandons (actionable: data is missing).
 */
export interface UniversalisSnapshot {
    requests: number;
    succeeded: number;
    abandoned: number;
    attemptFailures: number;
    attemptsByStatus: Record<string, number>;
    abandonsByStatus: Record<string, number>;
    sampleFailures: string[];
    currentRate: number;
    configuredRate: number;
    inFlight: number;
    queued: number;
}

const stats = {
    requests: 0,
    succeeded: 0,
    abandoned: 0,
    attemptFailures: 0,
    attemptsByStatus: {} as Record<string, number>,
    abandonsByStatus: {} as Record<string, number>,
    sampleFailures: [] as string[]
};

function bump(bucket: Record<string, number>, key: string): void {
    bucket[key] = (bucket[key] || 0) + 1;
}

export function snapshotStats(): UniversalisSnapshot {
    return {
        requests: stats.requests,
        succeeded: stats.succeeded,
        abandoned: stats.abandoned,
        attemptFailures: stats.attemptFailures,
        attemptsByStatus: {...stats.attemptsByStatus},
        abandonsByStatus: {...stats.abandonsByStatus},
        sampleFailures: stats.sampleFailures.slice(),
        currentRate: limiter.currentRate,
        configuredRate: RATE_PER_SEC,
        inFlight: gate.inFlight,
        queued: gate.queued
    };
}

export function resetStats(): void {
    stats.requests = 0;
    stats.succeeded = 0;
    stats.abandoned = 0;
    stats.attemptFailures = 0;
    stats.attemptsByStatus = {};
    stats.abandonsByStatus = {};
    stats.sampleFailures = [];
}

/** Target throughput, used to estimate cycle duration before anything is measured. */
export function getConfiguredRate(): number {
    return RATE_PER_SEC;
}

/**
 * Gradual circuit breaker: if Universalis struggles we slow down on our own, then
 * ramp back up once it recovers.
 */
let consecutiveFailures = 0;

function onFailure(): void {
    consecutiveFailures++;
    if (consecutiveFailures % 10 === 0) {
        limiter.setRate(Math.max(2, limiter.currentRate * 0.5));
        console.warn(`[universalis] ${consecutiveFailures} consecutive failures, throughput reduced to ${limiter.currentRate.toFixed(1)} req/s`);
    }
}

function onSuccess(): void {
    if (consecutiveFailures > 0) {
        consecutiveFailures = 0;
    }
    if (limiter.currentRate < RATE_PER_SEC) {
        limiter.setRate(Math.min(RATE_PER_SEC, limiter.currentRate * 1.1));
    }
}

function classify(err: AxiosError): { retryable: boolean, status: number | null, waitMs?: number } {
    const status = err.response ? err.response.status : null;
    if (status === null) {
        // timeout, ECONNRESET, DNS... : transient
        return {retryable: true, status};
    }
    if (status === 429) {
        const retryAfter = Number(err.response.headers ? err.response.headers['retry-after'] : NaN);
        return {retryable: true, status, waitMs: isFinite(retryAfter) ? retryAfter * 1000 : undefined};
    }
    if (status >= 500) {
        return {retryable: true, status};
    }
    // 400 / 404: the world or item does not exist, retrying is pointless
    return {retryable: false, status};
}

/**
 * Never rejects and always yields exactly one result: this is what stops a downstream
 * combineLatest from hanging forever on a dead request.
 */
export async function universalisGet<T = any>(url: string, errors$?: ErrorSink): Promise<UniversalisResult<T>> {
    const deadline = Date.now() + TOTAL_BUDGET_MS;
    let lastStatus: number | null = null;
    let lastReason = 'unknown error';
    let lastLabel = 'unknown';
    stats.requests++;

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        await limiter.take();
        await gate.acquire();
        try {
            const res = await http.get<T>(url);
            onSuccess();
            stats.succeeded++;
            return {ok: true, data: res.data};
        } catch (e) {
            const err = e as AxiosError;
            const {retryable, status, waitMs} = classify(err);
            lastStatus = status;
            lastLabel = String(status !== null ? status : err.code || 'network');
            lastReason = `[${lastLabel}] ${err.message}`;
            onFailure();
            stats.attemptFailures++;
            bump(stats.attemptsByStatus, lastLabel);
            if (!retryable || attempt === MAX_ATTEMPTS || Date.now() > deadline) {
                break;
            }
            // capped exponential backoff, with full jitter to desynchronise retries
            const base = Math.min(30000, 500 * Math.pow(2, attempt));
            await sleep(waitMs !== undefined ? waitMs : Math.random() * base);
        } finally {
            gate.release();
        }
    }

    stats.abandoned++;
    bump(stats.abandonsByStatus, lastLabel);
    if (stats.sampleFailures.length < 5) {
        stats.sampleFailures.push(`[${lastLabel}] ${url.slice(0, 110)}`);
    }

    console.error(`${lastReason}\n${url}`);
    // one report per request, not one per attempt
    if (errors$) {
        errors$.next({source: `[Universalis] ${url}`, message: lastReason});
    }
    return {ok: false, status: lastStatus, reason: lastReason};
}
