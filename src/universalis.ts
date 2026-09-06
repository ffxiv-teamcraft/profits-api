import {Agent} from 'https';
import axios, {AxiosError, AxiosInstance} from 'axios';

/**
 * Nombre de connexions simultanees vers Universalis, et debit cible en requetes/seconde.
 * Le debit reel est pilote par le token bucket ci-dessous : il ne depend pas de la latence,
 * contrairement a un simple delay() couple a une concurrence fixe.
 */
const CONCURRENCY = Number(process.env.UNIVERSALIS_CONCURRENCY || 8);
const RATE_PER_SEC = Number(process.env.UNIVERSALIS_RPS || 20);
const REQUEST_TIMEOUT = 20000;
const MAX_ATTEMPTS = 5;
const TOTAL_BUDGET_MS = 90000;

/**
 * Interface plate plutot qu'union discriminee : le projet compile avec strictNullChecks
 * desactive, ou le narrowing sur `ok` ne fonctionne pas.
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
    // keep-alive : evite un handshake TLS par requete (~20k requetes par cycle)
    httpsAgent: new Agent({keepAlive: true, maxSockets: CONCURRENCY, keepAliveMsecs: 30000})
});

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Token bucket : lisse le debit sur la duree, avec un petit burst autorise.
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
 * Borne le nombre de requetes en vol. Une requete lente ne peut plus confisquer
 * un slot indefiniment : le timeout axios et le budget total y veillent.
 */
class Semaphore {
    private active = 0;
    private readonly waiting: (() => void)[] = [];

    constructor(private readonly limit: number) {
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
 * Coupe-circuit progressif : si Universalis souffre, on ralentit tout seul,
 * puis on remonte doucement des que ca repasse.
 */
let consecutiveFailures = 0;

function onFailure(): void {
    consecutiveFailures++;
    if (consecutiveFailures % 10 === 0) {
        limiter.setRate(Math.max(2, limiter.currentRate * 0.5));
        console.warn(`[universalis] ${consecutiveFailures} echecs consecutifs, debit reduit a ${limiter.currentRate.toFixed(1)} req/s`);
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
        // timeout, ECONNRESET, DNS... : transitoire
        return {retryable: true, status};
    }
    if (status === 429) {
        const retryAfter = Number(err.response.headers ? err.response.headers['retry-after'] : NaN);
        return {retryable: true, status, waitMs: isFinite(retryAfter) ? retryAfter * 1000 : undefined};
    }
    if (status >= 500) {
        return {retryable: true, status};
    }
    // 400 / 404 : le monde ou l'item n'existe pas, retenter est inutile
    return {retryable: false, status};
}

/**
 * Ne rejette jamais et emet toujours exactement un resultat : c'est ce qui empeche
 * un combineLatest en aval de rester bloque indefiniment sur une requete morte.
 */
export async function universalisGet<T = any>(url: string, errors$?: ErrorSink): Promise<UniversalisResult<T>> {
    const deadline = Date.now() + TOTAL_BUDGET_MS;
    let lastStatus: number | null = null;
    let lastReason = 'unknown error';

    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
        await limiter.take();
        await gate.acquire();
        try {
            const res = await http.get<T>(url);
            onSuccess();
            return {ok: true, data: res.data};
        } catch (e) {
            const err = e as AxiosError;
            const {retryable, status, waitMs} = classify(err);
            lastStatus = status;
            lastReason = `[${status !== null ? status : err.code}] ${err.message}`;
            onFailure();
            if (!retryable || attempt === MAX_ATTEMPTS || Date.now() > deadline) {
                break;
            }
            // backoff exponentiel plafonne, avec jitter complet pour desynchroniser les reprises
            const base = Math.min(30000, 500 * Math.pow(2, attempt));
            await sleep(waitMs !== undefined ? waitMs : Math.random() * base);
        } finally {
            gate.release();
        }
    }

    console.error(`${lastReason}\n${url}`);
    // un seul signalement par requete, et non un par tentative
    if (errors$) {
        errors$.next({source: `[Universalis] ${url}`, message: lastReason});
    }
    return {ok: false, status: lastStatus, reason: lastReason};
}
