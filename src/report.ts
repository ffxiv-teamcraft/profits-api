import {COLOR_ERROR, COLOR_OK, COLOR_RUNNING, COLOR_WARN, DiscordEmbed, formatDuration, progressBar, relativeTime} from './discord';
import {UniversalisSnapshot} from './universalis';

export interface ServerRunResult {
    server: string;
    success: boolean;
    failedChunks: number;
    time: number;
    items: number;
    error?: string;
}

export interface CycleState {
    startedAt: number;
    serversTotal: number;
    serversDone: number;
    chunksTotal: number;
    chunksDone: number;
    currentServer: string;
    estimateMs: number;
    estimateFromHistory: boolean;
    results: ServerRunResult[];
    messageId: string | null;
    editFailures: number;
}

/** Discord limits: 1024 characters per field, 6000 for the whole embed. */
const FIELD_LIMIT = 1024;

/**
 * A priori estimate, used until a cycle has actually been measured. The limiting
 * factor is the token bucket: two requests per chunk, capped at the target rate.
 * The previous formula (a hardcoded 180s per server) predated the API work and
 * announced ~6 hours for a cycle that now takes a fraction of that.
 */
export function aprioriCycleMs(chunksTotal: number, serversTotal: number, ratePerSec: number): number {
    const httpMs = (chunksTotal * 2 / Math.max(1, ratePerSec)) * 1000;
    // cache recomputation plus batched writes, roughly a second per server
    const redisMs = serversTotal * 1500;
    return Math.round(httpMs + redisMs);
}

/**
 * Remaining time. As soon as one chunk is done we extrapolate from the rate the
 * cycle is actually running at, which absorbs a slow Universalis within seconds
 * instead of waiting for the next cycle.
 */
export function remainingMs(state: CycleState, now: number = Date.now()): number {
    const elapsed = now - state.startedAt;
    if (state.chunksDone > 0) {
        return Math.max(0, (elapsed / state.chunksDone) * (state.chunksTotal - state.chunksDone));
    }
    return Math.max(0, state.estimateMs - elapsed);
}

function breakdown(bucket: Record<string, number>): string {
    return Object.entries(bucket)
        .sort((a, b) => b[1] - a[1])
        .map(([key, count]) => `${key}×${count}`)
        .join(', ');
}

export function incidentSummary(snap: UniversalisSnapshot, failed: ServerRunResult[]): string {
    const lines: string[] = [];
    if (snap.abandoned > 0) {
        lines.push(`**${snap.abandoned}** request(s) abandoned — ${breakdown(snap.abandonsByStatus)}`);
    }
    if (snap.attemptFailures > snap.abandoned) {
        lines.push(`${snap.attemptFailures} failed attempt(s), recovered on retry — ${breakdown(snap.attemptsByStatus)}`);
    }
    if (snap.currentRate < snap.configuredRate) {
        lines.push(`Throughput automatically reduced: ${snap.currentRate.toFixed(1)} / ${snap.configuredRate} req/s`);
    }
    if (failed.length > 0) {
        const names = failed.slice(0, 12).map(r => r.server).join(', ');
        const extra = failed.length > 12 ? ` (+${failed.length - 12} more)` : '';
        lines.push(`Incomplete servers: ${names}${extra}`);
    }
    return lines.join('\n').slice(0, FIELD_LIMIT) || 'None';
}

export function runningEmbed(state: CycleState, snap: UniversalisSnapshot, now: number = Date.now()): DiscordEmbed {
    const elapsed = now - state.startedAt;
    const remaining = remainingMs(state, now);
    const failed = state.results.filter(r => !r.success);
    const degraded = snap.abandoned > 0 || failed.length > 0;

    const fields = [
        {name: 'Servers', value: `${state.serversDone} / ${state.serversTotal}`, inline: true},
        {name: 'Chunks', value: `${state.chunksDone} / ${state.chunksTotal}`, inline: true},
        {name: 'Throughput', value: `${snap.currentRate.toFixed(1)} req/s`, inline: true},
        {name: 'Elapsed', value: formatDuration(elapsed), inline: true},
        {name: 'Remaining', value: formatDuration(remaining), inline: true},
        {name: 'Requests OK', value: `${snap.succeeded} / ${snap.requests}`, inline: true}
    ];
    if (degraded) {
        fields.push({name: 'Incidents', value: incidentSummary(snap, failed), inline: false});
    }

    return {
        title: 'Full update in progress',
        color: degraded ? COLOR_WARN : COLOR_RUNNING,
        description: `${progressBar(state.chunksDone / Math.max(1, state.chunksTotal))}\n`
            + `Expected to finish ${relativeTime(now + remaining)}`,
        fields,
        footer: {
            text: state.serversDone === 0
                ? `Estimate ${state.estimateFromHistory ? 'based on previous cycles' : 'theoretical (first cycle)'}`
                : `Current server: ${state.currentServer}`
        }
    };
}

export function finalEmbed(state: CycleState, snap: UniversalisSnapshot, nextRunAt: number,
                           now: number = Date.now()): DiscordEmbed {
    const duration = now - state.startedAt;
    const failed = state.results.filter(r => !r.success);
    const success = failed.length === 0;
    const slowest = state.results.slice().sort((a, b) => b.time - a.time)[0];
    const itemsWritten = state.results.reduce((acc, r) => acc + r.items, 0);

    const fields = [
        {name: 'Duration', value: formatDuration(duration), inline: true},
        {name: 'Avg per server', value: formatDuration(duration / Math.max(1, state.results.length)), inline: true},
        {name: 'Slowest', value: slowest ? `${slowest.server} · ${formatDuration(slowest.time)}` : '—', inline: true},
        {name: 'Requests OK', value: `${snap.succeeded} / ${snap.requests}`, inline: true},
        {name: 'Entries written', value: `${itemsWritten}`, inline: true},
        {name: 'Next cycle', value: relativeTime(nextRunAt), inline: true}
    ];
    if (!success || snap.abandoned > 0) {
        fields.push({name: 'Incidents', value: incidentSummary(snap, failed), inline: false});
    }

    return {
        title: success ? 'Full update complete' : 'Full update complete, with gaps',
        color: success ? COLOR_OK : COLOR_ERROR,
        description: progressBar(1),
        fields,
        footer: {text: `${state.serversTotal} servers · ${state.chunksTotal} chunks`}
    };
}

/**
 * A cycle only deserves its own ping when it is actionable: editing an embed fires
 * no Discord notification, so a dedicated message is posted instead.
 */
export function isDegraded(state: CycleState, snap: UniversalisSnapshot): boolean {
    const failed = state.results.filter(r => !r.success).length;
    const manyServers = failed * 10 > state.serversTotal;
    const manyAbandons = snap.requests > 0 && snap.abandoned * 20 > snap.requests;
    return manyServers || manyAbandons;
}
