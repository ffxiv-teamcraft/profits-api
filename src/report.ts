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

/** Limites Discord : 1024 caracteres par champ, 6000 pour l'embed entier. */
const FIELD_LIMIT = 1024;

/**
 * Estimation a priori, utilisee tant qu'aucun cycle n'a ete mesure. Le facteur
 * limitant est le token bucket : deux requetes par chunk, plafonnees au debit cible.
 * L'ancienne formule (180 s par serveur, en dur) datait d'avant l'optimisation et
 * annoncait ~6 h pour un cycle qui en prend une fraction.
 */
export function aprioriCycleMs(chunksTotal: number, serversTotal: number, ratePerSec: number): number {
    const httpMs = (chunksTotal * 2 / Math.max(1, ratePerSec)) * 1000;
    // recalcul du cache + ecritures groupees, de l'ordre de la seconde par serveur
    const redisMs = serversTotal * 1500;
    return Math.round(httpMs + redisMs);
}

/**
 * Temps restant. Des qu'un chunk est passe on extrapole sur le rythme reel du cycle
 * en cours, ce qui absorbe un Universalis lent sans attendre le cycle suivant.
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
        lines.push(`**${snap.abandoned}** requête(s) abandonnée(s) — ${breakdown(snap.abandonsByStatus)}`);
    }
    if (snap.attemptFailures > snap.abandoned) {
        lines.push(`${snap.attemptFailures} tentative(s) en échec puis reprises — ${breakdown(snap.attemptsByStatus)}`);
    }
    if (snap.currentRate < snap.configuredRate) {
        lines.push(`Débit réduit automatiquement : ${snap.currentRate.toFixed(1)} / ${snap.configuredRate} req/s`);
    }
    if (failed.length > 0) {
        const names = failed.slice(0, 12).map(r => r.server).join(', ');
        const extra = failed.length > 12 ? ` (+${failed.length - 12} autres)` : '';
        lines.push(`Serveurs incomplets : ${names}${extra}`);
    }
    return lines.join('\n').slice(0, FIELD_LIMIT) || 'Aucun';
}

export function runningEmbed(state: CycleState, snap: UniversalisSnapshot, now: number = Date.now()): DiscordEmbed {
    const elapsed = now - state.startedAt;
    const remaining = remainingMs(state, now);
    const failed = state.results.filter(r => !r.success);
    const degraded = snap.abandoned > 0 || failed.length > 0;

    const fields = [
        {name: 'Serveurs', value: `${state.serversDone} / ${state.serversTotal}`, inline: true},
        {name: 'Chunks', value: `${state.chunksDone} / ${state.chunksTotal}`, inline: true},
        {name: 'Débit', value: `${snap.currentRate.toFixed(1)} req/s`, inline: true},
        {name: 'Écoulé', value: formatDuration(elapsed), inline: true},
        {name: 'Restant', value: formatDuration(remaining), inline: true},
        {name: 'Requêtes OK', value: `${snap.succeeded} / ${snap.requests}`, inline: true}
    ];
    if (degraded) {
        fields.push({name: 'Incidents', value: incidentSummary(snap, failed), inline: false});
    }

    return {
        title: 'Mise à jour en cours',
        color: degraded ? COLOR_WARN : COLOR_RUNNING,
        description: `${progressBar(state.chunksDone / Math.max(1, state.chunksTotal))}\n`
            + `Fin estimée ${relativeTime(now + remaining)}`,
        fields,
        footer: {
            text: state.serversDone === 0
                ? `Estimation ${state.estimateFromHistory ? 'basée sur les cycles précédents' : 'théorique (premier cycle)'}`
                : `Serveur courant : ${state.currentServer}`
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
        {name: 'Durée', value: formatDuration(duration), inline: true},
        {name: 'Moy. / serveur', value: formatDuration(duration / Math.max(1, state.results.length)), inline: true},
        {name: 'Plus lent', value: slowest ? `${slowest.server} · ${formatDuration(slowest.time)}` : '—', inline: true},
        {name: 'Requêtes OK', value: `${snap.succeeded} / ${snap.requests}`, inline: true},
        {name: 'Entrées écrites', value: `${itemsWritten}`, inline: true},
        {name: 'Prochain cycle', value: relativeTime(nextRunAt), inline: true}
    ];
    if (!success || snap.abandoned > 0) {
        fields.push({name: 'Incidents', value: incidentSummary(snap, failed), inline: false});
    }

    return {
        title: success ? 'Mise à jour terminée' : 'Mise à jour terminée avec des manques',
        color: success ? COLOR_OK : COLOR_ERROR,
        description: progressBar(1),
        fields,
        footer: {text: `${state.serversTotal} serveurs · ${state.chunksTotal} chunks`}
    };
}

/**
 * Un cycle merite un ping separe seulement s'il est actionnable : editer un embed
 * ne declenche aucune notification Discord, donc on poste un message dedie.
 */
export function isDegraded(state: CycleState, snap: UniversalisSnapshot): boolean {
    const failed = state.results.filter(r => !r.success).length;
    const manyServers = failed * 10 > state.serversTotal;
    const manyAbandons = snap.requests > 0 && snap.abandoned * 20 > snap.requests;
    return manyServers || manyAbandons;
}
