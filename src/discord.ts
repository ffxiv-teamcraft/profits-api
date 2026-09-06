import axios from 'axios';

const WEBHOOK = process.env.WEBHOOK;
const USERNAME = 'Profits Helper Updater';

export const COLOR_INFO = 5832650;
export const COLOR_RUNNING = 5814783;
export const COLOR_OK = 4169782;
export const COLOR_WARN = 16755200;
export const COLOR_ERROR = 16734296;

export interface DiscordEmbed {
    title?: string;
    description?: string;
    color?: number;
    fields?: { name: string, value: string, inline?: boolean }[];
    footer?: { text: string };
    timestamp?: string;
}

export interface DiscordPayload {
    content?: string;
    embeds?: DiscordEmbed[];
}

function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * Discord tolere ~5 requetes / 2s par webhook. Tout passe par une chaine serialisee :
 * deux editions concurrentes ne peuvent pas se doubler ni declencher un 429.
 */
let chain: Promise<any> = Promise.resolve();

function enqueue<T>(task: () => Promise<T>, fallback: T): Promise<T> {
    const run = chain.then(() => task().catch(err => {
        console.log(`[DISCORD] ${err.message}`);
        return fallback;
    }));
    // On espace les appels, et la chaine ne doit jamais rester cassee.
    chain = run.then(() => sleep(350), () => sleep(350));
    return run;
}

async function send(method: 'post' | 'patch', url: string, body: any): Promise<any> {
    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            const res = method === 'post' ? await axios.post(url, body) : await axios.patch(url, body);
            return res.data;
        } catch (err) {
            const status = err.response?.status;
            if (status === 429 && attempt < 3) {
                const retryAfter = Number(err.response?.data?.retry_after) || 1;
                await sleep(retryAfter * 1000 + 250);
                continue;
            }
            throw err;
        }
    }
    return null;
}

/**
 * Poste un message. Avec `track`, renvoie son id pour pouvoir l'editer ensuite.
 */
export function postMessage(payload: DiscordPayload, track = false): Promise<string | null> {
    return enqueue(async () => {
        if (!WEBHOOK) {
            return null;
        }
        const url = track ? `${WEBHOOK}?wait=true` : WEBHOOK;
        const data = await send('post', url, {username: USERNAME, content: null, ...payload});
        return track && data ? data.id : null;
    }, null);
}

/**
 * Edite un message deja poste. C'est ce qui permet de suivre un cycle en direct
 * sans ajouter une ligne dans le salon a chaque rafraichissement.
 *
 * L'endpoint d'edition n'accepte pas `username` : on ne renvoie que le contenu.
 */
export function editMessage(messageId: string, payload: DiscordPayload): Promise<boolean> {
    return enqueue(async () => {
        if (!WEBHOOK || !messageId) {
            return false;
        }
        await send('patch', `${WEBHOOK}/messages/${messageId}`, {
            content: payload.content ?? null,
            embeds: payload.embeds || []
        });
        return true;
    }, false);
}

/** Barre de progression textuelle, lisible dans une description d'embed. */
export function progressBar(ratio: number, width = 22): string {
    const clamped = Math.min(1, Math.max(0, ratio || 0));
    const filled = Math.round(clamped * width);
    return `\`${'█'.repeat(filled)}${'░'.repeat(width - filled)}\` ${Math.round(clamped * 100)}%`;
}

/**
 * Horodatage relatif Discord : le client reaffiche "dans 12 minutes" tout seul,
 * ce qui garde l'estimation vivante entre deux editions.
 */
export function relativeTime(timestampMs: number): string {
    return `<t:${Math.floor(timestampMs / 1000)}:R>`;
}

export function formatDuration(ms: number): string {
    const total = Math.max(0, Math.round(ms / 1000));
    const h = Math.floor(total / 3600);
    const m = Math.floor((total % 3600) / 60);
    const s = total % 60;
    if (h > 0) {
        return `${h}h ${m}min`;
    }
    if (m > 0) {
        return `${m}min ${s}s`;
    }
    return `${s}s`;
}
