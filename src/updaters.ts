import axios from "axios";
import {chunk} from "lodash";
import {combineLatest, debounceTime, defer, from, of, repeat, ReplaySubject, shareReplay, Subject} from "rxjs";
import {switchMap} from "rxjs/operators";
import {
    buildStaticData,
    createRedisClient,
    MbEntry,
    StaticItemData,
    updateCache,
    updateItems,
    writeMarketEntries
} from "./common";
import {getConfiguredRate, resetStats, snapshotStats, universalisGet} from "./universalis";
import {Item} from "./item";
import {exec} from "child_process";
import {GAME_SERVERS} from "./servers";
import {RedisClientType} from "redis";
import {COLOR_ERROR, COLOR_INFO, DiscordEmbed, editMessage, formatDuration, postMessage} from "./discord";
import {
    aprioriCycleMs,
    CycleState,
    finalEmbed,
    incidentSummary,
    isDegraded,
    runningEmbed,
    ServerRunResult
} from "./report";

interface ItemsBundle {
    items: Record<number, Item>;
    staticData: Record<number, StaticItemData>;
}

const items$ = new ReplaySubject<ItemsBundle>(1);
const delayBetweenRuns = 3600000;
const updated$ = new Subject<void>();

/** Rafraichissement du message de progression. Une edition ne cree pas de ligne. */
const PROGRESS_INTERVAL_MS = 45000;
const MENTION = '<@194378871317987328>';
const CYCLE_AVG_KEY = 'updater:avg-cycle-ms';
const PROGRESS_MSG_KEY = 'updater:progress-message';

(async () => {
    console.log('Preparing items');
    const items = {};
    const extractsReq = await axios.get('https://raw.githubusercontent.com/ffxiv-teamcraft/ffxiv-teamcraft/staging/libs/data/src/lib/extracts/extracts.json');
    const recipesReq = await axios.get('https://raw.githubusercontent.com/ffxiv-teamcraft/ffxiv-teamcraft/staging/libs/data/src/lib/json/recipes.json');
    const extracts = extractsReq.data;
    const recipes = recipesReq.data;
    Object.values<any>(extracts)
        .filter(e => !e.sources.some((s: any) => s.type === -1))
        .forEach(extract => {
            const crafting = extract.sources.find((source: any) => source.type === 1)?.data || null;
            const gathering = extract.sources.find((source: any) => source.type === 7)?.data || null;
            const vendors = extract.sources.find((source: any) => source.type === 3)?.data || null;
            const trades = extract.sources.find((source: any) => source.type === 2)?.data || null;
            const reduction = extract.sources.find((source: any) => source.type === 4)?.data || null;
            const requirements = crafting ? recipes.find((r: any) => r.id.toString() === crafting[0].id.toString())?.ingredients : null;
            items[extract.id] = {
                id: extract.id,
                crafting,
                gathering,
                vendors,
                trades,
                reduction,
                requirements
            };
        });
    // complexity / levelReqs ne dependent pas du serveur : une seule passe pour les 118 mondes
    console.log('Precomputing server-independent item data');
    items$.next({items, staticData: buildStaticData(items)});
})();

/**
 * La liste des items marchands conditionne tout le cycle : on insiste jusqu'a l'obtenir
 * plutot que de demarrer sur une liste vide.
 */
async function fetchMarketableIds(): Promise<number[]> {
    for (; ;) {
        const res = await universalisGet<number[]>('https://universalis.app/api/marketable');
        if (res.ok) {
            return res.data;
        }
        console.error(`Liste des items marchands indisponible (${res.reason}), nouvelle tentative dans 30s`);
        await new Promise(resolve => setTimeout(resolve, 30000));
    }
}

async function loadAvgCycleMs(redis: RedisClientType): Promise<number> {
    const raw = await redis.get(CYCLE_AVG_KEY);
    const value = raw ? Number(raw) : 0;
    return isFinite(value) && value > 0 ? value : 0;
}

/** Moyenne glissante : un cycle atypique ne fausse pas durablement l'estimation. */
async function recordCycleMs(redis: RedisClientType, ms: number): Promise<void> {
    const previous = await loadAvgCycleMs(redis);
    const next = previous ? Math.round(previous * 0.7 + ms * 0.3) : Math.round(ms);
    await redis.set(CYCLE_AVG_KEY, next);
}

/**
 * Rafraichit le message du cycle. On edite le message existant : le salon garde une
 * seule ligne par cycle, quelle que soit la frequence de rafraichissement.
 */
async function refresh(state: CycleState, embed?: DiscordEmbed): Promise<void> {
    const payload = {embeds: [embed || runningEmbed(state, snapshotStats())]};
    if (state.messageId) {
        if (await editMessage(state.messageId, payload)) {
            state.editFailures = 0;
            return;
        }
        state.editFailures++;
        // Message supprime ou webhook recree : on en repost un, mais pas a chaque essai.
        if (state.editFailures < 3) {
            return;
        }
        state.messageId = null;
        state.editFailures = 0;
    }
    state.messageId = await postMessage(payload, true);
}

/**
 * Un serveur complet. Les requetes partent toutes d'un coup : c'est le token bucket
 * de universalis.ts qui regule le debit.
 *
 * Un chunk en echec ne fait plus tomber (ni bloquer) le serveur entier : on ecrit ce
 * qu'on a, et on remonte le nombre de chunks manquants dans le rapport.
 */
async function updateServer(server: string, bundle: ItemsBundle, itemIds: number[],
                            redis: RedisClientType, onChunkDone: () => void): Promise<ServerRunResult> {
    const start = Date.now();
    const chunks = chunk(itemIds, 100);
    console.log(`Starting MB data aggregation for ${server}`);
    try {
        const results = await Promise.all(chunks.map(ids => updateItems(server, ids).then(res => {
            onChunkDone();
            return res;
        })));
        const failedChunks = results.filter(res => !res.ok).length;

        const data: Record<number, MbEntry> = {};
        for (const res of results) {
            Object.assign(data, res.data);
        }

        const items = Object.keys(data).length;
        if (items > 0) {
            await writeMarketEntries(redis, server, data);
            await updateCache(server, bundle.items, bundle.staticData, redis);
            await redis.set(`profit:${server}:updated`, Date.now());
        }

        const time = Date.now() - start;
        console.log(`${server} ${failedChunks === 0 ? 'ok' : `partiel (${failedChunks}/${chunks.length} chunks KO)`}, ${Math.floor(time / 1000)}s`);
        return {server, success: failedChunks === 0, failedChunks, time, items};
    } catch (err) {
        console.log(`${server} KO: ${err.message}`);
        return {
            server,
            success: false,
            failedChunks: chunks.length,
            time: Date.now() - start,
            items: 0,
            error: err.message
        };
    }
}

/** Une alerte separee, et seulement quand c'est actionnable : un embed edite ne ping pas. */
async function maybeAlert(state: CycleState): Promise<void> {
    const snap = snapshotStats();
    if (!isDegraded(state, snap)) {
        return;
    }
    const failed = state.results.filter(r => !r.success);
    await postMessage({
        content: MENTION,
        embeds: [{
            title: 'Cycle dégradé',
            color: COLOR_ERROR,
            description: `${failed.length}/${state.serversTotal} serveurs incomplets, `
                + `${snap.abandoned}/${snap.requests} requêtes abandonnées.`,
            fields: [
                {name: 'Détail', value: incidentSummary(snap, failed)},
                ...(snap.sampleFailures.length > 0
                    ? [{name: 'Exemples', value: snap.sampleFailures.join('\n').slice(0, 1024)}]
                    : [])
            ]
        }]
    });
}

async function runCycle(servers: string[], bundle: ItemsBundle, itemIds: number[],
                        redis: RedisClientType): Promise<void> {
    const chunksTotal = Math.ceil(itemIds.length / 100) * servers.length;
    const average = await loadAvgCycleMs(redis);
    const state: CycleState = {
        startedAt: Date.now(),
        serversTotal: servers.length,
        serversDone: 0,
        chunksTotal,
        chunksDone: 0,
        currentServer: servers[0],
        estimateMs: average || aprioriCycleMs(chunksTotal, servers.length, getConfiguredRate()),
        estimateFromHistory: average > 0,
        results: [],
        messageId: null,
        editFailures: 0
    };
    resetStats();

    await refresh(state);
    if (state.messageId) {
        await redis.set(PROGRESS_MSG_KEY, state.messageId);
    }

    const timer = setInterval(() => {
        refresh(state).catch(err => console.log(`[PROGRESS] ${err.message}`));
    }, PROGRESS_INTERVAL_MS);

    try {
        for (const server of servers) {
            state.currentServer = server;
            state.results.push(await updateServer(server, bundle, itemIds, redis, () => state.chunksDone++));
            state.serversDone++;
        }
    } finally {
        clearInterval(timer);
    }

    await recordCycleMs(redis, Date.now() - state.startedAt);
    await refresh(state, finalEmbed(state, snapshotStats(), Date.now() + delayBetweenRuns));
    await redis.del(PROGRESS_MSG_KEY);
    await maybeAlert(state);
    updated$.next(void 0);
}

/**
 * Au demarrage : si un message de progression traine, c'est que le process est tombe
 * en cours de cycle. On le clot explicitement plutot que de laisser une barre figee.
 */
async function announceStartup(redis: RedisClientType, servers: number, itemCount: number): Promise<void> {
    const orphan = await redis.get(PROGRESS_MSG_KEY);
    if (orphan) {
        await editMessage(orphan, {
            embeds: [{
                title: 'Cycle interrompu',
                color: COLOR_ERROR,
                description: 'Le process a redémarré avant la fin de ce cycle.'
            }]
        });
        await redis.del(PROGRESS_MSG_KEY);
    }
    const average = await loadAvgCycleMs(redis);
    const chunks = Math.ceil(itemCount / 100) * servers;
    await postMessage({
        embeds: [{
            title: 'Updater démarré',
            color: COLOR_INFO,
            description: `${servers} serveurs · ${itemCount} items · ${chunks} chunks (${chunks * 2} requêtes par cycle).`,
            fields: [{
                name: 'Durée attendue',
                value: average
                    ? `${formatDuration(average)} (moyenne des cycles précédents)`
                    : `${formatDuration(aprioriCycleMs(chunks, servers, getConfiguredRate()))} (estimation théorique)`,
                inline: true
            }]
        }]
    });
}

console.log('Creating core data Observable');

const coreData$ = combineLatest([
    of(GAME_SERVERS),
    from(createRedisClient()),
    items$,
    from(fetchMarketableIds())
]).pipe(
    shareReplay(1)
);

console.log('Creating full data scheduler');

coreData$.pipe(
    switchMap(([servers, redis, bundle, itemIds]) => {
        return from(announceStartup(redis, servers.length, itemIds.length)).pipe(
            switchMap(() => defer(() => from(runCycle(servers, bundle, itemIds, redis))).pipe(
                repeat({delay: delayBetweenRuns})
            ))
        );
    })
).subscribe({
    error: err => {
        console.error('PIPELINE ERROR', err.message);
        postMessage({
            content: MENTION,
            embeds: [{title: 'Updater arrêté', color: COLOR_ERROR, description: err.message.slice(0, 500)}]
        });
    }
});

// If no updates after an entire day, ping Miu in the monitoring channel !
updated$.pipe(debounceTime(86400000)).subscribe(() => {
    postMessage({
        content: MENTION,
        embeds: [{title: 'Aucune mise à jour depuis plus de 24 h', color: COLOR_ERROR}]
    });
    exec('pm2 restart Updater');
});
