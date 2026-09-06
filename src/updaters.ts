import axios from "axios";
import {chunk} from "lodash";
import {
    BehaviorSubject,
    combineLatest,
    debounceTime,
    defer,
    first,
    from,
    Observable,
    of,
    repeat,
    ReplaySubject,
    scan,
    shareReplay,
    skip,
    Subject,
    throttleTime
} from "rxjs";
import {switchMap, tap} from "rxjs/operators";
import {
    buildStaticData,
    createRedisClient,
    MbEntry,
    StaticItemData,
    updateCache,
    updateItems,
    writeMarketEntries
} from "./common";
import {universalisGet} from "./universalis";
import {Item} from "./item";
import {intervalToDuration} from "date-fns";
import {exec} from "child_process";
import {GAME_SERVERS} from "./servers";
import {RedisClientType} from "redis";

interface ItemsBundle {
    items: Record<number, Item>;
    staticData: Record<number, StaticItemData>;
}

interface ServerRunResult {
    server: string;
    success: boolean;
    failedChunks: number;
    time: number;
}

const items$ = new ReplaySubject<ItemsBundle>(1);
const delayBetweenRuns = 3600000;
const updated$ = new Subject<void>();

function properConcat<T>(sources: Observable<T>[]): Observable<T[]> {
    const index$ = new BehaviorSubject<number>(0);
    return index$.pipe(
        switchMap(i => sources[i]),
        scan((acc, res) => [...acc, res], []),
        tap(() => {
            if (index$.value < sources.length - 1) {
                index$.next(index$.value + 1)
            } else {
                index$.complete();
            }
        }),
        skip(sources.length - 1),
        first()
    )
}

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

const errors$ = new Subject<{ source: string, message: string }>();

errors$.pipe(
    throttleTime(60000),
).subscribe(({source, message}) => {
    axios.post(process.env.WEBHOOK, {
        content: null,
        embeds: [{
            title: message,
            description: `${source.slice(0, 256)}...`,
            color: 16711680
        }],
        username: 'Profits Helper Updater'
    }).catch(err => {
        console.log(`[DISCORD ERROR HOOK] ${err.message}`)
    });
})

/**
 * La liste des items marchands conditionne tout le cycle : on insiste jusqu'a l'obtenir
 * plutot que de demarrer sur une liste vide.
 */
async function fetchMarketableIds(): Promise<number[]> {
    for (; ;) {
        const res = await universalisGet<number[]>('https://universalis.app/api/marketable', errors$);
        if (res.ok) {
            return res.data;
        }
        console.error(`Liste des items marchands indisponible (${res.reason}), nouvelle tentative dans 30s`);
        await new Promise(resolve => setTimeout(resolve, 30000));
    }
}

/**
 * Un serveur complet. Les requetes sont toutes lancees d'un coup : c'est le token bucket
 * de universalis.ts qui regule le debit, plus la structure du pipeline.
 *
 * Un chunk en echec ne fait plus tomber (ni bloquer) le serveur entier : on ecrit ce
 * qu'on a, et on remonte le nombre de chunks manquants dans le rapport.
 */
async function updateServer(server: string, bundle: ItemsBundle, itemIds: number[],
                            redis: RedisClientType): Promise<ServerRunResult> {
    const start = Date.now();
    console.log(`Starting MB data aggregation for ${server}`);
    try {
        const chunks = chunk(itemIds, 100);
        const results = await Promise.all(chunks.map(ids => updateItems(server, ids, errors$)));
        const failedChunks = results.filter(res => !res.ok).length;

        const data: Record<number, MbEntry> = {};
        for (const res of results) {
            Object.assign(data, res.data);
        }

        if (Object.keys(data).length > 0) {
            await writeMarketEntries(redis, server, data);
            await updateCache(server, bundle.items, bundle.staticData, redis);
            await redis.set(`profit:${server}:updated`, Date.now());
        }

        const time = Date.now() - start;
        console.log(`${server} ${failedChunks === 0 ? 'ok' : `partiel (${failedChunks}/${chunks.length} chunks KO)`}, ${Math.floor(time / 1000)}s`);
        return {server, success: failedChunks === 0, failedChunks, time};
    } catch (err) {
        errors$.next({source: `[Updater] Server ${server}`, message: err.message});
        console.log(err.message);
        return {server, success: false, failedChunks: -1, time: Date.now() - start};
    }
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

axios.post(process.env.WEBHOOK, {
    embeds: [{
        title: 'Updater started',
        color: 5832650,
        description: `Updater process has been started, initializing now... expect an update starting in a couple of seconds.`,
    }],
    username: 'Profits Helper Updater'
});


coreData$.pipe(
    switchMap(([servers, redis, bundle, itemIds]) => {
        return defer(() => {
            const expectedDuration = intervalToDuration({start: 0, end: servers.length * 180000});
            axios.post(process.env.WEBHOOK, {
                embeds: [{
                    title: 'Full update starting',
                    color: 5814783,
                    description: `Starting full update for ${servers.length} servers, ${itemIds.length} items (${Math.ceil(itemIds.length / 100)} chunks, ${Math.ceil(itemIds.length / 100) * servers.length} requests), this is expected to take about **${expectedDuration.hours} hours and ${expectedDuration.minutes} minutes** and should be done on <t:${Math.floor(new Date(Date.now() + servers.length * 180000).getTime() / 1000)}>`,
                }],
                username: 'Profits Helper Updater'
            }).catch(err => console.log(err.message));
            return properConcat(servers.map(server => {
                return defer(() => from(updateServer(server, bundle, itemIds, redis)));
            }));
        }).pipe(
            repeat({
                delay: delayBetweenRuns
            })
        )
    })
).subscribe((result) => {
    const success = result.every(row => row.success);
    const failedServers = result.filter(row => !row.success).map(row => row.server);
    const totalTime = result.reduce((acc, r) => acc + r.time, 0);
    const missingChunks = result.reduce((acc, r) => acc + Math.max(0, r.failedChunks), 0);
    const duration = intervalToDuration({start: 0, end: totalTime});
    const fields = [
        {
            name: "Avg per server",
            value: `${Math.floor(totalTime / 1000 / result.length)}s`
        },
        {
            name: "Total time for this run",
            value: `${duration.hours}h ${duration.minutes}min ${duration.seconds}s`
        }
    ];
    if (!success) {
        fields.push({
            name: 'Failed servers',
            value: failedServers.map(server => ` - ${server}`).join('\n').slice(0, 1024)
        });
        fields.push({
            name: 'Missing chunks',
            value: `${missingChunks}`
        });
    }
    const report = {
        content: null,
        embeds: [{
            title: 'Full update status report',
            color: success ? 4169782 : 16734296,
            fields,
            footer: {
                text: `Next update cycle in 1h`
            }
        }],
        username: 'Profits Helper Updater'
    };
    axios.post(process.env.WEBHOOK, report).catch(err => console.log(err.message));
    updated$.next(void 0);
});

// If no updates after an entire day, ping Miu in the monitoring channel !
updated$.pipe(debounceTime(86400000)).subscribe(() => {
    axios.post(process.env.WEBHOOK, {
        content: '<@194378871317987328>',
        embeds: [{
            title: 'No updates for more than a day',
            color: 16734296
        }],
        username: 'Profits Helper Updater'
    });
    exec('pm2 restart Updater');
});
