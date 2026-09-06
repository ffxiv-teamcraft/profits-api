import {createClient, RedisClientType} from 'redis';
import {Item} from './item';
import {subHours} from 'date-fns';
import {ErrorSink, universalisGet, UniversalisResult} from './universalis';
import {chunk, uniqBy} from 'lodash';

/** Market entry stored under mb:{server}:{itemId}. */
export interface MbEntry {
    v24: number;
    v48: number;
    avg24: number;
    c: number;
    c10: number;
    c50: number;
    t: number;
    tr24: number;
}

/** Server-independent data: computed once, not 118 times. */
export interface StaticItemData {
    crafting: boolean;
    gathering: boolean;
    complexity: number;
    levelReqs: number[];
}

export interface ChunkResult {
    server: string;
    ok: boolean;
    data: Record<number, MbEntry>;
    reason?: string;
}

export async function createRedisClient(): Promise<RedisClientType> {
    const REDISHOST = process.env.REDISHOST || '10.140.235.195';
    const REDISPORT = process.env.REDISPORT || 6379;
    const client = createClient({
        url: `redis://${REDISHOST}:${REDISPORT}`,
        socket: {
            // No longer kill the process on the first network hiccup: retry, and
            // only give up after 10 unsuccessful attempts.
            reconnectStrategy: retries => {
                if (retries > 10) {
                    console.error('REDIS: 10 reconnection attempts failed, giving up.');
                    return new Error('redis unreachable');
                }
                return Math.min(1000 * retries, 10000);
            }
        }
    }) as unknown as RedisClientType;
    client.on('error', err => {
        console.error('REDIS ERROR', err.message);
    });
    await client.connect();
    return client;
}

export function evaluateComplexity(item: Item, items: Record<number, Item>): number {
    if (!item) {
        return 99999;
    }
    if (item.requirements) {
        return item.requirements.filter(i => i.id > 19).reduce((acc, ingredient) => {
            return acc + Math.ceil(evaluateComplexity(items[+ingredient.id], items) * (ingredient.amount / 4));
        }, 1);
    }
    if (item.vendors) {
        return 1;
    }
    if (item.gathering) {
        if (item.gathering.nodes[0]?.limited) {
            return 4;
        }
        return 1;
    }
    if (item.reduction) {
        return 2;
    }
    if (item.trades) {
        return 3;
    }
    return 99999;
}

export function getLevelRequirements(item: Item, items: Record<number, Item>): number[] {
    let baseRequirements = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    if (!item) {
        return baseRequirements;
    }
    if (item.id < 20) {
        return baseRequirements;
    }
    if (item.gathering) {
        baseRequirements[9 + item.gathering.type % 2] = item.gathering.level;
        return baseRequirements;
    } else if (item.crafting) {
        baseRequirements[item.crafting[0].job - 8] = item.crafting[0].lvl;
        item.requirements?.forEach(req => {
            const reqRequirements = getLevelRequirements(items[+req.id], items);
            baseRequirements = baseRequirements.map((lvl, i) => {
                if (reqRequirements[i] > lvl) {
                    return reqRequirements[i];
                }
                return lvl;
            });
        });
    }
    return baseRequirements;
}

/**
 * complexity and levelReqs depend only on the recipe, never on the server. They are
 * computed once at startup instead of being recomputed for every world.
 */
export function buildStaticData(items: Record<number, Item>): Record<number, StaticItemData> {
    const staticData: Record<number, StaticItemData> = {};
    for (const [id, item] of Object.entries<Item>(items)) {
        staticData[+id] = {
            crafting: item.crafting !== null,
            gathering: item.gathering !== null || item.reduction !== null,
            complexity: evaluateComplexity(item, items),
            levelReqs: getLevelRequirements(item, items)
        };
    }
    return staticData;
}

/**
 * Resolves craft cost entirely in memory, with memoisation: an item shared by dozens
 * of recipes is evaluated only once per server.
 */
function createCostResolver(items: Record<number, Item>, prices: Map<number, MbEntry>): (id: number) => number {
    const memo = new Map<number, number>();
    const visiting = new Set<number>();
    const resolve = (id: number): number => {
        if (memo.has(id)) {
            return memo.get(id);
        }
        const item = items[id];
        if (!item) {
            return -1;
        }
        if (visiting.has(id)) {
            // guard: a cyclic recipe must not blow the stack
            return -1;
        }
        visiting.add(id);
        let result: number;
        if (item.requirements) {
            let total = 1;
            for (const ingredient of item.requirements) {
                if (items[+ingredient.id]) {
                    total += Math.floor(resolve(+ingredient.id) * ingredient.amount);
                }
            }
            result = total;
        } else {
            const entry = prices.get(id);
            result = entry ? entry.c : -1;
        }
        visiting.delete(id);
        memo.set(id, result);
        return result;
    };
    return resolve;
}

/** Only these two fields are consumed downstream. */
const LISTING_FIELDS = 'items.listings.pricePerUnit,items.listings.quantity';

/**
 * Listings only. `entries=0` is the key part: by default this endpoint also returns
 * recentHistory, which the old code then fetched again through /api/history -- history
 * was paid for twice. Measured on a 100-item chunk (Odin): 5115ms -> 82ms, 427KB -> 35KB.
 */
export function buildListingsUrl(server: string, itemIds: number[]): string {
    return `https://universalis.app/api/v2/${encodeURIComponent(server)}/${itemIds.join(',')}`
        + `?statsWithin=0&entries=0&fields=${encodeURIComponent(LISTING_FIELDS)}`;
}

/**
 * 48h history. Two quirks of this endpoint: it ignores `fields`, and its entries do
 * not expose `total` (unlike recentHistory on the main endpoint), so it is rebuilt
 * client-side.
 *
 * Do not merge this with the listings call: asking for listings + history in a single
 * request is 10 to 50x slower (measured: 8.5-23.5s against 0.45s for the parallel pair).
 */
export function buildHistoryUrl(server: string, itemIds: number[]): string {
    return `https://universalis.app/api/v2/history/${encodeURIComponent(server)}/${itemIds.join(',')}`
        + `?statsWithin=0&entriesWithin=172800`;
}

export async function updateItems(server: string, itemIds: number[], errors$?: ErrorSink): Promise<ChunkResult> {
    const yesterday = Math.floor(subHours(new Date(), 24).getTime() / 1000);
    const oneDaybeforeYesterday = Math.floor(subHours(new Date(), 48).getTime() / 1000);

    // Both calls go out together: the wait is the slower of the two, not their sum.
    const [listingsRes, historyRes]: UniversalisResult<any>[] = await Promise.all([
        universalisGet<any>(buildListingsUrl(server, itemIds), errors$),
        universalisGet<any>(buildHistoryUrl(server, itemIds), errors$)
    ]);
    if (!listingsRes.ok || !historyRes.ok) {
        // Both are required: keeping the previous Redis values beats writing an
        // incomplete set. A stale v24 is less wrong than a v24 of zero.
        return {server, ok: false, data: {}, reason: listingsRes.reason || historyRes.reason};
    }

    const historyItems = historyRes.data.items || {};
    const data: Record<number, MbEntry> = {};
    // In v2 `items` is an object keyed by id (not an array), and non-marketable
    // items are simply absent from the response.
    for (const [rawId, item] of Object.entries<any>(listingsRes.data.items || {})) {
        const listings = item.listings || [];
        const entries = historyItems[rawId]?.entries || [];
        const last24hSales = entries.filter((h: { timestamp: number }) => h.timestamp > yesterday);
        const tr24 = last24hSales.slice(-5).reduce((accp: number, row: any) => accp + row.pricePerUnit, 0)
            - last24hSales.slice(0, 5).reduce((accp: number, row: any) => accp + row.pricePerUnit, 0);
        const v24 = last24hSales.reduce((total: number, e: { quantity: number }) => total + e.quantity, 0);
        const v48 = entries.filter((h: { timestamp: number }) => h.timestamp > oneDaybeforeYesterday)
            .reduce((total: number, e: { quantity: number }) => total + e.quantity, 0);
        // The old code read e.total, absent from /api/history: the sum was NaN and
        // avg24 fell back to 0 through `|| 0`. Identity verified on 419 real sales
        // (325 of them with quantity > 1): total === pricePerUnit * quantity.
        const revenue24 = last24hSales.reduce((total: number, e: any) => total + e.pricePerUnit * e.quantity, 0);
        const avg24 = Math.floor(revenue24 / v24) || 0;
        const t = listings.reduce((accp: number, a: any) => accp + a.quantity, 0);
        const sorted = listings.slice().sort((a: any, b: any) => a.pricePerUnit - b.pricePerUnit);
        const c = sorted[0]?.pricePerUnit || 0;
        const c10 = sorted.filter((l: any) => l.quantity >= 10)[0]?.pricePerUnit || 0;
        const c50 = sorted.filter((l: any) => l.quantity >= 50)[0]?.pricePerUnit || 0;
        data[+rawId] = {v24, v48, avg24, c, c10, c50, t, tr24};
    }
    return {server, ok: true, data};
}

/** Batched writes: ~17,000 individual SETs become ~17 pipelines. */
export async function writeMarketEntries(redis: RedisClientType, server: string,
                                         data: Record<number, MbEntry>): Promise<void> {
    for (const batch of chunk(Object.entries(data), 1000)) {
        const multi = redis.multi();
        for (const [id, value] of batch) {
            multi.set(`mb:${server}:${id}`, JSON.stringify(value));
        }
        await multi.exec();
    }
}

/**
 * Previously ~240,000 sequential Redis round trips per server (one get per item, one
 * more per ingredient at every recipe depth, plus a redundant get for profit). Now: a
 * handful of MGETs, then everything computed in memory.
 */
export async function updateCache(server: string, items: Record<number, Item>,
                                  staticData: Record<number, StaticItemData>,
                                  redis: RedisClientType): Promise<void> {
    const ids = Object.keys(items);
    const prices = new Map<number, MbEntry>();
    for (const batch of chunk(ids, 5000)) {
        const raw = await redis.mGet(batch.map(id => `mb:${server}:${id}`));
        for (let i = 0; i < batch.length; i++) {
            if (raw[i]) {
                try {
                    prices.set(+batch[i], JSON.parse(raw[i]));
                } catch {
                    // corrupt entry: skip it rather than bringing the cycle down
                }
            }
        }
    }

    const resolveCost = createCostResolver(items, prices);
    const updated = Date.now();
    const serverCache: any[] = [];
    for (const id of ids) {
        if (+id === 1) {
            continue;
        }
        const entry = prices.get(+id);
        if (!entry) {
            continue;
        }
        const staticEntry = staticData[+id];
        serverCache.push({
            id,
            crafting: staticEntry.crafting,
            gathering: staticEntry.gathering,
            complexity: staticEntry.complexity,
            cost: resolveCost(+id),
            profit: {c: entry.c, c10: entry.c10, c50: entry.c50},
            v24: entry.v24,
            v48: entry.v48,
            total: entry.t,
            trend24: entry.tr24,
            levelReqs: staticEntry.levelReqs,
            updated
        });
    }

    const currentServerCacheRaw = await redis.get(`profit:${server}`);
    const currentServerCache = currentServerCacheRaw ? JSON.parse(currentServerCacheRaw) : [];
    await redis.set(`profit:${server}`, JSON.stringify(uniqBy([...serverCache, ...currentServerCache], 'id')));
}
