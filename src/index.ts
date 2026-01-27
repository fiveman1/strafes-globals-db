import mysql, { type ResultSetHeader, type RowDataPacket } from "mysql2/promise";
import * as dotenv from "dotenv";
import axios, { type AxiosResponse } from "axios";

dotenv.config();

interface Record {
    timeId: number
    userId: number
    username: string
    mapId: number
    game: number
    style: number
    course: number
    date: Date
    time: number
}

interface StrafesMap {
    id: number
    name: string
    creator: string
    game: number
    date: Date
    createdAt: Date
    updatedAt: Date
    submitter: number
    smallThumb: string | undefined
    largeThumb: string | undefined
    assetVersion: number
    loadCount: number
    modes: number
}

enum Mode {
    Seed,
    Refresh
}

const STRAFES_KEY = process.env.STRAFES_KEY;

async function main() {
    const user = process.env.DB_USER;
    const password = process.env.DB_PASSWORD;
    if (!user || !password) {
        console.error("Missing database user or password");
        process.exitCode = 1;
        return;
    }

    if (!STRAFES_KEY) {
        console.error("Missing strafes API key");
        process.exitCode = 1;
        return;
    }

    const args = process.argv.splice(2);
    let mode = Mode.Refresh;
    if (args[0] === "seed") {
        mode = Mode.Seed;
    }

    console.log(`Running in '${Mode[mode].toLowerCase()}' mode`);

    // CREATE DATABASE strafes_globals;
    const connection = await mysql.createConnection({
        host: "localhost",
        user: user,
        password: password,
        database: "strafes_globals",
        timezone: "Z", // UTC
        dateStrings: true,
        supportBigNumbers: true
    });

    let query = `CREATE TABLE IF NOT EXISTS users (
        user_id bigint NOT NULL,
        username varchar(64) NOT NULL,
        PRIMARY KEY (user_id)
    );`;

    await connection.query(query);

    query = `CREATE TABLE IF NOT EXISTS maps (
        map_id bigint NOT NULL,
        name nvarchar(128) NOT NULL,
        creator nvarchar(256) NOT NULL,
        game smallint NOT NULL,
        date datetime NOT NULL,
        created_at datetime NOT NULL,
        updated_at datetime NOT NULL,
        submitter bigint NOT NULL,
        small_thumb varchar(256),
        large_thumb varchar(256),
        asset_version bigint NOT NULL,
        load_count int NOT NULL,
        modes smallint NOT NULL,
        PRIMARY KEY (map_id)
    );`;

    await connection.query(query);

    query = `CREATE TABLE IF NOT EXISTS globals (
        time_id bigint NOT NULL,
        user_id bigint NOT NULL,
        map_id bigint NOT NULL,
        game smallint NOT NULL,
        style smallint NOT NULL,
        course smallint NOT NULL,
        date datetime NOT NULL,
        time int NOT NULL,
        PRIMARY KEY (time_id),
        FOREIGN KEY (user_id) REFERENCES users(user_id),
        FOREIGN KEY (map_id) REFERENCES maps(map_id),
        UNIQUE INDEX map_index (map_id, game, style, course),
        INDEX user_index (user_id)
    );`;

    await connection.query(query);

    let success = true;
    if (mode === Mode.Seed) {
        success = await seedWRs(connection);
    }
    else {
        success = await refreshWRs(connection);
    }

    await connection.end();
    process.exitCode = success ? 0 : 1;
}

async function refreshWRs(connection: mysql.Connection) {
    const response = await tryGetStrafes("time/worldrecord", {
        page_number: 1,
        page_size: 100
    });

    if (!response) {
        return false;
    }

    const data = response.data.data as any[];
    const wrs : Record[] = [];
    const timeIds = new Set<number>();
    
    for (const record of data) {
        if (timeIds.has(record.id)) {
            continue;
        }
        timeIds.add(record.id);
        wrs.push({
            timeId: record.id,
            userId: record.user.id,
            username: record.user.username,
            mapId: record.map.id,
            game: record.game_id,
            style: record.style_id,
            course: record.mode_id,
            date: new Date(record.date),
            time: record.time
        });
    }

    await insertUsers(connection, wrs);

    if (!(await wrsHaveMapsLoaded(connection, wrs)) && !(await updateMaps(connection))) {
        return false;
    }

    const wrRows = wrs.map((record) => [
        record.timeId,
        record.userId,
        record.mapId,
        record.game,
        record.style,
        record.course,
        record.date,
        record.time
    ]);

    const query = `INSERT INTO globals (time_id, user_id, map_id, game, style, course, date, time) 
        VALUES ? AS new 
        ON DUPLICATE KEY UPDATE
            time_id=new.time_id,
            user_id=new.user_id,
            map_id=new.map_id,
            game=new.game,
            style=new.style,
            course=new.course,
            date=new.date,
            time=new.time
    ;`;

    const [inserted] = await connection.query<ResultSetHeader>(query, [wrRows]);
    console.log("Inserted WR rows: " + inserted.affectedRows);

    return true;
}

async function seedWRs(connection: mysql.Connection) {
    if (!(await updateMaps(connection))) {
        return false;
    }
    
    const wrs = await loadAllWRs();

    await insertUsers(connection, wrs);

    const wrRows = wrs.map((record) => [
        record.timeId,
        record.userId,
        record.mapId,
        record.game,
        record.style,
        record.course,
        record.date,
        record.time
    ]);
    
    let query = `TRUNCATE TABLE globals;`;
    await connection.query(query);

    query = `INSERT INTO globals (time_id, user_id, map_id, game, style, course, date, time) VALUES ?`;
    const [inserted] = await connection.query<ResultSetHeader>(query, [wrRows]);
    console.log("Inserted WR rows: " + inserted.affectedRows);

    return true;
}

async function wrsHaveMapsLoaded(connection: mysql.Connection, wrs: Record[]) {
    const mapIdSet = new Set<number>();
    const mapIds: number[] = [];
    for (const wr of wrs) {
        if (mapIdSet.has(wr.mapId)) continue;
        mapIdSet.add(wr.mapId);
        mapIds.push(wr.mapId);
    }

    const query = `SELECT map_id FROM maps WHERE map_id IN (?);`;
    const [rows] = await connection.query<RowDataPacket[]>(query, [mapIds]);
    return rows.length === mapIds.length;
}

async function updateMaps(connection: mysql.Connection) {
    const maps = await loadMaps();
    if (!maps) {
        return false;
    }

    const mapRows = maps.map((map) => [
        map.id,
        map.name,
        map.creator,
        map.game,
        map.date,
        map.createdAt,
        map.updatedAt,
        map.submitter,
        map.smallThumb,
        map.largeThumb,
        map.assetVersion,
        map.loadCount,
        map.modes
    ]);
    
    let query = `TRUNCATE TABLE maps;`;
    await connection.query(query);

    query = `INSERT INTO maps (map_id, name, creator, game, date, created_at, updated_at, submitter, small_thumb, large_thumb, asset_version, load_count, modes) VALUES ?`;
    const [inserted] = await connection.query<ResultSetHeader>(query, [mapRows]);
    console.log("Inserted map rows: " + inserted.affectedRows);

    return true;
}

async function loadMaps() {
    let i = 1;
    const maps: StrafesMap[] = [];
    while (true) {
        const mapRes = await tryGetMaps("map", {
            page_number: i,
            page_size: 100
        });

        ++i;

        if (!mapRes) {
            return [];
        }

        const data = mapRes.data.data as any[];
        if (data.length < 1) {
            break;
        }

        const assetToThumb = new Map<number, Map<string, string>>();
        const assetIds: number[] = [];
        for (const map of data) {
            if (map.thumbnail) {
                assetIds.push(map.thumbnail);
            }
        }

        const largeReqPromise = tryGetRequest("https://thumbnails.roproxy.com/v1/assets", {
            "assetIds": assetIds,
            "size": "420x420",
            "format": "Webp"
        });
        
        const smallReqPromise = tryGetRequest("https://thumbnails.roproxy.com/v1/assets", {
            "assetIds": assetIds,
            "size": "75x75",
            "format": "Webp"
        });

        const largeReq = await largeReqPromise;
        const smallReq = await smallReqPromise;

        if (largeReq) {
            for (const assetInfo of largeReq.data.data) {
                const targetId = assetInfo.targetId;
                const url = assetInfo.imageUrl;
                assetToThumb.set(targetId, new Map<string, string>([["large", url]]));
            }
        }

        if (smallReq) {
            for (const assetInfo of smallReq.data.data) {
                const targetId = assetInfo.targetId;
                const url = assetInfo.imageUrl;
                const urlMap = assetToThumb.get(targetId);
                if (urlMap) {
                    urlMap.set("small", url);
                }
                else {
                    assetToThumb.set(targetId, new Map<string, string>([["small", url]]));
                }
            }
        }

        for (const map of data) {
            let small, large;
            if (map.thumbnail) {
                const urls = assetToThumb.get(map.thumbnail);
                small = urls?.get("small");
                large = urls?.get("large");
            }
           
            maps.push({
                id: map.id,
                name: map.display_name,
                creator: map.creator,
                game: map.game_id,
                date: new Date(map.date),
                createdAt: new Date(map.created_at),
                updatedAt: new Date(map.updated_at),
                submitter: map.submitter,
                smallThumb: small,
                largeThumb: large,
                assetVersion: map.asset_version,
                loadCount: map.load_count,
                modes: map.modes
            });
        }

        if (data.length < 100) {
            break;
        }
    }

    return maps;
}

async function loadAllWRs(): Promise<Record[]> {
    const wrs : Record[] = [];
    const timeIds = new Set<number>();

    let burstRemaining = 999;
    let page = 1;
    let hasMore = true;
    while (hasMore) {
        console.log("Loading page " + page);
        const promises: Promise<AxiosResponse<any, any> | undefined>[] = [];
        for (let i = 0; i < 5; ++i) {
            promises.push(tryGetStrafes("time/worldrecord", {
                page_number: page + i,
                page_size: 100
            }));
        }
        page += 5;
        const resolved = await Promise.all(promises);
        for (const response of resolved) {
            if (!response) {
                continue;
            }
            
            const remaining = response.headers["x-rate-limit-burst"];
            if (typeof remaining === "string" && +remaining < burstRemaining) {
                burstRemaining = +remaining;
            }
            
            const data = response.data.data as any[];
            if (data.length <= 0) {
                hasMore = false;
                continue;
            }
            
            for (const record of data) {
                if (timeIds.has(record.id)) {
                    continue;
                }
                timeIds.add(record.id);
                wrs.push({
                    timeId: record.id,
                    userId: record.user.id,
                    username: record.user.username,
                    mapId: record.map.id,
                    game: record.game_id,
                    style: record.style_id,
                    course: record.mode_id,
                    date: new Date(record.date),
                    time: record.time
                });
            }
        }

        if (!hasMore) break;

        if (burstRemaining < 70) {
            burstRemaining = 999;
            console.log("Sleeping for a minute");
            await sleep(1000 * 60); // Wait a minute for burst limit to reset
        }
    }

    return wrs;
}

async function insertUsers(connection: mysql.Connection, wrs: Record[]) {
    const userIdSet = new Set<number>();
    const userRows = [];
    for (const wr of wrs) {
        if (userIdSet.has(wr.userId)) continue;
        userIdSet.add(wr.userId);
        userRows.push([wr.userId, wr.username]);
    }

    const query = `INSERT INTO users (user_id, username) VALUES ? AS new ON DUPLICATE KEY UPDATE username=new.username;`;
    const [usersInserted] = await connection.query<ResultSetHeader>(query, [userRows]);
    console.log("Inserted user rows: " + usersInserted.affectedRows);
}

async function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function tryGetStrafes(end_of_url: string, params?: any) {
    const headers = {
        "X-API-Key": STRAFES_KEY
    };
    return await tryGetRequest(`https://api.strafes.net/api/v1/${end_of_url}`, params, headers);
}

async function tryGetMaps(end_of_url: string, params?: any) {
    const headers = {
        "X-API-Key": STRAFES_KEY
    };
    return await tryGetRequest(`https://maps.strafes.net/public-api/v1/${end_of_url}`, params, headers);
}

async function tryGetRequest(url: string, params?: any, headers?: any) {
    try {
        return await axios.get(url, {params: params, headers: headers, timeout: 3000});
    } 
    catch (err) {
        console.log(err);
        return undefined;
    }
}

main();