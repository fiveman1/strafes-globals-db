import mysql, { type ResultSetHeader } from "mysql2/promise";
import * as dotenv from "dotenv";
import axios, { type AxiosResponse } from "axios";

dotenv.config();

interface Record {
    time_id: bigint,
    user_id: bigint,
    map_id: bigint,
    game: number,
    style: number,
    course: number,
    date: Date,
    time: number
}

async function main() {
    const user = process.env.DB_USER;
    const password = process.env.DB_PASSWORD;
    if (!user || !password) {
        console.error("Missing database user or password");
        process.exitCode = 1;
        return;
    }

    const strafesKey = process.env.STRAFES_KEY;
    if (!strafesKey) {
        console.error("Missing strafes API key");
        process.exitCode = 1;
        return;
    }

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

    let query = `CREATE TABLE IF NOT EXISTS globals (
        time_id bigint NOT NULL,
        user_id bigint NOT NULL,
        map_id bigint NOT NULL,
        game int NOT NULL,
        style int NOT NULL,
        course int NOT NULL,
        date datetime NOT NULL,
        time int NOT NULL,
        PRIMARY KEY (time_id),
        UNIQUE INDEX map_index (map_id, game, style, course),
        INDEX user_index (user_id)
    );`;

    await connection.query(query);

    const wrs = await loadWRs(strafesKey);

    const rows = wrs.map((record) => [
        record.time_id,
        record.user_id,
        record.map_id,
        record.game,
        record.style,
        record.course,
        record.date,
        record.time
    ]);
    
    query = `TRUNCATE TABLE globals;`;
    await connection.query(query);

    query = `INSERT INTO globals (time_id, user_id, map_id, game, style, course, date, time) VALUES ?`;
    const [inserted] = await connection.query<ResultSetHeader>(query, [rows]);
    console.log("Inserted rows: " + inserted.affectedRows);

    await connection.end();
    process.exitCode = 0;
}

async function loadWRs(strafesKey: string): Promise<Record[]> {
    const wrs : Record[] = [];
    const timeIds = new Set<number>();

    let burstRemaining = 999;
    let page = 1;
    let hasMore = true;
    while (hasMore) {
        console.log("Loading page " + page);
        const promises: Promise<AxiosResponse<any, any> | undefined>[] = [];
        for (let i = 0; i < 5; ++i) {
            promises.push(tryGetStrafes(strafesKey, "time/worldrecord", {
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
                    time_id: record.id,
                    user_id: record.user.id,
                    map_id: record.map.id,
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

async function sleep(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function tryGetStrafes(strafesKey: string, end_of_url: string, params?: any) {
    const headers = {
        "X-API-Key": strafesKey
    };
    return await tryGetRequest(`https://api.strafes.net/api/v1/${end_of_url}`, params, headers);
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