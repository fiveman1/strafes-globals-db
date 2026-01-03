import type { RowDataPacket } from "mysql2";
import mysql from "mysql2/promise";
import * as dotenv from "dotenv";

dotenv.config();

interface Record {
    time_id: bigint,
    user_id: bigint,
    map_id: bigint,
    game: number,
    style: number,
    course: number,
    date: string,
    time: number
}

type RecordRow = Record & RowDataPacket;

async function main() {
    // CREATE DATABASE strafes_globals;
    const user = process.env.DB_USER;
    const password = process.env.DB_PASSWORD;
    if (!user || !password) {
        console.error("Missing database user or password");
        process.exitCode = 1;
        return;
    }
    const connection = await mysql.createConnection({
        host: "localhost",
        user: user,
        password: password,
        database: "strafes_globals",
        timezone: "Z",
        dateStrings: true,
        supportBigNumbers: true
    });

    const query = `create table if not exists globals (
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
        INDEX user_index (user_id, game, style, course)
    );`;

    await connection.query(query);
    process.exit(0);
}

main();