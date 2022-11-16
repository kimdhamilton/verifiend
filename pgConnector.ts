import * as pulumi from "@pulumi/pulumi";
import * as _ from "lodash";
import { Status } from './types';

let config = new pulumi.Config();

const DB_HOST = config.require("dbhost");
const DB_PORT = config.getNumber("dbport") || 5432;
const DB_NAME = config.require("dbname");
const DB_USER = config.require("dbuser");
const DB_PASSWORD = config.require("dbpassword");
const DB_TABLE_NAME_COUNTS = config.require("verifiedcount_tn");

export async function saveRawCounts(verifiedCount: number) {
    const { Client: PGClient } = require("pg");
    const pgclient = new PGClient({
        user: DB_USER,
        host: DB_HOST,
        database: DB_NAME,
        password: DB_PASSWORD,
        port: DB_PORT,
    });
    await pgclient.connect();
    const result = await pgclient.query(`INSERT INTO ${DB_TABLE_NAME_COUNTS}(timestamp, count) VALUES(now(), '${verifiedCount}');`);
    console.log(`${result.rowCount} rows updated`);
    await pgclient.end();
}

export async function getTimestampCounts(): Promise<[]> {
    const { Client: PGClient } = require("pg");
    const pgclient = new PGClient({
        user: DB_USER,
        host: DB_HOST,
        database: DB_NAME,
        password: DB_PASSWORD,
        port: DB_PORT,
    });
    await pgclient.connect();
    const result = await pgclient.query(`SELECT "timestamp", "count" from ${DB_TABLE_NAME_COUNTS};`);

    let timestampCounts = [];
    if (result.rows?.length > 0) {
        console.log(JSON.stringify(result.rows));
        timestampCounts = result.rows;
    }
    await pgclient.end();
    return timestampCounts;
}

export async function getQueryStatus(tableName: string): Promise<Status | undefined> {
    const { Client: PGClient } = require("pg");
    const pgclient = new PGClient({
        user: DB_USER,
        host: DB_HOST,
        database: DB_NAME,
        password: DB_PASSWORD,
        port: DB_PORT,
    })
    await pgclient.connect();

    let status: Status | undefined;
    const result = await pgclient.query(`SELECT * FROM ${tableName} ORDER BY id DESC LIMIT 1;`);
    if (result.rows?.length > 0) {
        status = result.rows[0];
    }
    await pgclient.end();
    return status;
}

export async function saveQueryStatus(tableName: string, newQuery: boolean, runningTotal: number, nextToken: string, rowId: string | undefined): Promise<string> {
    const { Client: PGClient } = require("pg");
    const pgclient = new PGClient({
        user: DB_USER,
        host: DB_HOST,
        database: DB_NAME,
        password: DB_PASSWORD,
        port: DB_PORT,
    });
    await pgclient.connect();

    let returnRowId = rowId;

    if (newQuery) {
        // This is a fresh pagination sequence
        const result = await pgclient.query(`INSERT INTO ${tableName}(total, next, start_time, latest_time) VALUES(${runningTotal}, '${nextToken}', now(), now()) RETURNING id;`);
        returnRowId = result.rows[0].id;
        console.log(`Inserted row, ${result.rowCount} rows updated, id = ${rowId}`);
    } else {
        // otherwise, we are midquery, we need to update the db state. We'll set next_token to empty string here if all done
        if (_.isEmpty(nextToken)) {
            nextToken = '';
        }
        const result = await pgclient.query(`UPDATE ${tableName} SET next = '${nextToken}', latest_time = now(), total = '${runningTotal}' where ID = '${rowId}';`);
        console.log(`Updated id=${rowId}, next=${nextToken}. ${result.rowCount} rows updated`);
    }
    await pgclient.end();
    return returnRowId!;

}

