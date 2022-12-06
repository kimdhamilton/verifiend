import * as aws from "@pulumi/aws";
import * as awsx from "@pulumi/awsx";
import * as _ from "lodash";

import { getQueryStatus, getHourlyCounts, saveQueryStatus, saveRawCounts, getDailyCounts } from './pgConnector';
import { diff } from './tasks';
import { Followee } from './types';
import { getResults, getFollowingCount } from './twitterConnector';
import { QueryInfo, getQueryInfo } from './queryInfo';

const verifiedCounts = new aws.s3.Bucket("verifiedCounts");
export const VERIFIED_COUNTS_BUCKET = verifiedCounts.bucket;
export const HOURLY_COUNTS_FILE_NAME = 'hourly.json';
export const DAILY_COUNTS_FILE_NAME = 'daily.json';
export const MERGED_SUFFIX = 'merged';
export const ADDED_SUFFIX = 'added';
export const DELETED_SUFFIX = 'deleted';

const verifiedAccounts = new aws.s3.Bucket("verifiedAccounts");
export const verifiedAccountsBucket = verifiedAccounts.bucket;

async function fetchVerifiedAccounts() {
    const queryInfo = getQueryInfo(verifiedAccountsBucket.get());
    return pagedFetch(queryInfo);
}

// experiment / distraction
const samTweets = new aws.s3.Bucket("samTweets");
export const samTweetsBucket = samTweets.bucket;
// end distraction

async function pagedFetch(queryInfo: QueryInfo) {

    try {
        const status = await getQueryStatus(queryInfo.getDbTableName());

        let next = status?.next;
        let newQuery = _.isEmpty(next);
        let nextToken = newQuery ? '' : next!;
        let rowId = status?.id;
        let runningTotal = 0;

        if (!newQuery && status?.total) {
            runningTotal = Number(status.total);
        }

        console.log(`Current query status: ${JSON.stringify(status)}. Is this a new query? ${newQuery}`);

        let resultsArr: any[];
        [resultsArr, nextToken] = await getResults(nextToken, queryInfo)
        if (resultsArr.length > 0) {
            runningTotal += Number(resultsArr.length);
            console.log(`Running total: ${runningTotal}`);
            rowId = await saveQueryStatus(queryInfo.getDbTableName(), newQuery, runningTotal, nextToken, rowId);

            // dump batch of results to s3
            const timestampInMs = new Date().getTime();
            const fileName = `${rowId}-${timestampInMs}`;
            await writeToS3(queryInfo.getS3BucketName(), fileName, JSON.stringify(resultsArr));
            if (_.isEmpty(nextToken)) {
                console.log(`All done! Found ${resultsArr.length} results. Merging results`);
                // TODO: later add retry logic by moving this up to an AWS pipeline
                await mergeBatch(rowId);
            }
        } else {
            // just log and clean up resources; we'll try again next time
            console.warn('This is either caused by an intermittent error or rate limiting when we got no results. No state to preserve here.');
        }
    } catch (e: any) {
        console.error(e);
    } finally {
        console.log("fetchAll complete...for now");
    }
}

async function writeToS3(bucket: any, fileName: string, content: any) {
    const s3 = new aws.sdk.S3();
    const obj = await s3.putObject({
        Bucket: bucket,
        Key: fileName,
        Body: content,
    }).promise();

    console.log(`Wrote object to s3 with name: ${fileName}`);
}

async function readS3FileAsJson(bucketName: string, fileName: string): Promise<any> {
    const s3 = new aws.sdk.S3();
    const obj = await s3.getObject({
        Bucket: bucketName,
        Key: fileName
    }).promise();
    return JSON.parse(obj.Body?.toString('utf-8')!);
}

async function rawCount() {
    const verifiedCount = await getFollowingCount();
    console.log(`Number of verified users based on verified following ${verifiedCount}`);
    try {
        await saveRawCounts(verifiedCount);
    } catch (e: any) {
        console.error(e);
    } finally {
        console.log("finished");
    }
}

// Dump latest counts to s3
// 1. Last 3 days hourly
// 2. Since beginning daily
async function dumpCountsToS3() {
    try {
        // get hourly for last 3 days
        const now = new Date();
        const threeDaysAgo = new Date().setDate(now.getDate() - 3);
        let timestampCounts = await getHourlyCounts(threeDaysAgo);
        await writeToS3(VERIFIED_COUNTS_BUCKET.get(), HOURLY_COUNTS_FILE_NAME, JSON.stringify(timestampCounts));

        // get daily averages
        let dailyCounts = await getDailyCounts();
        await writeToS3(VERIFIED_COUNTS_BUCKET.get(), DAILY_COUNTS_FILE_NAME, JSON.stringify(dailyCounts));
    } catch (e: any) {
        console.error(e);
        // can ignore errors, but log them just in case
    } finally {
        console.log("finished");
    }
}

async function diffBatches(id1: string, id2: string) {
    const bucketName = verifiedAccountsBucket.get();
    const f1 = `${id1}-${MERGED_SUFFIX}`;
    const f2 = `${id2}-${MERGED_SUFFIX}`;

    const res1: Followee[] = await readS3FileAsJson(bucketName, f1);
    const res2: Followee[] = await readS3FileAsJson(bucketName, f2);

    const { deletedValues, addedValues } = diff(res1, res2);

    await writeToS3(bucketName, `${id1}_${id2}-${DELETED_SUFFIX}`, JSON.stringify(deletedValues));
    await writeToS3(bucketName, `${id1}_${id2}-${ADDED_SUFFIX}`, JSON.stringify(addedValues));
}

async function mergeBatch(id: string) {
    const bucketName = verifiedAccountsBucket.get();
    const prefix = `${id}-`;

    const s3 = new aws.sdk.S3();
    // Just need the latest; this works since we write in reverse chron order
    const objs = await s3.listObjectsV2({
        Bucket: bucketName,
        Prefix: prefix
    }).promise();

    let verified = new Map<string, Followee>();
    for (const f of objs.Contents!) {
        const fileContents: any[] = await readS3FileAsJson(bucketName, f.Key!);
        fileContents.forEach((obj) => {
            verified.set(obj.id, obj);
        });
    }
    const values = Array.from(verified.values());
    await writeToS3(bucketName, `${id}-${MERGED_SUFFIX}`, JSON.stringify(values));
}

export const mergeHandler = new aws.lambda.CallbackFunction("merge-handler", {
    memorySize: 4096,
    callback: async (ev: any, ctx) => {
        const batchId = ev.batchId;
        console.log(`Merging batch with id=${batchId}`);
        await mergeBatch(batchId);
        console.log("Finished merging batch");
        return true;
    },
});

export const diffHandler = new aws.lambda.CallbackFunction("diff-handler", {
    memorySize: 4096,
    callback: async (ev: any, ctx) => {
        console.log(JSON.stringify(ev));
        const batchId1 = ev.batchId1;
        const batchId2 = ev.batchId2;
        console.log(`Diffing batches: ${batchId1}, ${batchId2}`);
        await diffBatches(batchId1, batchId2);
        console.log("Finished diffing batches");
        return true;
    },
});

export const backfillMergeHandler = new aws.lambda.CallbackFunction("backfill-merge-handler", {
    memorySize: 4096,
    callback: async (ev: any, ctx) => {
        const minBatchId = Number(ev.minBatchId);
        const maxBatchId = Number(ev.maxBatchId);
        console.log(`Merging batches with minBatchId=${minBatchId}, maxBatchId=${maxBatchId}`);
        for (let batchId = minBatchId; batchId <= maxBatchId; batchId++) {
            await mergeBatch(batchId.toString());
        }
        console.log(`Finished merging batches with minBatchId=${minBatchId}, maxBatchId=${maxBatchId}`);
        return true;
    },
});


export const backfillDiffs = new aws.lambda.CallbackFunction("backfill-diff-handler", {
    memorySize: 4096,
    callback: async (ev: any, ctx) => {
        console.log(JSON.stringify(ev));
        const minBatchId = Number(ev.minBatchId);
        const maxBatchId = Number(ev.maxBatchId);
        console.log(`Diffing batches with minBatchId=${minBatchId}, maxBatchId=${maxBatchId}`);
        for (let batchId = minBatchId; batchId < maxBatchId; batchId++) {
            await diffBatches(batchId.toString(), (batchId + 1).toString());
        }
        console.log(`Fininshed diffing batches with minBatchId=${minBatchId}, maxBatchId=${maxBatchId}`);
        return true;
    },
});

// Create an API endpoint
const endpoint = new awsx.apigateway.API("verifiend", {
    routes: [
        /* {
             path: "/",
             localPath: "www",
         },*/
        {
            path: "/counts", // TODO: refactor and change this path to daily
            method: "GET",
            eventHandler: async (event) => {
                console.log(`Getting daily verified counts, path: ${JSON.stringify, event}`);
                const latest = await readS3FileAsJson(VERIFIED_COUNTS_BUCKET.get(), DAILY_COUNTS_FILE_NAME);
                console.log(`Finished`);
                return {
                    statusCode: 200,
                    headers: {
                        "Access-Control-Allow-Headers": "Content-Type",
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Methods": "GET"
                    },
                    body: JSON.stringify(latest),
                };
            },
        },
        {
            path: "/hourly",
            method: "GET",
            eventHandler: async (event) => {
                console.log(`Getting hourly verified counts`);
                const latest = await readS3FileAsJson(VERIFIED_COUNTS_BUCKET.get(), HOURLY_COUNTS_FILE_NAME);
                console.log(`Finished`);
                return {
                    statusCode: 200,
                    headers: {
                        "Access-Control-Allow-Headers": "Content-Type",
                        "Access-Control-Allow-Origin": "*",
                        "Access-Control-Allow-Methods": "GET"
                    },
                    body: JSON.stringify(latest),
                };
            },
        }
    ],
});

export const restEndpoint = endpoint.url;

aws.cloudwatch.onSchedule("verified-following-snapshot", "cron(36 * * * ? *)", rawCount);

aws.cloudwatch.onSchedule("verified-following-dump-to-s3", "cron(55 * * * ? *)", dumpCountsToS3);

aws.cloudwatch.onSchedule("verified-following-details-snapshot", "cron(0,15,30,45 * ? * * *)", fetchVerifiedAccounts);

// aws.cloudwatch.onSchedule("sbf-tweets-snapshot", "cron(33 * * * ? *)", fetchTweets);