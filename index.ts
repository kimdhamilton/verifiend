import * as pulumi from "@pulumi/pulumi";
import * as aws from "@pulumi/aws";
import * as awsx from "@pulumi/awsx";
import * as _ from "lodash";

import { getQueryStatus, getTimestampCounts, saveQueryStatus, saveRawCounts } from './pgConnector';
import { diff } from './tasks';
import { Followee } from './types';
import { getResults, getFollowingCount } from './twitterConnector';
import { QueryInfo, getQueryInfo } from './queryInfo';

const verifiedCounts = new aws.s3.Bucket("verifiedCounts");
export const verifiedCountsBucket = verifiedCounts.bucket;
export const latestCountsFileName = 'latest-counts';

const verifiedAccounts = new aws.s3.Bucket("verifiedAccounts");
export const verifiedAccountsBucket = verifiedAccounts.bucket;

const samTweets = new aws.s3.Bucket("samTweets");
export const samTweetsBucket = samTweets.bucket;

// this lets us write to s3 in reverse chron order
const MAX_DATETIME = Number(315360000000000);


async function fetchFollowing() {
    const queryInfo = getQueryInfo('following', verifiedAccountsBucket.get());
    return fetchAll(queryInfo);
}
async function fetchTweets() {
    const queryInfo = getQueryInfo('tweets', samTweetsBucket.get());
    return fetchAll(queryInfo);
}
async function fetchAll(queryInfo: QueryInfo) {

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
                console.log(`All done! Found ${resultsArr.length} results`);
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

async function readLatestsCounts(bucketName: string) {
    const s3 = new aws.sdk.S3();
    // Just need the latest; this works since we write in reverse chron order
    /*const name = await s3.listObjectsV2({
        Bucket: bucketName,
        MaxKeys: 1
    }).promise();
    const key = name.Contents?.shift()?.Key;
    if (!key) {
        console.error("couldn't find verified counts in s3");
        return [];
    }*/
    const obj = await s3.getObject({
        Bucket: bucketName,
        Key: latestCountsFileName
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

async function dumpCountsToS3() {
    try {
        let timestampCounts = await getTimestampCounts();

        /*
        const timestampInMs = Number(new Date().getTime());
        // always write the latest info as the earliest file, because s3 always wants to return ascending
        const fileNameNumeric = MAX_DATETIME - timestampInMs;
        const fileName = `${fileNameNumeric}`;
        await writeToS3(verifiedCountsBucket.get(), fileName, JSON.stringify(timestampCounts));
        */

        await writeToS3(verifiedCountsBucket.get(), latestCountsFileName, JSON.stringify(timestampCounts));


    } catch (e: any) {
        console.error(e);
    } finally {
        console.log("finished");
    }
}

async function diffBatches(id1: string, id2: string) {
    const bucketName = verifiedAccountsBucket.get();
    const f1 = `${id1}-merged`;
    const f2 = `${id2}-merged`;

    const s3 = new aws.sdk.S3();
    const obj1 = await s3.getObject({
        Bucket: bucketName,
        Key: f1
    }).promise();

    const obj2 = await s3.getObject({
        Bucket: bucketName,
        Key: f2
    }).promise();

    const res1: Followee[] = JSON.parse(obj1.Body?.toString('utf-8')!);
    const res2: Followee[] = JSON.parse(obj2.Body?.toString('utf-8')!);

    const { deletedValues, addedValues } = diff(res1, res2);

    await writeToS3(bucketName, `${id1}_${id2}-deleted`, JSON.stringify(deletedValues));
    await writeToS3(bucketName, `${id1}_${id2}-added`, JSON.stringify(addedValues));
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
        const obj = await s3.getObject({
            Bucket: bucketName,
            Key: f.Key!
        }).promise();
        const fileContents: any[] = JSON.parse(obj.Body?.toString('utf-8')!);
        fileContents.forEach((obj) => {
            verified.set(obj.id, obj);
        });
    }
    const values = Array.from(verified.values());
    await writeToS3(bucketName, `${id}-merged`, JSON.stringify(values));
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

// Create an API endpoint
const endpoint = new awsx.apigateway.API("verifiend", {
    routes: [
        /* {
             path: "/",
             localPath: "www",
         },*/
        {
            path: "/counts",
            method: "GET",
            eventHandler: async (event) => {
                console.log(`Getting latest verified counts`);
                const latest = await readLatestsCounts(verifiedCountsBucket.get());
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
        }],
});

export const restEndpoint = endpoint.url;

aws.cloudwatch.onSchedule("verified-following-snapshot", "cron(36 * * * ? *)", rawCount);

aws.cloudwatch.onSchedule("verified-following-dump-to-s3", "cron(55 * * * ? *)", dumpCountsToS3);

aws.cloudwatch.onSchedule("verified-following-details-snapshot", "cron(0,15,30,45 * ? * * *)", fetchFollowing);

// aws.cloudwatch.onSchedule("sbf-tweets-snapshot", "cron(33 * * * ? *)", fetchTweets);