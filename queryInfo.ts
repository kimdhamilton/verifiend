import * as pulumi from "@pulumi/pulumi";

let config = new pulumi.Config();

const DB_TABLE_NAME_QUERY_STATUS = config.require("querystatus_tn");
const DB_TABLE_NAME_TWEET_STATUS = config.require("tweetstatus_tn");

const MAX_ITERATIONS_FOLLOWING = 14;
const MAX_ITERATIONS_TWEETS = 900;

const MAX_RESULTS_FOLLOWING = 1000;
const MAX_RESULTS_TWEETS = 100;

export const TWITTER_VERIFIED_ACCOUNT_ID = "63796828";
const SBF_ACCOUNT_ID = "1110877798820777986"

export class QueryInfo {
    queryType: string;
    accountId: string;
    maxResults: number;
    maxIterations: number;
    dbTableName: string;
    s3bucketName: string;

    constructor(queryType: string, accountId: string, maxResults: number, maxIterations: number, dbTableName: string, s3BucketName: string) {
        this.queryType = queryType;
        this.accountId = accountId;
        this.maxResults = maxResults;
        this.maxIterations = maxIterations;
        this.dbTableName = dbTableName;
        this.s3bucketName = s3BucketName;
    }
    getQueryType(): string {
        return this.queryType;
    }
    getAccountId(): string {
        return this.accountId;
    }
    getMaxResults(): number {
        return this.maxResults;
    }
    getMaxIterations(): number {
        return this.maxIterations;
    }
    getDbTableName(): string {
        return this.dbTableName;
    }
    getS3BucketName(): string {
        return this.s3bucketName;
    }

}

export function getQueryInfo(queryType: string, bucketName: string): QueryInfo {
    if (queryType === 'tweets') {
        return new QueryInfo('tweets', SBF_ACCOUNT_ID, MAX_RESULTS_TWEETS, MAX_ITERATIONS_TWEETS, DB_TABLE_NAME_TWEET_STATUS, bucketName);
    } else {
        return new QueryInfo('following', TWITTER_VERIFIED_ACCOUNT_ID, MAX_RESULTS_FOLLOWING, MAX_ITERATIONS_FOLLOWING, DB_TABLE_NAME_QUERY_STATUS, bucketName);
    }

}