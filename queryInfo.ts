import * as pulumi from "@pulumi/pulumi";

let config = new pulumi.Config();

const DB_TABLE_NAME_QUERY_STATUS = config.require("querystatus_tn");

const MAX_ITERATIONS_FOLLOWING = 14;

const MAX_RESULTS_FOLLOWING = 1000;

export const TWITTER_VERIFIED_ACCOUNT_ID = "63796828";

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

export function getQueryInfo(bucketName: string): QueryInfo {
    return new QueryInfo('following', TWITTER_VERIFIED_ACCOUNT_ID, MAX_RESULTS_FOLLOWING, MAX_ITERATIONS_FOLLOWING, DB_TABLE_NAME_QUERY_STATUS, bucketName);
}