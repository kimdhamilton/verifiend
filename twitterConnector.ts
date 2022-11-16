import * as pulumi from "@pulumi/pulumi";
import * as _ from "lodash";
import { QueryInfo, TWITTER_VERIFIED_ACCOUNT_ID } from "./queryInfo";

let config = new pulumi.Config();

const TWITTER_BEARER_TOKEN = config.require("twittertoken");
const SLEEP_TIME = config.getNumber("sleeptime") || 500;


function queryKickoff(client: any, nextToken: string, queryInfo: QueryInfo) {
    const args: any = { max_results: queryInfo.getMaxResults() };
    if (!_.isEmpty(nextToken)) {
        args['pagination_token'] = nextToken;
    }
    if (queryInfo.getQueryType() === 'tweets') {
        // // geo.place_id
        return client.tweets.usersIdTimeline(queryInfo.getAccountId(), args);
    } else {
    return  client.users.usersIdFollowing(queryInfo.getAccountId(), args);
    }
}


export async function getResults(nextToken: string, queryInfo: QueryInfo): Promise<[any[], string]> {
    const Client = require("twitter-api-sdk").Client;
    const client = new Client(TWITTER_BEARER_TOKEN);
    const sleep = (ms: number) => new Promise(r => setTimeout(r, ms));

    let resultsArr = [] as any[];

    const results = queryKickoff(client, nextToken, queryInfo);
    let iterationsThisCall = 0;
    try {
        for await (let page of results) {
            const paginator = page.meta;
            nextToken = paginator?.next_token;
            console.log(`PAGINATOR META: ${JSON.stringify(paginator)}`);

            if (page.data) {
                resultsArr = resultsArr.concat(page.data!);
                // break if we've exceeded rate limits or if there are no more results
                if (iterationsThisCall++ >= queryInfo.maxIterations) {
                    console.log(`Reached max iterations; will resume with ${nextToken}`);
                    break;
                }
                if (_.isEmpty(nextToken)) {
                    console.log(`Found all results; can start anew!`);
                    break;
                }
                await sleep(SLEEP_TIME);
            } else {
                // not sure we can reach here; but just in case
                console.log("no more results in this query");
                nextToken = '';
                break;
            }
        }
    } catch (e: any) {
        console.log("We expect this to be a rate limiting error; either way, keep our last state");
        console.error(e);
    } 
    return [ resultsArr, nextToken ];
}


export async function getFollowingCount(): Promise<number> {
    const Client = require("twitter-api-sdk").Client;
    const client = new Client(TWITTER_BEARER_TOKEN);
    const user = await client.users.findUserById(TWITTER_VERIFIED_ACCOUNT_ID, { "user.fields": ["public_metrics"] });
    const verifiedCount = user.data?.public_metrics?.following_count;
    return verifiedCount;
}
