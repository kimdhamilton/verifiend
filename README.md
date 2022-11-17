## Veri-FIEND, the Twitter Verified Verifier

### Goals
- Get hourly counts of Twitter verified users
- Get detailed dumps of the list of Twitter verified users (as rate limits allow)
- Determine additions and delettions

Future Goals include issuing portable Verifiable Credentials attesting a user was verified as of X date.

### Details and assumptions:

#### Determining verified users
The first problem was determining verified users given API rate limits. 

Searching around, the best approach achievable through twitter's apis was to query the list of users [Twitter's "verified" account](https://twitter.com/verified) is following. 

#### Collecting the complete set of verified uesrs

When I started there were 440k verified accounts. The twitter API endpoint to obtain a list of followers caps results at 1k per API call, with a max of 15 requests in a 15-minute interval. This meant I could collect at most 15k users per 15 mintes. With 440k verified acocunts, that means I get a complete snapshot of verified users every 8 hours. 

(With the way the pagination works, there was no clever trick to speed up the query, e.g. by trying to split the query among multiple API accounts). 

For reference this query is:
```
GET https://developer.twitter.com/en/docs/twitter-api/users/follows/api-reference/get-users-id-following
```

#### Determining new and deleted verified accounts
When we collect detailed lists of verified users, we're collecting id, name, and username. We're assuming id is stable and running diffs across batches to determine if a user is added or removed.


### Services

The following are deployed as AWS lambdas
1. hourly dumps of the "following" count for the official twitter "verified" account
2. quarter-hourly fetch of batch of verified users of up to 15000 results to avoid rate limiting. Storing pagination token in between calls to obtain the complete set.
3. at end of a batch (every 8 hours), dump a "merged" json file
    - Dump to S3 (json: id, name, username)
4. when a batch is finished and after merged, we can diff it (by id). Write new and deleted snapshots
5. REST service to serve counts

Using AWS lambdas plus postgres, and S3 for storage

