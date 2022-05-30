# ReqSnowflake
[Req](https://github.com/wojtekmach/req) plugin for [Snowflake](https://www.snowflake.com).

**WIP, NOT PRODUCTION READY YET**

NOTE: THIS DRIVER/CONNECTOR IS NOT OFFICIALLY AFFILIATED WITH SNOWFLAKE, NOR HAS OFFICIAL SUPPORT FROM THEM.

A pure-elixir driver for [Snowflake](https://www.snowflake.com/), the cloud data platform.

## Usage

```elixir
Mix.install([
  {:req_snowflake, github: "joshuataylor/req_snowflake"}
])

# With plain string query
Req.new()
|> ReqSnowflake.attach(
  username: "rosebud",
  password: "hunter2",
  account_name: "foobar",
  region: "us-east-1",
  warehouse: "compute_wh", # optional
  role: "myrole", # optional
  database: "mydb", # optional
  schema: "myschema" # optional
)
|> Req.post!(snowflake_query: "select L_ORDERKEY, L_PARTKEY from snowflake_sample_data.tpch_sf1.lineitem limit 2").body
#=>
# %ReqSnowflake.Result{
#   columns: ["L_ORDERKEY", "L_PARTKEY"],
#   num_rows: 2,
#   rows: [[3_000_001, 14406], [3_000_002, 34422]],
#   success: true
# }

# With query parameters for inserting
Req.new()
|> ReqSnowflake.attach(
  username: "rosebud",
  password: "hunter2",
  account_name: "foobar",
  region: "us-east-1",
  warehouse: "compute_wh", # optional
  role: "myrole", # optional
  database: "mydb", # optional
  schema: "myschema" # optional
)
|> Req.post!(snowflake_query: "select L_ORDERKEY, L_PARTKEY from snowflake_sample_data.tpch_sf1.lineitem limit 2").body
|> Req.post!(
  snowflake_query: "INSERT INTO \"foo\".\"bar\".\"baz\" (\"hello\") VALUES (?)",
  bindings: %{"1" => %{type: "TEXT", value: "xxx"}}
)
#=>
# %ReqSnowflake.Result {
#   columns: ["number of rows inserted"],
#   num_rows: 1,
#   rows: [[1]],
#   success: true
# }
```

# What this is

It uses the Snowflake REST API to communicate with Snowflake, with an earlier version set for JSON.
There isn't an Elixir Arrow library (yet!), so it seems that setting an earlier Java version seems
to give us back JSON results. The REST API is used by the Python, Golang, NodeJS and other languages to
send requests to Snowflake, so it is stable and shouldn't just randomly break.  I've been using `snowflake_elixir`
(predecessor to this package) in production for 18 months and the API hasn't changed once.

This library does not use the [Snowflake SQL API](https://docs.snowflake.com/en/developer-guide/sql-api/index.html), which is
limited in its implementation and features. We might as well use the REST API.

Right now the library doesn't support [MFA](https://docs.snowflake.com/en/user-guide/security-mfa.html), so you'll
need to either use [private key auth](https://docs.snowflake.com/en/user-guide/odbc-parameters.html#using-key-pair-authentication) or
connect using a username & password. A private key auth is highly recommended as you can rotate passwords easier.

Once I have time I will write a library that will use Arrow responses, as [apparently it's faster](https://www.snowflake.com/blog/fetching-query-results-from-snowflake-just-got-a-lot-faster-with-apache-arrow/)

One of the major notes when using Ecto is you will need to enable Snowflakes `QUOTED_IDENTIFIERS_IGNORE_CASE` setting, which you can
find here: https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html#third-party-tools-and-case-insensitive-identifier-resolution

Note that this can be done on an account or if needed on a session level which you can set below.

## Features
* Queries

## Short term Roadmap
- Add async queries back, and add option to poll for response
- Document the different insert types
- Add support for caching the token for the user/password/role, with default as `true`
- Pass the review gambit, incorporate feedback from the community :)
- Add this to `snowflake_elixir` as a generic Snowflake library
- Document the library thoroughly

## Medium term roadmap
- Add support for MFA
- Add support for private key auth
- Add support for the way Snowflake does Arrow streaming, using Arrow Rust with Rustler support we can even provide precompiled binaries! (maybe as a different repository)

## Thanks
I just want to thank the opensource community, especially dbconnection/ecto/ecto_sql/postgrex for being amazing, and 
being able to copy most of the decoding code from that. I also want to thank the [@wojtekmach](https://github.com/wojtekmach) for the awesome work on req :).

## License

Copyright (C) 2022 Josh Taylor

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.