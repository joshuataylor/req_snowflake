<p align="center">
  <a href="https://github.com/joshuataylor/req_snowflake">
    <img alt="req_snowflake" src="https://user-images.githubusercontent.com/225131/175007256-0f3e5afd-8e90-47ad-a836-a38c57bf28ec.png" width="50">
  </a>
</p>

# req_snowflake

**NOTE: THIS DRIVER/CONNECTOR IS NOT OFFICIALLY AFFILIATED WITH SNOWFLAKE, NOR HAS OFFICIAL SUPPORT FROM THEM.**

An Elixir driver for [Snowflake](https://www.snowflake.com/), the cloud data platform.

Also has support for both pure-Elixir using JSON, or decoding Arrow files via [snowflake_arrow_elixir](), a Rust library which decodes the Arrow streaming file into Elixir.

## Table of Contents

- [Usage](#usage)
- [What this is](#what-this-is)
- [Features](#features)
- [Options](#options)
- [Short term Roadmap](#short-term-roadmap)
- [Medium term roadmap](#medium-term-roadmap)
- [Thanks](#thanks)

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
  schema: "myschema" # optional,
  session_parameters: %{} # Passing in session parameters from
  parallel_downloads: 10 # optional, but recommended. Defaults to 5 (what the other connectors default to).
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

## What this is

It uses the Snowflake REST API to communicate with Snowflake, with an earlier version set for JSON (with support for Arrow if using [snowflake_arrow](https://github.com/joshuataylor/snowflake_arrow).
The REST API is used by the Python, Golang, NodeJS and other languages to send requests to Snowflake, so it is stable and changes are communicated.

This library does not use the [Snowflake SQL API](https://docs.snowflake.com/en/developer-guide/sql-api/index.html), which is limited in its implementation and features.

Right now the library doesn't support [MFA](https://docs.snowflake.com/en/user-guide/security-mfa.html), so you'll need to either use [private key auth](https://docs.snowflake.com/en/user-guide/odbc-parameters.html#using-key-pair-authentication) or connect using a username & password. A private key auth is highly recommended as you can rotate passwords easier.

One of the major notes when using Ecto is you will need to enable Snowflakes `QUOTED_IDENTIFIERS_IGNORE_CASE` setting, which you can find here: https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html#third-party-tools-and-case-insensitive-identifier-resolution

Note that this can be done on an account or if needed on a session level which you can set below.

## Features
* Running queries and inserts.
* JSON row decoding, using Flow this is much faster to decode. It's recommended to also use Jiffy, benchmarking shows a 2x speedup due to the size of the JSON files Snowflake returns.
* Arrow row decoding, using [snowflake_arrow](https://github.com/joshuataylor/snowflake_arrow)
* Passing async queries and getting back a query ID, or polling for results

## Options
There are a lot of options that you can pass in, and you can also pass in [Snowflake Session Parameters](https://docs.snowflake.com/en/sql-reference/parameters.html) as documented below.

- snowflake_query *string* **required**

  Your snowflake query. `select L_ORDERKEY, L_PARTKEY from snowflake_sample_data.tpch_sf1.lineitem limit 2`

- username *string* **required**

  Your snowflake username.

- password *string* **required**

  Your snowflake password.

- account_name  *string* **required**

  Your account name, this is found before the region name in the URL. `https://abc1234.us-east-1.snowflakecomputing.com` would be `abc1234` .

- region *string* **required**

  Your snowflake region, this is found after your account name.

- arrow (boolean) *optional*

  Whether or not to use Arrow. You must have snowflake_arrow included in your project for this to work.
- cache_token *optional*

  Cache the login token between queries, for up to 10 minutes. 10 minutes is the standard login token time for Snowflake.
  If you change a parameter (apart from the query) this will relog you in.
- warehouse *optional*
  The warehouse to use. If none is provided, will use the users default warehouse

- role *optional*

  The role to use. If none is provided, will use the users default role
- database *optional*

  The database to use. If none is provided, will use the users default database
- schema **string** *optional*

  The schema to use. If none is provided, will use the users default schema
- application_name **string** *optional*

  Application name to pass. By default will not use an application name.
- bindings **map** *optional*

  Any bindings to pass.
- session_parameters **map** *optional*

  You can pass any session parameters from https://docs.snowflake.com/en/sql-reference/parameters.html.

  Example: `session_parameters: %{ROWS_PER_RESULTSET: 50}` will return 50 results only.

- parallel_downloads **integer** *optional*

  How many parallel downloads to perform for s3. This defaults to 5, which is the default for other connectors.
- async **boolean** *optional*

  Will run the query in async mode, returning you the query ID.

- async_poll **boolean** *optional*

  Will run the query in async mode, then poll every 5000ms (unless defined by `async_poll_interval`) for the result.

- async_poll_interval **integer**  *optional*

  Will run the query in async mode, then poll every interval milliseconds.

- async_poll_timeout **integer** *optional*

  How many times it will try to poll for the result before giving up.

- download_chunks **boolean** *optional*

  Whether to download the chunks or just return the base64.

- return_dataframe **boolean** *optional*

  Whether to return the results as rows when using Arrow or return the dataframe.

- json_library **module** *optional*

  When decoding JSON, jiffy has shown to be 2x faster and use less memory than Jason for larger JSON blobs, as Snowflake can send large JSON files.
  Examples: `json_library: Jason` or `json_library: :jiffy`. :jiffy is an atom because it's an Erlang library.
  Defaults to JSON.

## Short term Roadmap
- Add this to `db_connection_snowflake` as a generic Snowflake library for db_connection
- Add this to `ecto_snowflake` as a Snowflake library for Ecto.
- Integrate with kino_db

## Medium term roadmap
- Add support for MFA
- Add support for private key auth
- Add support for [telemetry](https://github.com/beam-telemetry/telemetry)

## Thanks
I just want to thank the opensource community, especially dbconnection/ecto/ecto_sql/postgrex for being amazing, and 
being able to copy most of the decoding code from that. I also want to thank the [@wojtekmach](https://github.com/wojtekmach) for the awesome work on req, and the help converting this to req.

Thanks to [@lewisvrobinson](https://github.com/lewisvrobinson) for the logo.

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