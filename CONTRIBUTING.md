# Contributing

Thanks so much for wanting to contribute to this library. We want this to be a community effort, and we want this library
to also be a good source of learning how more complicated req plugins work.

If you have a bug to report, please open an issue outlining what you were trying to do, and what you expected to happen. If you
have a reproduction, even better! We know that sharing internal data can tricky, so if you could make a smaller table/view
if you are finding something isn't working as expected please also provide a DML statement that is failing.

If you would like to propose a feature, please open an issue with a description of what you would like to see. Feel free to
also open a Pull Request, but we'd love to iterate first, no pressure either way though!

## Snowflake

You can signup for a free 30 day trial, which does not require a credit card, [here](https://signup.snowflake.com/). This should
get you going. Please note that we don't receive anything for you signing up at this stage (we're just fans of Snowflake who want to use it in Elixir)
so you can also just use your existing account :).

### Setup
The easiest way to see what is going on is to use a tool like [mitmproxy](https://mitmproxy.org/), which you can then use the following command:
```bash
mitmweb --mode reverse:https://youraccount.us-east-1.snowflakecomputing.com
```

This will then forward any requests to http://127.0.0.1:8080/ to your Snowflake. You can then use mitmweb UI at http://127.0.0.1:8081 to see what is happening.

You can then set the config options:
```elixir
Application.put_env(:req_snowflake, :snowflake_hostname, "127.0.0.1")
Application.put_env(:req_snowflake, :snowflake_url, "http://127.0.0.1:8080")
```

This will forward all traffic over mitmweb.

If you want to create a small module to test queries, doing the following is a good starting point, and you can use environment variables
```elixir
defmodule ReqSnowflake.Testing do
  def query_snowflake() do
    Application.put_env(:req_snowflake, :snowflake_hostname, "127.0.0.1")
    Application.put_env(:req_snowflake, :snowflake_url, "http://127.0.0.1:8080")

    username = System.get_env("SNOWFLAKE_USERNAME")
    password = System.get_env("SNOWFLAKE_PASSWORD")
    account_name = System.get_env("SNOWFLAKE_ACCOUNT_NAME")
    region = System.get_env("SNOWFLAKE_REGION")
    warehouse = System.get_env("SNOWFLAKE_WAREHOUSE")
    role = System.get_env("SNOWFLAKE_ROLE")
    database = System.get_env("SNOWFLAKE_DATABASE")
    schema = System.get_env("SNOWFLAKE_SCHEMA")

    response =
      Req.new(http_errors: :raise)
      |> ReqSnowflake.attach(
        username: username,
        password: password,
        account_name: account_name,
        region: region,
        warehouse: warehouse,
        role: role,
        database: database,
        schema: schema
      )
      |> Req.post!(snowflake_query: "select * from foo.bar.test_data_tiny limit 10")
  end
end
```

Using `iex -S mix`, you can then run this:
```elixir
ReqSnowflake.Testing.query_snowflake()
```