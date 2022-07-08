defmodule ReqSnowflakeLogin.LoginIntegrationTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  setup do
    # Ensure that we we are not using bypass.
    :application.unset_env(:req_snowflake, :snowflake_hostname)
    :application.unset_env(:req_snowflake, :snowflake_url)
  end

  test "Can login to Snowflake using valid credentials" do
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
      |> ReqSnowflakeLogin.attach(
        username: username,
        password: password,
        account_name: account_name,
        region: region,
        warehouse: warehouse,
        role: role,
        database: database,
        schema: schema
      )
      |> Req.post!(
        url:
          "https://#{ReqSnowflake.Snowflake.snowflake_host(account_name, region)}/console/bootstrap-data-request",
        json: %{"dataKinds" => ["DATABASES"]}
      )

    assert response.status == 200
    refute response.body["code"]

    assert length(response.body["data"]["databases"]) > 0
    assert hd(response.body["data"]["databases"])["name"]
    assert hd(response.body["data"]["databases"])["id"]
  end

  test "Attempting to login to Snowflake for a valid account returns incorrect username or password RuntimeError" do
    account_name = System.get_env("SNOWFLAKE_ACCOUNT_NAME")
    region = System.get_env("SNOWFLAKE_REGION")

    assert_raise RuntimeError, ~r/Incorrect username or password was specified/, fn ->
      Req.new(http_errors: :raise)
      |> ReqSnowflakeLogin.attach(
        username: "invalid",
        password: "invalid",
        account_name: account_name,
        region: region
      )
      |> Req.post!(
        url:
          "https://#{ReqSnowflake.Snowflake.snowflake_host(account_name, region)}/console/bootstrap-data-request",
        json: %{"dataKinds" => ["DATABASES"]}
      )
    end
  end

  test "Attempting to login to Snowflake for an invalid account name returns an error" do
    assert_raise RuntimeError, ~r/403 Forbidden/, fn ->
      Req.new(http_errors: :raise)
      |> ReqSnowflakeLogin.attach(
        username: "invalid",
        password: "invalid",
        account_name: "invalid",
        region: "us-east-1"
      )
      |> Req.post!(
        url: "https://invalid.us-east-1.snowflakecomputing.com/console/bootstrap-data-request",
        json: %{"dataKinds" => ["DATABASES"]}
      )
    end
  end

  test "Not going to Snowflake won't try and log you in" do
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
      |> ReqSnowflakeLogin.attach(
        username: username,
        password: password,
        account_name: account_name,
        region: region,
        warehouse: warehouse,
        role: role,
        database: database,
        schema: schema
      )
      |> Req.get!(url: "https://elixir-lang.org/")

    assert response.status == 200
  end
end
