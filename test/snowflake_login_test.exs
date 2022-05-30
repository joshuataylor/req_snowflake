defmodule ReqSnowflakeLogin.LoginTest do
  use ExUnit.Case, async: true

  test "Can login to Snowflake using valid credentials" do
    bypass = Bypass.open()
    Application.put_env(:req_snowflake, :snowflake_hostname, "127.0.0.1")
    Application.put_env(:req_snowflake, :snowflake_url, "http://127.0.0.1:#{bypass.port}")

    Bypass.expect(bypass, "POST", "/session/v1/login-request", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/snowflake_valid_login_response.json"
        ])
      )
      |> json(conn, 200)
    end)

    Bypass.expect(bypass, "POST", "/console/bootstrap-data-request", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/snowflake_bootstrap_data_request_response.json"
        ])
      )
      |> json(conn, 200)
    end)

    response =
      Req.new(http_errors: :raise)
      |> ReqSnowflakeLogin.attach(
        username: "elixir",
        password: "elixir123",
        account_name: "elixir123",
        region: "us-east-1",
        warehouse: "COMPUTE_WH",
        role: "FOOBAR",
        database: "SNOWFLAKE_SAMPLE_DATA",
        schema: "TPCH_SF1"
      )
      |> Req.post!(
        url: "http://127.0.0.1:#{bypass.port}/console/bootstrap-data-request",
        json: %{"dataKinds" => ["DATABASES"]}
      )

    assert response.status == 200
    refute response.body["code"]

    assert length(response.body["data"]["databases"]) > 0
    assert hd(response.body["data"]["databases"])["name"]
    assert hd(response.body["data"]["databases"])["id"]
  end

  #  test "Attempting to login to Snowflake for a valid account returns incorrect username or password RuntimeError" do
  #    assert_raise RuntimeError, ~r/Incorrect username or password was specified/, fn ->
  #      Req.new(http_errors: :raise)
  #      |> ReqSnowflakeLogin.attach(
  #        username: "invalid",
  #        password: "invalid",
  #        account_name: account_name,
  #        region: region
  #      )
  #      |> Req.post!(
  #        url:
  #          "https://#{account_name}.#{region}.snowflakecomputing.com/console/bootstrap-data-request",
  #        json: %{"dataKinds" => ["DATABASES"]}
  #      )
  #    end
  #  end
  #
  #  test "Attempting to login to Snowflake for an invalid account name returns an error" do
  #    assert_raise RuntimeError, ~r/403 Forbidden/, fn ->
  #      Req.new(http_errors: :raise)
  #      |> ReqSnowflakeLogin.attach(
  #        username: "invalid",
  #        password: "invalid",
  #        account_name: "invalid",
  #        region: "us-east-1"
  #      )
  #      |> Req.post!(
  #        url: "https://invalid.us-east-1.snowflakecomputing.com/console/bootstrap-data-request",
  #        json: %{"dataKinds" => ["DATABASES"]}
  #      )
  #    end
  #  end

  #  test "Not going to Snowflake won't try and log you in" do
  #    bypass = Bypass.open()
  #    Application.put_env(:req_snowflake, :snowflake_hostname, "127.0.0.1")
  #    Application.put_env(:req_snowflake, :snowflake_url, "http://127.0.0.1:#{bypass.port}")
  #
  #    Bypass.expect(bypass, "POST", "/session/v1/login-request", fn conn ->
  #      json =
  #        File.read!(
  #          Path.join([
  #            :code.priv_dir(:req_snowflake),
  #            "testing/snowflake_valid_login_response.json"
  #          ])
  #        )
  #
  #      json(conn, 200, json)
  #    end)
  #
  #    response =
  #      Req.new(http_errors: :raise)
  #      |> ReqSnowflakeLogin.attach(
  #        username: "x",
  #        password: "y",
  #        account_name: "z",
  #        region: "x",
  #        warehouse: "a",
  #        role: "x",
  #        database: "a",
  #        schema: "a"
  #      )
  #      |> Req.get!(url: "https://elixir-lang.org/")
  #
  #    assert response.status == 200
  #  end

  defp json(data, conn, status) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, data)
  end
end
