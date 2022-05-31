defmodule ReqSnowflake.InsertTest do
  use ExUnit.Case, async: false
  alias ReqSnowflake.Result

  setup do
    bypass = Bypass.open()
    Application.put_env(:req_snowflake, :snowflake_hostname, "127.0.0.1")
    Application.put_env(:req_snowflake, :snowflake_url, "http://127.0.0.1:#{bypass.port}")
    Application.put_env(:req_snowflake, :snowflake_uuid, "0000000-0000-0000-0000-000000000000")

    {:ok, %{bypass: bypass}}
  end

  test "Can insert to Snowflake", %{bypass: bypass} do
    Bypass.expect(bypass, "POST", "/session/v1/login-request", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/snowflake_valid_login_response.json"
        ])
      )
      |> json(conn, 200)
    end)

    Bypass.expect(bypass, "POST", "/queries/v1/query-request", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/snowflake_insert_query_response.json"
        ])
      )
      |> json(conn, 200)
    end)

    response =
      Req.new()
      |> ReqSnowflake.attach(
        username: "myuser",
        password: "hunter2",
        account_name: "elixir",
        region: "us-east-1",
        warehouse: "compute_wh",
        role: "somerole",
        database: "snowflake_sample_data",
        schema: "tpch_sf1"
      )
      |> Req.post!(
        snowflake_query: "INSERT INTO \"foo\".\"bar\".\"baz\" (\"hello\") VALUES (?)",
        bindings: %{"1" => %{type: "TEXT", value: "xxx"}}
      )

    assert response.body == %Result{
             columns: ["number of rows inserted"],
             messages: [],
             metadata: [],
             num_rows: 1,
             rows: [[1]],
             success: true
           }
  end

  defp json(data, conn, status) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, data)
  end
end
