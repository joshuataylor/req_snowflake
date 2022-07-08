defmodule ReqSnowflake.InsertTest do
  use ReqSnowflake.SnowflakeCase, async: false
  alias ReqSnowflake.Result

  test "Can insert to Snowflake", %{bypass: bypass} do
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
             messages: nil,
             metadata: nil,
             format: "json",
             total_rows: 1,
             rows: [[1]],
             success: true,
             query_id: "11111111-1111-1111-0000-111111111111"
           }
  end

  defp json(data, conn, status) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, data)
  end
end
