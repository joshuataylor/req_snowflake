defmodule ReqSnowflake.QueryTest do
  use ExUnit.Case, async: false

  setup do
    Application.delete_env(:req_snowflake, :snowflake_hostname)
    Application.delete_env(:req_snowflake, :snowflake_url)
    Application.delete_env(:req_snowflake, :snowflake_uuid)

    bypass = Bypass.open()
    Application.put_env(:req_snowflake, :snowflake_hostname, "127.0.0.1")
    Application.put_env(:req_snowflake, :snowflake_url, "http://127.0.0.1:#{bypass.port}")
    Application.put_env(:req_snowflake, :snowflake_uuid, "0000000-0000-0000-0000-000000000000")

    {:ok, %{bypass: bypass}}
  end

  test "Can query Snowflake with a valid query", %{bypass: bypass} do
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
          "testing/snowflake_inline_query_response.json"
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
        schema: "tpch_sf1",
        cache_token: false
      )
      |> Req.post!(
        snowflake_query: "select * from snowflake_sample_data.tpch_sf1.lineitem limit 2"
      )

    assert response.body == %ReqSnowflake.Result{
             columns: [
               "L_ORDERKEY",
               "L_PARTKEY",
               "L_SUPPKEY",
               "L_LINENUMBER",
               "L_QUANTITY",
               "L_EXTENDEDPRICE",
               "L_DISCOUNT",
               "L_TAX",
               "L_RETURNFLAG",
               "L_LINESTATUS",
               "L_SHIPDATE",
               "L_COMMITDATE",
               "L_RECEIPTDATE",
               "L_SHIPINSTRUCT",
               "L_SHIPMODE",
               "L_COMMENT"
             ],
             messages: [],
             metadata: [],
             num_rows: 2,
             rows: [
               [
                 3_000_001,
                 14406,
                 4407,
                 1,
                 "22.00",
                 "29048.80",
                 "0.02",
                 "0.06",
                 "A",
                 "F",
                 Date.from_iso8601!("1993-01-31"),
                 Date.from_iso8601!("1993-03-16"),
                 Date.from_iso8601!("1993-02-28"),
                 "DELIVER IN PERSON",
                 "AIR",
                 "uriously silent patterns across the f"
               ],
               [
                 3_000_002,
                 34422,
                 4423,
                 1,
                 "45.00",
                 "61038.90",
                 "0.06",
                 "0.04",
                 "N",
                 "O",
                 Date.from_iso8601!("1995-09-28"),
                 Date.from_iso8601!("1995-08-27"),
                 Date.from_iso8601!("1995-10-15"),
                 "NONE",
                 "AIR",
                 "al braids wake idly regular a"
               ]
             ],
             success: true
           }
  end

  test "Can query and get data from S3", %{bypass: bypass} do
    Bypass.expect(bypass, "POST", "/session/v1/login-request", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/snowflake_valid_login_response.json"
        ])
      )
      |> json(conn, 200)
    end)

    # Add the chunks to be downloaded via Bypass
    chunks = [
      %{
        "url" => "http://127.0.0.1:#{bypass.port}/s31",
        "rowCount" => 2909,
        "uncompressedSize" => 426_128,
        "compressedSize" => 110_925
      },
      %{
        "url" => "http://127.0.0.1:#{bypass.port}/s32",
        "rowCount" => 247,
        "uncompressedSize" => 36148,
        "compressedSize" => 10189
      }
    ]

    Bypass.expect(bypass, "POST", "/queries/v1/query-request", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/snowflake_s3_query_response.json"
        ])
      )
      |> Jason.decode!()
      |> put_in(["data", "chunks"], chunks)
      |> Jason.encode!()
      |> json(conn, 200)
    end)

    Bypass.expect(bypass, "GET", "/s31", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/s3/response_1.json"
        ])
      )
      |> json_gzip(conn, 200)
    end)

    Bypass.expect(bypass, "GET", "/s32", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/s3/response_2.json"
        ])
      )
      |> json_gzip(conn, 200)
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
        schema: "tpch_sf1",
        cache_token: false
      )
      |> Req.post!(
        snowflake_query: "select * from snowflake_sample_data.tpch_sf1.lineitem limit 2"
      )

    assert response.body == %ReqSnowflake.Result{
             columns: [
               "L_ORDERKEY",
               "L_PARTKEY",
               "L_SUPPKEY",
               "L_LINENUMBER",
               "L_QUANTITY",
               "L_EXTENDEDPRICE",
               "L_DISCOUNT",
               "L_TAX",
               "L_RETURNFLAG",
               "L_LINESTATUS",
               "L_SHIPDATE",
               "L_COMMITDATE",
               "L_RECEIPTDATE",
               "L_SHIPINSTRUCT",
               "L_SHIPMODE",
               "L_COMMENT"
             ],
             messages: [],
             metadata: [],
             num_rows: 4,
             rows: [
               [
                 3_003_586,
                 193_197,
                 755,
                 1,
                 "24.00",
                 "30964.56",
                 "0.03",
                 "0.02",
                 "N",
                 "O",
                 Date.from_iso8601!("1996-05-14"),
                 Date.from_iso8601!("1996-04-19"),
                 Date.from_iso8601!("1996-06-04"),
                 "COLLECT COD",
                 "RAIL",
                 "quests haggle furiously regular, "
               ],
               [
                 3_003_586,
                 54440,
                 6946,
                 2,
                 "49.00",
                 "68327.56",
                 "0.01",
                 "0.05",
                 "N",
                 "O",
                 Date.from_iso8601!("1996-06-11"),
                 Date.from_iso8601!("1996-05-20"),
                 Date.from_iso8601!("1996-06-26"),
                 "DELIVER IN PERSON",
                 "FOB",
                 "heodolites are furio"
               ],
               [
                 3_003_586,
                 164_196,
                 6713,
                 3,
                 "37.00",
                 "46627.03",
                 "0.02",
                 "0.02",
                 "N",
                 "O",
                 Date.from_iso8601!("1996-06-02"),
                 Date.from_iso8601!("1996-06-16"),
                 Date.from_iso8601!("1996-06-28"),
                 "TAKE BACK RETURN",
                 "AIR",
                 "de of the unusual, regular "
               ],
               [
                 1_203_681,
                 53253,
                 769,
                 4,
                 "47.00",
                 "56693.75",
                 "0.05",
                 "0.06",
                 "R",
                 "F",
                 Date.from_iso8601!("1995-01-11"),
                 Date.from_iso8601!("1995-03-03"),
                 Date.from_iso8601!("1995-01-25"),
                 "DELIVER IN PERSON",
                 "AIR",
                 "e. furiously ironic requests wake careful"
               ],
               [
                 1_203_681,
                 14329,
                 1833,
                 5,
                 "4.00",
                 "4973.28",
                 "0.00",
                 "0.08",
                 "A",
                 "F",
                 Date.from_iso8601!("1995-01-25"),
                 Date.from_iso8601!("1995-03-26"),
                 Date.from_iso8601!("1995-02-09"),
                 "NONE",
                 "AIR",
                 "s. slyly final"
               ],
               [
                 1_203_682,
                 11930,
                 6933,
                 1,
                 "13.00",
                 "23945.09",
                 "0.06",
                 "0.04",
                 "R",
                 "F",
                 Date.from_iso8601!("1995-04-15"),
                 Date.from_iso8601!("1995-04-22"),
                 Date.from_iso8601!("1995-05-09"),
                 "DELIVER IN PERSON",
                 "TRUCK",
                 "ts use above the "
               ]
             ],
             success: true
           }
  end

  test "Can perform an async query", %{bypass: bypass} do
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
          "testing/snowflake_async_query_response.json"
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
        schema: "tpch_sf1",
        async: true,
        cache_token: false
      )
      |> Req.post!(
        snowflake_query: "select * from snowflake_sample_data.tpch_sf1.lineitem limit 2"
      )

    assert response.status == 200

    assert response.body == %{
             "code" => "333334",
             "data" => %{
               "getResultUrl" => "/queries/11111111-1111-1111-1111-111111111111/result",
               "progressDesc" => nil,
               "queryAbortsAfterSecs" => 300,
               "queryId" => "11111111-1111-1111-1111-111111111111"
             },
             "message" =>
               "Asynchronous execution in progress. Use provided query id to perform query monitoring and management.",
             "success" => true
           }
  end

  # Snowflake sends back gzipped encoding, so gzip the response here we send back with bypass.
  # Also send back some other headers s3 sends for goodluck.
  defp json_gzip(data, conn, status) do
    conn
    |> Plug.Conn.put_resp_content_type("binary/octet-stream")
    |> Plug.Conn.put_resp_header("content-encoding", "gzip")
    |> Plug.Conn.put_resp_header("x-amz-id-2", "aaa")
    |> Plug.Conn.put_resp_header("x-amz-request-id", "xxx")
    |> Plug.Conn.put_resp_header("x-amz-server-side-encryption-customer-algorithm", "AES256")
    |> Plug.Conn.put_resp_header("x-amz-server-side-encryption-customer-key-md5", "abcd")
    |> Plug.Conn.put_resp_header("accept-ranges", "bytes")
    |> Plug.Conn.send_resp(status, :zlib.gzip(data))
  end

  defp json(data, conn, status) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, data)
  end
end
