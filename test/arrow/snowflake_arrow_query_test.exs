defmodule ReqSnowflake.ArrowQueryTest do
  use ExUnit.Case, async: false

  setup do
    bypass = Bypass.open()
    Application.put_env(:req_snowflake, :snowflake_hostname, "127.0.0.1")
    Application.put_env(:req_snowflake, :snowflake_url, "http://127.0.0.1:#{bypass.port}")
    Application.put_env(:req_snowflake, :snowflake_uuid, "0000000-0000-0000-0000-000000000000")

    {:ok, %{bypass: bypass}}
  end

  test "Can query Snowflake with a valid query and get a response back without chunks and only rowsetBase64",
       %{bypass: bypass} do
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
          "testing/arrow/snowflake_query_response_arrow_inline.json"
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
        arrow: true,
        cache_token: false
      )
      |> Req.post!(
        snowflake_query: "select * from snowflake_sample_data.tpch_sf1.lineitem limit 2"
      )

    assert response.body.columns == [
             "SF_BOOLEAN",
             "SF_VARCHAR",
             "SF_INTEGER",
             "SF_FLOAT",
             "SF_FLOAT_TWO_PRECISION",
             "SF_DECIMAL_38_2",
             "SF_TIMESTAMP_NTZ",
             "SF_TIMESTAMP_LTZ",
             "SF_TIMESTAMP",
             "SF_DATE",
             "SF_VARIANT_JSON",
             "SF_ARRAY",
             "SF_OBJECT",
             "SF_HEX_BINARY",
             "SF_BASE64_BINARY"
           ]

    assert length(response.body.rows) == 10

    assert hd(response.body.rows) == [
             false,
             "vRQOhzFFXN6eKG8ZJt2h",
             nil,
             16132.77511762,
             29268.82,
             nil,
             ~N[2022-10-28 16:14:06.438000],
             ~N[2023-11-21 06:47:35.438000],
             ~N[2022-08-25 07:38:33.438000],
             nil,
             "{\n  \"key_sDzOGefLLlcamCUIYwM8\": true\n}",
             nil,
             "{\n  \"arr1_Ybboz\": 13,\n  \"zero\": 0\n}"
           ]
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
        "rowCount" => 630,
        "uncompressedSize" => 129_368,
        "compressedSize" => 41971
      },
      %{
        "url" => "http://127.0.0.1:#{bypass.port}/s32",
        "rowCount" => 1876,
        "uncompressedSize" => 376_080,
        "compressedSize" => 124_150
      },
      %{
        "url" => "http://127.0.0.1:#{bypass.port}/s33",
        "rowCount" => 2494,
        "uncompressedSize" => 505_712,
        "compressedSize" => 172_230
      }
    ]

    Bypass.expect(bypass, "POST", "/queries/v1/query-request", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/arrow/snowflake_query_response_chunks_no_rowsetBase64.json"
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
          "testing/arrow/0.arrow"
        ])
      )
      |> json_gzip(conn, 200)
    end)

    Bypass.expect(bypass, "GET", "/s32", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/arrow/1.arrow"
        ])
      )
      |> json_gzip(conn, 200)
    end)

    Bypass.expect(bypass, "GET", "/s33", fn conn ->
      File.read!(
        Path.join([
          :code.priv_dir(:req_snowflake),
          "testing/arrow/2.arrow"
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
        arrow: true,
        cache_token: false
      )
      |> Req.post!(snowflake_query: "select * from snowflake_sample_data.tpch_sf1.lineitem")

    assert response.status == 200
    assert response.body.num_rows == 5000

    assert response.body.columns == [
             "SF_BOOLEAN",
             "SF_VARCHAR",
             "SF_INTEGER",
             "SF_FLOAT",
             "SF_FLOAT_TWO_PRECISION",
             "SF_DECIMAL_38_2",
             "SF_TIMESTAMP_NTZ",
             "SF_TIMESTAMP_LTZ",
             "SF_TIMESTAMP",
             "SF_DATE",
             "SF_VARIANT_JSON",
             "SF_ARRAY",
             "SF_OBJECT",
             "SF_HEX_BINARY",
             "SF_BASE64_BINARY"
           ]

    assert length(response.body.rows) == 5000
  end

  test "Can query Snowflake and get correct records back in order", %{bypass: bypass} do
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
          "testing/arrow/snowflake_spot_check_response.json"
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
        snowflake_query:
          "select row_number, sf_boolean, sf_varchar, sf_integer, sf_float, sf_float_two_precision, sf_decimal_38_2, sf_timestamp_ntz, sf_timestamp_ltz, sf_timestamp, sf_date, sf_variant_json, sf_array, sf_object, sf_hex_binary, sf_base64_binary from foo.bar.test_data order by row_number limit 100;"
      )

    assert response.body.num_rows == 100
    assert length(response.body.rows) == 100

    assert response.body.rows |> hd == [
             1,
             nil,
             "oOHlCC3Gu9B0c2kh1lsg",
             1_211_510_379,
             nil,
             6167.02,
             nil,
             nil,
             nil,
             nil,
             ~D[2023-11-11],
             "{\n  \"key_MP706epxjSQWY6nBaJLy\": true\n}",
             nil,
             nil
           ]
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
