defmodule ReqSnowflake.ArrowQueryTest do
  use ReqSnowflake.SnowflakeCase, async: false
  import ReqSnowflake.SnowflakeTestHelpers

  test "Can query Snowflake with a valid query and get a response back without chunks and only rowsetBase64",
       %{bypass: bypass} do
    query_bypass(bypass, "testing/arrow/snowflake_query_response_arrow_inline.json")

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

    query_bypass_chunks(
      bypass,
      "testing/arrow/snowflake_query_response_chunks_no_rowsetBase64.json",
      chunks
    )

    s3_bypass(bypass, "s31", "testing/arrow/0.arrow")
    s3_bypass(bypass, "s32", "testing/arrow/1.arrow")
    s3_bypass(bypass, "s33", "testing/arrow/2.arrow")

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
    assert response.body.total_rows == 5000

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
    query_bypass(bypass, "testing/arrow/snowflake_spot_check_response.json")

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

    assert response.body.total_rows == 100
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

  test "Can query Snowflake with a valid query and get a response back without chunks and only rowsetBase64 for table",
       %{bypass: bypass} do
    query_bypass(bypass, "testing/arrow/snowflake_query_response_arrow_inline.json")

    assert Req.new()
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
             cache_token: false,
             table: true
           )
           |> Req.post!(
             snowflake_query: "select * from snowflake_sample_data.tpch_sf1.lineitem limit 2"
           )
           |> Map.get(:body)
           |> Table.to_rows()
           |> Enum.slice(0, 1) == [
             %{
               "SF_BOOLEAN" => false,
               "SF_DATE" => nil,
               "SF_DECIMAL_38_2" => nil,
               "SF_FLOAT" => 16132.77511762,
               "SF_FLOAT_TWO_PRECISION" => 29268.82,
               "SF_INTEGER" => nil,
               "SF_TIMESTAMP" => ~N[2022-08-25 07:38:33.438000],
               "SF_TIMESTAMP_LTZ" => ~N[2023-11-21 06:47:35.438000],
               "SF_TIMESTAMP_NTZ" => ~N[2022-10-28 16:14:06.438000],
               "SF_VARCHAR" => "vRQOhzFFXN6eKG8ZJt2h",
               "SF_ARRAY" => nil,
               "SF_OBJECT" => "{\n  \"arr1_Ybboz\": 13,\n  \"zero\": 0\n}",
               "SF_VARIANT_JSON" => "{\n  \"key_sDzOGefLLlcamCUIYwM8\": true\n}"
             }
           ]
  end

  test "Can query and get data from S3 with table", %{bypass: bypass} do
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

    query_bypass_chunks(
      bypass,
      "testing/arrow/snowflake_query_response_chunks_no_rowsetBase64.json",
      chunks
    )

    s3_bypass(bypass, "s31", "testing/arrow/0.arrow")

    assert Req.new()
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
             cache_token: false,
             table: true
           )
           |> Req.post!(snowflake_query: "select * from snowflake_sample_data.tpch_sf1.lineitem")
           |> Map.get(:body)
           |> Table.to_rows()
           |> Enum.slice(0, 1) == [
             %{
               "SF_BOOLEAN" => false,
               "SF_DATE" => nil,
               "SF_DECIMAL_38_2" => 28584.17,
               "SF_FLOAT" => 10266.7637929,
               "SF_ARRAY" => nil,
               "SF_FLOAT_TWO_PRECISION" => 16421.79,
               "SF_INTEGER" => 9_500_348_786,
               "SF_OBJECT" => "{\n  \"arr1_93JrD\": 13,\n  \"zero\": 0\n}",
               "SF_TIMESTAMP" => ~N[2023-08-05 21:06:52.582000],
               "SF_TIMESTAMP_LTZ" => ~N[2022-09-04 23:48:08.582000],
               "SF_TIMESTAMP_NTZ" => ~N[2022-11-18 15:47:36.582000],
               "SF_VARCHAR" => "5UEDwtdM5PtgdNtPY2jW",
               "SF_VARIANT_JSON" => nil
             }
           ]
  end
end
