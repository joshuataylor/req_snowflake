defmodule ReqSnowflake.ArrowIntegrationQueryTest do
  use ExUnit.Case, async: false
  @moduletag :integration

  setup do
    :application.unset_env(:req_snowflake, :snowflake_hostname)
    :application.unset_env(:req_snowflake, :snowflake_url)
    :application.unset_env(:req_snowflake, :snowflake_uuid)
  end

  test "Can query Snowflake with a valid query and get a response back without chunks and only rowsetBase64" do
    username = System.get_env("SNOWFLAKE_USERNAME")
    password = System.get_env("SNOWFLAKE_PASSWORD")
    account_name = System.get_env("SNOWFLAKE_ACCOUNT_NAME")
    region = System.get_env("SNOWFLAKE_REGION")
    warehouse = System.get_env("SNOWFLAKE_WAREHOUSE")
    role = System.get_env("SNOWFLAKE_ROLE")
    database = System.get_env("SNOWFLAKE_DATABASE")
    schema = System.get_env("SNOWFLAKE_SCHEMA")

    response =
      Req.new()
      |> ReqSnowflake.attach(
        username: username,
        password: password,
        account_name: account_name,
        region: region,
        warehouse: warehouse,
        role: role,
        database: database,
        schema: schema,
        arrow: true,
        cache_token: false
      )
      |> Req.post!(
        snowflake_query:
          "select row_number, sf_boolean, sf_varchar, sf_integer, sf_float, sf_float_two_precision, sf_decimal_38_2, sf_timestamp_ntz, sf_timestamp_ltz, sf_timestamp, sf_date, sf_variant_json, sf_array, sf_object, sf_hex_binary, sf_base64_binary from foo.bar.test_data order by row_number limit 100;"
      )

    assert response.body.num_rows == 100

    assert response.body.columns == [
             "ROW_NUMBER",
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
  end
end
