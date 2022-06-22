defmodule ReqSnowflake.Snowflake do
  @moduledoc false

  # This is set as the base URL is dynamic, so makes it easier for testing using bypass.
  def snowflake_host(account_name, region) do
    Application.get_env(
      :req_snowflake,
      :snowflake_hostname,
      "#{account_name}.#{region}.snowflakecomputing.com"
    )
  end

  def snowflake_url(account_name, region) do
    Application.get_env(
      :req_snowflake,
      :snowflake_url,
      "https://#{snowflake_host(account_name, region)}"
    )
  end
end
