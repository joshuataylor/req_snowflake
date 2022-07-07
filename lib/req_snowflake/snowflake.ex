defmodule ReqSnowflake.Snowflake do
  @moduledoc false

  # This is set as the base URL is dynamic, so makes it easier for testing using bypass.
  @spec snowflake_host(String.t(), String.t()) :: String.t()
  def snowflake_host(account_name, region) do
    Application.get_env(
      :req_snowflake,
      :snowflake_hostname,
      "#{account_name}.#{region}.snowflakecomputing.com"
    )
  end

  @spec snowflake_url(String.t(), String.t()) :: String.t()
  def snowflake_url(account_name, region) do
    Application.get_env(
      :req_snowflake,
      :snowflake_url,
      "https://#{snowflake_host(account_name, region)}"
    )
  end
end
