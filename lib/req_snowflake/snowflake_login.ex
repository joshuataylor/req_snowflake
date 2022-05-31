defmodule ReqSnowflake.SnowflakeLogin do
  alias ReqSnowflake.Snowflake

  def get_snowflake_login_token(options) when is_map(options) do
    Keyword.new(options)
    |> get_snowflake_login_token()
  end

  def get_snowflake_login_token(options \\ []) when is_list(options) do
    data = build_snowflake_login(options)
    url = snowflake_url(options)

    Req.post!(url, json: data)
    |> decode_response()
  end

  defp snowflake_url(options) when is_list(options) do
    account_name = options[:account_name]
    region = options[:region]

    base_url = Snowflake.snowflake_url(account_name, region)

    query_params =
      %{
        databaseName: Keyword.get(options, :database, nil),
        schemaName: Keyword.get(options, :schema, nil),
        roleName: Keyword.get(options, :role, nil),
        warehouse: Keyword.get(options, :warehouse, nil)
      }
      |> Enum.filter(fn {_, v} -> v != nil end)
      |> URI.encode_query()

    "#{base_url}/session/v1/login-request"
    |> URI.parse()
    |> Map.put(:query, query_params)
    |> URI.to_string()
  end

  defp decode_response(%Req.Response{status: 200, body: %{"data" => %{"token" => token}}}),
    do: token

  defp decode_response(%Req.Response{body: %{"message" => message}}),
    do: RuntimeError.exception(message)

  defp decode_response(%Req.Response{body: body}), do: RuntimeError.exception(body)

  # Something unexpected happened, such as the server being down, timeout, etc.
  # Probably wise to later bubble this back up to the user?
  defp decode_response(error), do: RuntimeError.exception(error)

  @spec build_snowflake_login(
          database: String.t(),
          account_name: String.t(),
          username: String.t(),
          password: String.t(),
          region: String.t(),
          application_name: String.t()
        ) :: {:ok, %{token: String.t(), session_id: String.t()}} | {:error, String.t()}
  # Builds the snowflake login map that needs to sent in JSON.
  # This will soon need to support many options, so it's moved to its own function.
  # Full list of parameters needed to be supported: https://docs.snowflake.com/en/sql-reference/parameters.html
  defp build_snowflake_login(options) do
    %{
      data: %{
        ACCOUNT_NAME: options[:account_name],
        PASSWORD: options[:password],
        # This way we get JSON results
        CLIENT_APP_ID: "JavaScript",
        # Version supporting JSON results
        CLIENT_APP_VERSION: "1.5.3",
        LOGIN_NAME: options[:username],
        SESSION_PARAMETERS: %{
          VALIDATE_DEFAULT_PARAMETERS: true,
          QUOTED_IDENTIFIERS_IGNORE_CASE: true
        },
        CLIENT_ENVIRONMENT: %{
          tracing: "DEBUG",
          OS: "Linux",
          OCSP_MODE: "FAIL_OPEN",
          APPLICATION: Keyword.get(options, :application_name, "req_snowflake"),
          serverURL:
            "https://#{options[:account_name]}.#{options[:region]}.snowflakecomputing.com",
          role: options[:role],
          user: options[:username],
          account: options[:account_name]
        }
      }
    }
    |> add_client_environment(:schema, Keyword.get(options, :schema))
    |> add_client_environment(:warehouse, Keyword.get(options, :warehouse))
    |> add_client_environment(:database, Keyword.get(options, :database))
  end

  defp add_client_environment(data, _key, nil), do: data

  defp add_client_environment(data, key, value) do
    put_in(data, [:data, :CLIENT_ENVIRONMENT, key], value)
  end
end
