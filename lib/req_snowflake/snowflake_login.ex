defmodule ReqSnowflake.SnowflakeLogin do
  alias ReqSnowflake.Snowflake

  def get_snowflake_login_token(options) when is_list(options) do
    options
    |> Enum.into(%{})
    |> get_snowflake_login_token()
  end

  def get_snowflake_login_token(options) when is_map(options) do
    Req.post!(snowflake_url(options), json: build_snowflake_login(options))
    |> decode_response()
  end

  defp snowflake_url(options) when is_map(options) do
    base_url = Snowflake.snowflake_url(options[:account_name], options[:region])

    query_params =
      %{
        databaseName: options[:database],
        schemaName: options[:schema],
        roleName: options[:role],
        warehouse: options[:warehouse]
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

  defp build_snowflake_login(options) do
    session_parameters = options[:session_parameters] || %{}

    %{
      data: %{
        ACCOUNT_NAME: options[:account_name],
        PASSWORD: options[:password],
        CLIENT_APP_ID: "PythonConnector",
        CLIENT_APP_VERSION: "2.7.8",
        LOGIN_NAME: options[:username],
        SESSION_PARAMETERS:
          Map.merge(session_parameters, %{
            VALIDATE_DEFAULT_PARAMETERS: true,
            QUOTED_IDENTIFIERS_IGNORE_CASE: true
          }),
        CLIENT_ENVIRONMENT: %{
          tracing: "DEBUG",
          OS: "Linux",
          OCSP_MODE: "FAIL_OPEN",
          APPLICATION: options[:application_name] || "req_snowflake",
          serverURL:
            "https://#{options[:account_name]}.#{options[:region]}.snowflakecomputing.com",
          role: options[:role],
          user: options[:username],
          account: options[:account_name]
        }
      }
    }
    |> add_client_environment(:schema, options[:schema])
    |> add_client_environment(:warehouse, options[:schema])
    |> add_client_environment(:database, options[:schema])
    |> add_arrow_data(options[:arrow] && Code.ensure_loaded?(SnowflakeArrow.Native))
  end

  defp add_client_environment(data, _key, nil), do: data

  defp add_client_environment(data, key, value),
    do: put_in(data, [:data, :CLIENT_ENVIRONMENT, key], value)

  # To ensure we get back Arrow, we set ARROW. This shouldn't be necessary, but it's better to be safe.
  defp add_arrow_data(data, true) do
    put_in(
      data,
      [:data, :SESSION_PARAMETERS, :PYTHON_CONNECTOR_QUERY_RESULT_FORMAT],
      "ARROW"
    )
  end

  # Otherwise to force back JSON, we have to set JavaScript as the CLIENT_APP_ID with version 1.5.3
  defp add_arrow_data(data, _) do
    data
    |> put_in([:data, :CLIENT_APP_ID], "JavaScript")
    |> put_in([:data, :CLIENT_APP_VERSION], "1.5.3")
  end
end
