defmodule ReqSnowflakeLogin do
  @moduledoc """
  `Req` plugin for [Snowflake](https://www.snowflake.com), used for logging into Snowflake.
  Right now only username/password authentication is supported, with [OAuth](https://docs.snowflake.com/en/user-guide/oauth-intro.html),
  [Key Pair](https://docs.snowflake.com/en/user-guide/key-pair-auth.html) and others to come.
  """

  alias Req.Request
  alias ReqSnowflake.Snowflake

  @allowed_options ~w(username password account_name region warehouse role database schema query_tag rows_per_resultset statement_timeout_in_seconds application_name)a

  @doc """
  Attaches to Req request, used for logging into Snowflake.

  ## Request Options
  * `:username` - Required. The username for your account.
  * `:password` - Required. The password for your account.
  * `:account_name` - Required. Account name. This is usually the name between the https:// and us-east-1 (or whatever region).
                      If unsure, run `select current_account();` in Snowflake.
  * `:region` - Required. Your snowflake region, the region is found between the account name and ".snowflakecomputing.com" on the portal.
                If unsure, run `select current_region();` in Snowflake to show it. Example is `us-east-1`. Region names and their
                IDs can be [found here](https://docs.snowflake.com/en/user-guide/intro-regions.html)
  * `:warehouse` - Optional. The warehouse to use on Snowflake. If none set, will use default for the account.
  * `:role` - Optional. The role to use. If none set, will use default for the account. Default role is public otherwise.
  * `:database` - Optional. If set the database to connect to by default.
  * `:schema` - Optional. If set the schema to connect to by default.
  * `:query_tag` - Optional. If set, a query tag is attached to all queries ran in this session. [See QUERY_TAG](https://docs.snowflake.com/en/sql-reference/parameters.html#query-tag)
  * `:rows_per_resultset` - Optional. If set, how many rows to return when issuing a query for the session. Default is unlimited (0). [See ROWS_PER_RESULTSET](https://docs.snowflake.com/en/sql-reference/parameters.html#rows-per-resultset)
  * `:statement_timeout_in_seconds` - Optional. If set, the amount of time, in seconds, after which a running SQL statement (query, DDL, DML, etc.) is canceled by the system.
                                      [See STATEMENT_TIMEOUT_IN_SECONDS](https://docs.snowflake.com/en/sql-reference/parameters.html#rows-per-resultset)
  * `:application_name` - Optional. If set, the application name will show when viewing logs in Snowflake. Defaults to `req_snowflake`.
  * `:cache` - Optional. Defaults to true, if false will not cache the token using persistent storage.
  """
  @spec attach(Request.t(), keyword()) :: Request.t()
  def attach(%Request{} = request, options \\ []) do
    request
    |> Request.register_options(@allowed_options)
    |> Request.merge_options(options)
    |> Req.Request.prepend_request_steps(req_snowflake_login_parse_url: &auth_snowflake/1)
  end

  defp auth_snowflake(
         %{
           url: %URI{host: host},
           options: %{account_name: account_name, region: region} = options
         } = request
       ) do
    # Since we can't get the host before now, we have to check here.
    if Snowflake.snowflake_host(account_name, region) == host do
      keyword_options = Keyword.new(options)
      token = ReqSnowflake.SnowflakeLogin.get_snowflake_login_token(keyword_options)

      return_token(request, token)
    else
      request
    end
  end

  # If we the token exists, we add it to the header response.
  defp return_token(request, token) when is_binary(token) do
    update_in(request.headers, &[{"authorization", "Snowflake Token=\"#{token}\""} | &1])
  end

  # Otherwise just return the error back upstream
  defp return_token(request, %RuntimeError{} = error), do: {request, error}
end
