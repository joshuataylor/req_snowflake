defmodule ReqSnowflake do
  @moduledoc """
  `Req` plugin for [Snowflake](https://www.snowflake.com).

  It uses the Snowflake REST API to communicate with Snowflake, with an earlier version set for JSON.
  There isn't an Elixir Arrow library (yet!), so it seems that setting an earlier Java version seems
  to give us back JSON results. The REST API is used by the Python, Golang and other languages to
  send requests to Snowflake, so it is stable and shouldn't just randomly break.

  It is not the [Snowflake SQL API](https://docs.snowflake.com/en/developer-guide/sql-api/index.html), which is
  limited in its implementation.

  Right now the library doesn't support [MFA](https://docs.snowflake.com/en/user-guide/security-mfa.html), so you'll
  need to either use [private key auth](https://docs.snowflake.com/en/user-guide/odbc-parameters.html#using-key-pair-authentication) or
  connect using a username & password. A private key auth is highly recommended as you can rotate passwords easier.

  This module handles the connection to Snowflake.

  The module also supports async queries, which means that the query will be sent to Snowflake, then polled for updates.
  This means that there won't (hopefully) be long connection times, so deploying should be easier as you won't run the risk
  of killing running queries.

  Query results are decoded into the `ReqSnowflake.Result` struct.

  The struct implements the `Table.Reader` protocol and thus can be efficiently traversed by rows or columns.
  """

  alias Req.Request
  alias ReqSnowflake.Result
  alias ReqSnowflake.Snowflake

  @allowed_options ~w(snowflake_query username password account_name region warehouse role database schema query_tag rows_per_resultset statement_timeout_in_seconds application_name bindings)a

  @doc """
  Attaches to Req request, used for querying Snowflake.

  ## Request Options
  * `:account_name` - Required. Account name. This is usually the name between the https:// and us-east-1 (or whatever region).
                      If unsure, run `select current_account();` in Snowflake.
  * `:region` - Required. Your snowflake region, the region is found between the account name and ".snowflakecomputing.com" on the portal.
                If unsure, run `select current_region();` in Snowflake to show it. Example is `us-east-1`. Region names and their
                IDs can be [found here](https://docs.snowflake.com/en/user-guide/intro-regions.html)
  """
  @spec attach(Request.t(), keyword()) :: Request.t()
  def attach(%Request{} = request, options \\ []) do
    request
    |> Request.prepend_request_steps(snowflake_run: &run/1)
    |> Request.register_options(@allowed_options)
    |> Request.merge_options(options)
  end

  # Bindings happen for inserts
  defp run(
         %Request{
           options:
             %{
               account_name: account_name,
               region: region,
               snowflake_query: query,
               bindings: bindings
             } = options
         } = request
       ) do
    token = ReqSnowflake.SnowflakeLogin.get_snowflake_login_token(options)
    base_url = Snowflake.snowflake_url(account_name, region)

    %{request | url: URI.parse(snowflake_query_url(base_url))}
    |> Request.merge_options(json: snowflake_insert_headers(query, bindings))
    |> Request.put_header("accept", "application/snowflake")
    |> Request.put_header("Authorization", "Snowflake Token=\"#{token}\"")
    |> Request.append_response_steps(snowflake_decode_response: &decode/1)
  end

  defp run(
         %Request{
           options:
             %{account_name: account_name, region: region, snowflake_query: query} = options
         } = request
       ) do
    token = ReqSnowflake.SnowflakeLogin.get_snowflake_login_token(options)
    base_url = Snowflake.snowflake_url(account_name, region)

    %{request | url: URI.parse(snowflake_query_url(base_url))}
    |> Request.merge_options(json: snowflake_query_body(query))
    |> Request.put_header("accept", "application/snowflake")
    |> Request.put_header("Authorization", "Snowflake Token=\"#{token}\"")
    |> Request.append_response_steps(snowflake_decode_response: &decode/1)
  end

  defp run(%Request{} = request), do: request

  defp decode({request, %{status: 200} = response}) do
    {request, update_in(response.body, &decode_body/1)}
  end

  defp decode(any), do: any

  defp decode_body(%{"success" => true} = data) do
    data
    |> Map.get("data")
    |> Map.get("queryResultFormat")
    |> process_query_result_format(data["data"])
  end

  defp process_query_result_format(
         "json",
         %{
           "rowset" => [],
           "rowtype" => row_type,
           "total" => total,
           "chunks" => chunks,
           "chunkHeaders" => %{
             "x-amz-server-side-encryption-customer-key" => key,
             "x-amz-server-side-encryption-customer-key-md5" => md5
           }
         }
       ) do
    urls = Enum.map(chunks, fn %{"url" => url} -> url end)

    parsed =
      Task.async_stream(urls, fn url -> s3_get_json(url, key, md5) end, max_concurrency: 5)
      |> Enum.map(fn {:ok, result} -> result end)
      |> Enum.join(", ")

    rows = Jason.decode!("[#{parsed}]")

    row_data = process_row_data(rows, row_type)

    columns = Enum.map(row_type, fn %{"name" => name} -> name end)

    %Result{
      success: true,
      rows: row_data,
      columns: columns,
      num_rows: total
    }
  end

  defp process_query_result_format(
         "json",
         %{
           "rowset" => rowset,
           "rowtype" => row_type,
           "total" => total,
           "chunks" => chunks,
           "chunkHeaders" => %{
             "x-amz-server-side-encryption-customer-key" => key,
             "x-amz-server-side-encryption-customer-key-md5" => md5
           }
         }
       ) do
    parsed =
      chunks
      |> Enum.map(fn %{"url" => url} -> url end)
      |> Task.async_stream(fn url -> s3_get_json(url, key, md5) end, max_concurrency: 5)
      |> Enum.map(fn {:ok, result} -> result end)
      |> Enum.join(", ")

    rows = Jason.decode!("[#{parsed}]")

    row_data = process_row_data(rowset, row_type) ++ process_row_data(rows, row_type)

    columns = Enum.map(row_type, fn %{"name" => name} -> name end)

    %Result{
      success: true,
      rows: row_data,
      columns: columns,
      num_rows: total
    }
  end

  defp process_query_result_format(
         "json",
         %{"rowset" => rows, "rowtype" => row_type, "total" => total}
       ) do
    row_data = process_row_data(rows, row_type)

    columns = Enum.map(row_type, fn %{"name" => name} -> name end)

    %Result{
      success: true,
      rows: row_data,
      columns: columns,
      num_rows: total
    }
  end

  defp snowflake_query_url(host) do
    uuid = Application.get_env(:req_snowflake, :snowflake_uuid, UUID.uuid4())

    "#{host}/queries/v1/query-request?requestId=#{uuid}"
  end

  defp snowflake_query_body(query) when is_binary(query) do
    %{
      sqlText: query,
      sequenceId: 0,
      bindings: nil,
      bindStage: nil,
      describeOnly: false,
      parameters: %{
        CLIENT_RESULT_CHUNK_SIZE: 48
      },
      describedJobId: nil,
      isInternal: false,
      asyncExec: false
    }
  end

  defp snowflake_insert_headers(query, bindings) when is_binary(query) and is_map(bindings) do
    %{
      sqlText: query,
      sequenceId: 0,
      bindings: bindings,
      bindStage: nil,
      describeOnly: false,
      parameters: %{
        CLIENT_RESULT_CHUNK_SIZE: 48
      },
      describedJobId: nil,
      isInternal: false,
      asyncExec: false
    }
  end

  defp s3_get_json(url, encryption_key, encryption_key_md5) do
    Req.new(url: url)
    |> Request.put_header("accept", "application/snowflake")
    |> Request.put_header("Accept-Encoding", "gzip,deflate")
    |> Request.put_header("x-amz-server-side-encryption-customer-key", encryption_key)
    |> Request.put_header("x-amz-server-side-encryption-customer-key-md5", encryption_key_md5)
    |> Req.get!()
    |> Map.get(:body)
  end

  defp process_row_data(rows, row_type) do
    rows
    |> Stream.map(fn r ->
      r
      |> Stream.with_index()
      |> Stream.map(fn {rr, column_no} ->
        decode_column(Enum.at(row_type, column_no), rr)
      end)
      |> Enum.to_list()
    end)
    |> Enum.to_list()
  end

  # Decodes a column type of null to nil
  def decode_column(%{"scale" => 0, "type" => "fixed", "byteLength" => nil}, nil) do
    nil
  end

  def decode_column(_, nil), do: nil

  def decode_column(%{"type" => "date"}, value) do
    unix_time = String.to_integer(value) * 86400

    case DateTime.from_unix(unix_time) do
      {:ok, time} -> DateTime.to_date(time)
      _ -> {:error, value}
    end
  end

  def decode_column(%{"type" => "timestamp_ntz"}, value) do
    String.replace(value, ".", "")
    |> String.slice(0..-4)
    |> String.to_integer()
    |> DateTime.from_unix!(:microsecond)
  end

  def decode_column(%{"type" => "timestamp_tz"}, value) do
    value
    |> String.split(" ")
    |> hd
    |> String.replace(".", "")
    |> String.slice(0..-4)
    |> String.to_integer()
    |> DateTime.from_unix!(:microsecond)
  end

  def decode_column(%{"type" => "timestamp_ltz"}, value) do
    String.replace(value, ".", "")
    |> String.slice(0..-4)
    |> String.to_integer()
    |> DateTime.from_unix!(:second)
  end

  # Decodes an integer column type
  def decode_column(%{"scale" => 0, "type" => "fixed", "byteLength" => nil}, value) do
    case Integer.parse(value) do
      {num, ""} ->
        num

      _ ->
        value
    end
  end

  # for everything else, just return the value
  def decode_column(_, value), do: value
end
