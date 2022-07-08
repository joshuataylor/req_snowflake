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

  The module also supports async queries by passing `:async: true`. This will then return a query id which you can poll for.

  Alternatively you can pass `async_poll: true`, which means that the query will be sent to Snowflake as async, then polled every 5 seconds for updates.

  Query results are decoded into the `ReqSnowflake.Result` struct.

  The struct implements the `Table.Reader` protocol and thus can be efficiently traversed by rows or columns.
  """

  alias Req.Request
  alias ReqSnowflake.Result
  alias ReqSnowflake.Snowflake
  alias ReqSnowflake.JSONResponseMapping

  @allowed_options ~w(snowflake_query arrow cache_token username password account_name region warehouse role database schema application_name bindings session_parameters parallel_downloads async async_poll async_poll_interval download_chunks return_dataframe json_library async_poll_timeout table cache_results)a

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
    default_opts = [
      parallel_downloads: 5,
      cache_token: true,
      async: false,
      download_chunks: true,
      table: false,
      cache_results: false
    ]

    options = Keyword.merge(default_opts, options)

    request
    |> Request.prepend_request_steps(snowflake_run: &snowflake_run/1)
    |> Request.register_options(@allowed_options)
    |> Request.merge_options(options)
  end

  defp default_options(options) do
    options
    |> set_default_option(:parallel_downloads, 5)
    |> set_default_option(:cache_token, true)
    |> set_default_option(:async, false)
    |> set_default_option(:download_chunks, true)
    |> set_default_option(:table, false)
    |> set_default_option(:cache_results, false)
  end

  defp set_default_option(options, key, value) when is_map_key(options, key) == false do
    Map.put(options, key, value)
  end

  defp set_default_option(options, _, _), do: options

  defp snowflake_run(
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
    token = read_memory_cache(options) || request_token(options)

    base_url = Snowflake.snowflake_url(account_name, region)

    %{request | url: URI.parse(snowflake_query_url(base_url))}
    |> Request.merge_options(json: snowflake_insert_headers(query, bindings))
    |> Request.put_header("accept", "application/snowflake")
    |> Request.put_header("Authorization", "Snowflake Token=\"#{token}\"")
    |> Request.append_response_steps(snowflake_decode_response: &decode/1)
  end

  defp snowflake_run(%Request{options: %{snowflake_query: query} = options} = request) do
    token = read_memory_cache(options) || request_token(options)

    base_url = Snowflake.snowflake_url(request.options[:account_name], request.options[:region])

    %{request | url: URI.parse(snowflake_query_url(base_url))}
    |> Request.merge_options(
      json: snowflake_query_body(query, request.options[:async] || request.options[:async_poll])
    )
    |> Request.put_header("accept", "application/snowflake")
    |> Request.put_header("Authorization", "Snowflake Token=\"#{token}\"")
    |> Request.append_response_steps(snowflake_decode_response: &decode/1)
  end

  defp snowflake_run(%Request{} = request), do: request

  defp read_memory_cache(%{cache_token: false}), do: nil

  defp read_memory_cache(options) do
    # We don't need the query here
    options = Map.drop(options, [:snowflake_query])

    case :persistent_term.get({__MODULE__, options}, nil) do
      nil -> nil
      {expiry, token} -> return_token(token, expiry, :os.system_time(:second))
    end
  end

  defp return_token(token, expires, now) when expires > now, do: token
  defp return_token(_token, _expires, _now), do: nil

  defp write_memory_cache(token, %{cache_token: false}), do: token

  defp write_memory_cache(token, options) do
    options = Map.drop(options, [:snowflake_query])
    :persistent_term.put({__MODULE__, options}, {:os.system_time(:second) + 3600, token})
  end

  defp request_token(options) do
    token = ReqSnowflake.SnowflakeLogin.get_snowflake_login_token(options)
    write_memory_cache(token, options)
    token
  end

  defp decode({request, %{status: 200} = response}) do
    {request, update_in(response.body, &decode_body(&1, request.options))}
  end

  defp decode(any), do: any

  defp decode_body(
         %{
           "success" => true,
           "data" => %{
             "rowtype" => row_type,
             "total" => total
           }
         } = d,
         %{download_chunks: false} = options
       ) do
    %Result{
      success: true,
      rows: [],
      columns: map_columns(row_type),
      total_rows: total
    }
  end

  defp decode_body(
         %{
           "success" => true,
           "code" => "333334",
           "data" => %{"queryId" => query_id}
         },
         %{async_poll: true} = options
       ),
       do: poll(options, query_id, 0)

  defp decode_body(
         %{"success" => true, "data" => %{"queryResultFormat" => "json"}} = response,
         options
       ) do
    build_result(
      response,
      options
    )
  end

  if Code.ensure_loaded?(SnowflakeArrow.Native) do
    defp decode_body(
           %{"success" => true, "data" => %{"queryResultFormat" => "arrow"}} = response,
           options
         ) do
      build_result(
        response,
        options
      )
    end
  end

  # Downloads all chunks using Task.async_stream, mith max_concurrency set to the maximum parallel downloads, then flat maps the results.
  # Decoding from JSON to Elixir types is done for each chunk instead of at the end, as sometimes a
  # dataset could be multiple gigabytes in size and doing it in chunks seems the more efficient way of doing things.
  # Maybe we should chunk the deserialising parts, but for for now the max size that'll be deserialised is ~50mb
  # uncompressed JSON (max size from Snowflake seems to be a 20mb gzipped file for JSON/Arrow).
  defp get_s3_json_rows(chunks, key, md5, row_type, %{table: false} = options) do
    chunks
    |> Task.async_stream(
      &get_s3(&1, key, md5),
      timeout: 180_000,
      max_concurrency: options[:parallel_downloads]
    )
    |> Stream.flat_map(fn {:ok, json} ->
      ReqSnowflake.JSONResponseMapping.json_decode!(json, options[:json_library] || Jason)
    end)
    |> Stream.map(&JSONResponseMapping.map_json_row(&1, row_type))
    |> Enum.to_list()
  end

  defp get_s3_json_rows(_, _, _, _, _) do
    []
  end

  defp decode_body(response, _), do: response

  # Because we need to add a unique UUID, we use UUID here. Up for debate if we should use :elixir_uuid , but I'm happy
  # using it here.
  defp snowflake_query_url(host) do
    uuid = Application.get_env(:req_snowflake, :snowflake_uuid, UUID.uuid4())

    "#{host}/queries/v1/query-request?requestId=#{uuid}"
  end

  defp snowflake_query_body({query, []}, async) when is_binary(query) do
    %{
      sqlText: query,
      sequenceId: 0,
      bindings: nil,
      bindStage: nil,
      describeOnly: false,
      parameters: %{},
      describedJobId: nil,
      isInternal: false,
      asyncExec: async == true
    }
  end

  defp snowflake_query_body({query, bindings}, async) when is_binary(query) do
    %{
      sqlText: query,
      sequenceId: 0,
      bindings: bindings,
      bindStage: nil,
      describeOnly: false,
      parameters: %{},
      describedJobId: nil,
      isInternal: false,
      asyncExec: async == true
    }
  end

  @spec snowflake_query_body(String.t(), boolean()) :: map()
  defp snowflake_query_body(query, async) when is_binary(query) do
    %{
      sqlText: query,
      sequenceId: 0,
      bindings: nil,
      bindStage: nil,
      describeOnly: false,
      parameters: %{},
      describedJobId: nil,
      isInternal: false,
      asyncExec: async == true
    }
  end

  @spec snowflake_query_body(String.t(), list) :: map()
  defp snowflake_insert_headers(query, bindings) when is_binary(query) and is_map(bindings) do
    %{
      sqlText: query,
      sequenceId: 0,
      bindings: bindings,
      bindStage: nil,
      describeOnly: false,
      parameters: %{},
      describedJobId: nil,
      isInternal: false,
      asyncExec: false
    }
  end

  # Here we get the result from S3. Later if we find it's more beneficial to read directly from the gzip file in rust for arrow
  # instead of getting it here, we might want to split this into a separate function for JSON/Arrow. For now the performance is
  # fine to do the ungzip here.
  def get_s3(%{"url" => url}, encryption_key, encryption_key_md5),
    do: get_s3(url, encryption_key, encryption_key_md5)

  def get_s3(url, encryption_key, encryption_key_md5) when is_binary(url) do
    Req.new(url: url, cache: true)
    |> Request.put_header("accept", "application/snowflake")
    |> Request.put_header("x-amz-server-side-encryption-customer-key", encryption_key)
    |> Request.put_header("x-amz-server-side-encryption-customer-key-md5", encryption_key_md5)
    |> Req.get!()
    |> Map.get(:body)
  end

  defp process_json_row_data(rows, row_type) do
    rows
    |> Enum.map(&ReqSnowflake.JSONResponseMapping.map_json_row(&1, row_type))
  end

  defp map_columns(row_type), do: Enum.map(row_type, fn %{"name" => name} -> name end)

  defp poll(options, query_id, iteration) do
    case process_query_complete(monitor_query_by_id(options, query_id).body) do
      true ->
        get_query_by_id(options, query_id)

      false ->
        if options.async_poll_timeout == iteration do
          raise "Query #{query_id} timed out"
        end

        poll(options, query_id, iteration + 1)
    end
  end

  defp monitor_query_by_id(options, query_id) do
    host = Snowflake.snowflake_url(options[:account_name], options[:region])
    token = read_memory_cache(options) || ReqSnowflake.request_token(options)

    Req.new(url: "#{host}/monitoring/queries/#{query_id}")
    |> Request.put_header("Authorization", "Snowflake Token=\"#{token}\"")
    |> Req.get!()
  end

  defp get_query_by_id(options, query_id) do
    host = Snowflake.snowflake_url(options[:account_name], options[:region])
    token = read_memory_cache(options) || ReqSnowflake.request_token(options)

    Req.new(url: "#{host}/queries/#{query_id}/result")
    |> Request.register_options(@allowed_options)
    |> Request.merge_options(Keyword.new(options))
    |> Request.put_header("accept", "application/snowflake")
    |> Request.put_header("Authorization", "Snowflake Token=\"#{token}\"")
    |> Request.append_response_steps(snowflake_decode_response_async: &decode_as/1)
    |> Req.get!()
    |> Map.get(:body)
  end

  defp decode_as({request, %{status: 200} = response}) do
    {request, update_in(response.body, &decode_body(&1, request.options))}
  end

  defp process_query_complete(%{"data" => %{"queries" => [%{"status" => "RUNNING"}]}}), do: false
  defp process_query_complete(%{"data" => %{"queries" => [%{"status" => "SUCCESS"}]}}), do: true

  defp process_query_complete(%{"data" => %{"queries" => [%{"status" => "FAILED_WITH_ERROR"}]}}),
    do: true

  defp process_query_complete(%{"data" => %{"queries" => [%{"status" => _}]}}), do: true
  defp process_query_complete(%{"data" => %{"queries" => []}}), do: false
  defp process_query_complete(_), do: true

  # Stupid temporary hack
  defp chunks(
         %{
           "data" => %{
             "chunks" => chunks,
             "chunkHeaders" => %{
               "x-amz-server-side-encryption-customer-key" => key,
               "x-amz-server-side-encryption-customer-key-md5" => md5
             }
           }
         },
         offset
       ) do
    Enum.map_reduce(chunks, %{prev: offset + 1}, fn chunk, acc ->
      row_from = if acc.prev == nil, do: 1, else: acc.prev

      c = %ReqSnowflake.Chunk{
        compressed_size: chunk["compressedSize"],
        row_count: chunk["rowCount"],
        uncompressed_size: chunk["uncompressedSize"],
        url: chunk["url"],
        row_from: row_from,
        row_to: row_from + chunk["rowCount"] - 1
      }

      {c, Map.put(acc, :prev, chunk["rowCount"] + row_from)}
    end)
    |> elem(0)
    |> Enum.sort_by(fn c -> c.row_from end)
  end

  defp chunks(_, _), do: []

  # Process JSON.
  # If the user has asked for rows back (table as false), return the rows here.
  defp build_result(
         %{
           "success" => true,
           "data" => %{
             "queryResultFormat" => "json",
             "rowset" => [],
             "rowtype" => row_type,
             "total" => total,
             "queryId" => query_id,
             "chunks" => chunks,
             "chunkHeaders" => %{
               "x-amz-server-side-encryption-customer-key" => key,
               "x-amz-server-side-encryption-customer-key-md5" => md5
             }
           }
         },
         %{table: false} = options
       ) do
    rows = get_s3_json_rows(chunks, key, md5, row_type, options)

    %Result{
      rows: rows,
      format: "json",
      total_rows: total,
      query_id: query_id,
      success: true,
      columns: map_columns(row_type),
      options: options
    }
  end

  defp build_result(
         %{
           "success" => true,
           "data" => %{
             "queryResultFormat" => "json",
             "rowset" => rows,
             "rowtype" => row_type,
             "total" => total,
             "queryId" => query_id
           }
         } = response,
         %{table: false} = options
       )
       when not is_map_key(response, "chunks") do
    rows = process_json_row_data(rows, row_type)

    %Result{
      rows: rows,
      format: "json",
      total_rows: total,
      query_id: query_id,
      success: true,
      columns: map_columns(row_type)
    }
  end

  defp build_result(
         %{
           "success" => true,
           "data" => %{
             "queryResultFormat" => "json",
             "rowset" => rows,
             "rowtype" => row_type,
             "total" => total,
             "queryId" => query_id
           }
         } = response,
         %{table: true} = options
       )
       when not is_map_key(response, "chunks") do
    rows = process_json_row_data(rows, row_type)

    %Result{
      format: "json",
      total_rows: total,
      query_id: query_id,
      success: true,
      columns: map_columns(row_type),
      initial_rowset: rows,
      chunks: chunks(response, length(rows)),
      chunk_data: %{
        key: response["data"]["chunkHeaders"]["x-amz-server-side-encryption-customer-key"],
        md5: response["data"]["chunkHeaders"]["x-amz-server-side-encryption-customer-key-md5"]
      },
      options: options
    }
  end

  # arrow
  if Code.ensure_loaded?(SnowflakeArrow.Native) do
    defp build_result(
           %{
             "success" => true,
             "data" => %{
               "queryResultFormat" => "arrow",
               "rowsetBase64" => base64,
               "rowtype" => row_type,
               "total" => total,
               "queryId" => query_id
             }
           } = response,
           %{table: false} = options
         ) do
      # @todo This is slow. Use [a|b] then flatten later.
      rows = initial_rowset_rows(response) ++ get_arrow_chunks(response, options)

      %Result{
        format: "arrow",
        total_rows: total,
        query_id: query_id,
        success: true,
        columns: map_columns(row_type),
        rows: rows,
        options: options
      }
    end

    # For table, we can do a catch-all as we can build dynamically
    defp build_result(
           %{
             "success" => true,
             "data" => %{
               "queryResultFormat" => "arrow",
               "rowtype" => row_type,
               "total" => total,
               "queryId" => query_id
             }
           } = response,
           %{table: true} = options
         ) do
      rows = initial_rowset_rows(response)

      %Result{
        format: "arrow",
        total_rows: total,
        query_id: query_id,
        success: true,
        columns: map_columns(row_type),
        initial_rowset: rows,
        chunks: chunks(response, length(rows)),
        chunk_data: %{
          key: response["data"]["chunkHeaders"]["x-amz-server-side-encryption-customer-key"],
          md5: response["data"]["chunkHeaders"]["x-amz-server-side-encryption-customer-key-md5"]
        },
        options: options
      }
    end

    defp initial_rowset_rows(%{"data" => %{"rowsetBase64" => rows}} = response) do
      response
      |> initial_rowset()
      |> Enum.zip_with(& &1)
    end

    defp initial_rowset_rows(_), do: []

    defp initial_rowset(%{"data" => %{"rowsetBase64" => rows}})
         when rows != "" and not is_nil(rows) do
      Base.decode64!(rows)
      |> SnowflakeArrow.convert_snowflake_arrow_stream()
    end

    defp initial_rowset(_), do: []

    # Downloads all chunks using stream, then passes them to the arrow2 binding for processing.
    defp get_arrow_chunks(
           %{
             "data" => %{
               "chunks" => chunks,
               "chunkHeaders" => %{
                 "x-amz-server-side-encryption-customer-key" => key,
                 "x-amz-server-side-encryption-customer-key-md5" => md5
               }
             }
           },
           options
         ) do
      chunks
      |> Task.async_stream(&get_s3(&1, key, md5),
        timeout: 180_000,
        max_concurrency: options[:parallel_downloads]
      )
      |> Stream.flat_map(fn {:ok, chunk} ->
        SnowflakeArrow.convert_snowflake_arrow_stream(chunk)
        |> Enum.zip_with(& &1)
      end)
      |> Enum.to_list()
    end

    defp get_arrow_chunks(_, _), do: []
  end

  defp build_result(response, _options), do: response
end
