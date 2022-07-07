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

  @allowed_options ~w(snowflake_query arrow cache_token username password account_name region warehouse role database schema application_name bindings session_parameters parallel_downloads async async_poll async_poll_interval download_chunks return_dataframe json_library async_poll_timeout table cache_results return_results)a

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
      return_results: true,
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
    |> set_default_option(:return_results, true)
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
         %{download_chunks: false}
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
         %{
           "success" => true,
           "data" => %{
             "queryResultFormat" => "json",
             "rowset" => [],
             "rowtype" => row_type,
             "total" => total,
             "chunks" => chunks,
             "chunkHeaders" => %{
               "x-amz-server-side-encryption-customer-key" => key,
               "x-amz-server-side-encryption-customer-key-md5" => md5
             }
           }
         } = response,
         options
       ) do
    build_result(
      %{
        success: true,
        columns: map_columns(row_type),
        total_rows: total,
        format: "json"
      },
      response,
      options
    )
  end

  defp decode_body(
         %{
           "success" => true,
           "data" => %{
             "queryResultFormat" => "json",
             "rowtype" => row_type,
             "total" => total,
             "chunks" => chunks,
             "chunkHeaders" => %{
               "x-amz-server-side-encryption-customer-key" => key,
               "x-amz-server-side-encryption-customer-key-md5" => md5
             }
           }
         } = response,
         options
       ) do
    build_result(
      %{
        success: true,
        columns: map_columns(row_type),
        total_rows: total,
        format: "json"
      },
      response,
      options
    )
  end

  defp decode_body(
         %{
           "success" => true,
           "data" => %{
             "queryResultFormat" => "json",
             "rowset" => rows,
             "rowtype" => row_type,
             "total" => total
           }
         },
         _options
       ) do
    build_result(
      %{
        success: true,
        columns: map_columns(row_type),
        total_rows: total,
        format: "json"
      },
      response,
      options
    )

    #    rows = process_json_row_data(rows, row_type) |> Enum.to_list()
    #
    #    %Result{
    #      success: true,
    #      rows: rows,
    #      columns: map_columns(row_type),
    #      total_rows: total,
    #      format: "json"
    #    }
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

    defp decode_body(
           %{
             "success" => true,
             "data" => %{
               "queryResultFormat" => "arrow",
               "rowsetBase64" => "",
               "rowtype" => row_type,
               "total" => total,
               "chunks" => chunks,
               "queryId" => query_id,
               "chunkHeaders" => %{
                 "x-amz-server-side-encryption-customer-key" => key,
                 "x-amz-server-side-encryption-customer-key-md5" => md5
               }
             }
           } = response,
           options
         ) do
      build_result(
        %{
          success: true,
          columns: map_columns(row_type),
          total_rows: total,
          format: "json"
        },
        response,
        options
      )

      %Result{
        success: true,
        rows: [],
        columns: map_columns(row_type),
        total_rows: total,
        format: "arrow",
        query_id: query_id,
        chunks: chunks(chunks, 0),
        chunk_data: %{key: key, md5: md5}
      }
    end

    defp decode_body(
           %{
             "success" => true,
             "data" => %{
               "queryResultFormat" => "arrow",
               "rowsetBase64" => base64,
               "rowtype" => row_type,
               "total" => total,
               "chunks" => chunks,
               "queryId" => query_id,
               "chunkHeaders" => %{
                 "x-amz-server-side-encryption-customer-key" => key,
                 "x-amz-server-side-encryption-customer-key-md5" => md5
               }
             }
           },
           options
         )
         when base64 != "" do
      # decode the base64 here
      base_data =
        SnowflakeArrow.convert_snowflake_arrow_stream(Base.decode64!(base64))
        |> Enum.zip_with(& &1)

      %Result{
        success: true,
        rows: [],
        columns: map_columns(row_type),
        total_rows: total,
        format: "arrow",
        query_id: query_id,
        chunks: chunks(chunks, length(base_data)),
        chunk_data: %{key: key, md5: md5},
        initial_rowset: base_data
      }
    end

    defp decode_body(
           %{
             "success" => true,
             "data" => %{
               "queryResultFormat" => "arrow",
               "rowsetBase64" => base64,
               "rowtype" => row_type,
               "total" => total,
               "queryId" => query_id
             }
           },
           _
         )
         when base64 != "" do
      base_data =
        SnowflakeArrow.convert_snowflake_arrow_stream(Base.decode64!(base64))
        |> Enum.zip_with(& &1)

      %Result{
        success: true,
        rows: [],
        columns: map_columns(row_type),
        total_rows: total,
        format: "arrow",
        query_id: query_id,
        initial_rowset: base_data
      }
    end

    defp decode_body(
           %{
             "success" => true,
             "data" => %{
               "queryResultFormat" => "arrow",
               "rowsetBase64" => "",
               "rowtype" => row_type,
               "total" => total,
               "queryId" => query_id
             }
           },
           _
         ) do
      %Result{
        success: true,
        format: "arrow",
        rows: [],
        columns: map_columns(row_type),
        total_rows: total,
        query_id: query_id
      }
    end

    defp decode_base64_arrow(data) when is_binary(data) do
      data
      |> convert_or_append_arrow(nil)
      |> Kernel.then(&SnowflakeArrow.to_owned(&1))
      |> Kernel.then(&get_columns/1)
    end

    defp arrow_columns(chunks, key, md5, %{return_dataframe: true} = options, base64) do
      with {:ok, ref} <- get_s3_arrow_columns_df(chunks, key, md5, options, base64) do
        ref
      end
    end

    defp arrow_columns(chunks, key, md5, options, base64),
      do: get_s3_arrow_columns(chunks, key, md5, options, base64)

    # Downloads all chunks using stream, then passes them to the arrow2 binding for processing.
    defp get_s3_arrow_columns(chunks, key, md5, options, base64) do
      get_s3_arrow_columns_df(chunks, key, md5, options, base64)
      |> Kernel.then(&get_columns/1)
    end

    defp get_s3_arrow_columns_df(chunks, key, md5, options, nil) do
      # get first item from chunk
      data = get_s3(hd(chunks), key, md5)
      reference = convert_or_append_arrow(data, nil)

      chunks
      |> List.delete_at(0)
      |> Task.async_stream(&get_s3(&1, key, md5),
        timeout: 180_000,
        max_concurrency: options[:parallel_downloads]
      )
      |> Stream.map(&convert_or_append_arrow(&1, reference))
      |> Stream.run()
      |> Kernel.then(fn _x -> SnowflakeArrow.to_owned(reference) end)
    end

    # repeated code :(
    defp get_s3_arrow_columns_df(chunks, key, md5, options, base64) do
      reference = convert_or_append_arrow(base64, nil)

      chunks
      |> Task.async_stream(&get_s3(&1, key, md5),
        timeout: 180_000,
        max_concurrency: options[:parallel_downloads]
      )
      |> Stream.map(&convert_or_append_arrow(&1, reference))
      |> Stream.run()
      |> Kernel.then(fn _x -> SnowflakeArrow.to_owned(reference) end)
    end

    defp get_columns({:ok, reference}) do
      {:ok, columns} = SnowflakeArrow.get_column_names(reference)

      columns
      |> Enum.map(fn column ->
        with {:ok, d} <- SnowflakeArrow.get_column(reference, column) do
          {column, d}
        end
      end)
    end

    defp convert_or_append_arrow(data, reference) when is_nil(reference) and is_binary(data) do
      with {:ok, ref} <- SnowflakeArrow.convert_arrow_to_df(data) do
        ref
      end
    end

    defp convert_or_append_arrow(data, reference) when is_binary(data) do
      with :ok <- SnowflakeArrow.append_snowflake_arrow_to_df(reference, data) do
        reference
      end
    end
  end

  # Downloads all chunks using Task.async_stream, mith max_concurrency set to the maximum parallel downloads, then flat maps the results.
  # Decoding from JSON to Elixir types is done for each chunk instead of at the end, as sometimes a
  # dataset could be multiple gigabytes in size and doing it in chunks seems the more efficient way of doing things.
  # Maybe we should chunk the deserialising parts, but for for now the max size that'll be deserialised is ~50mb
  # uncompressed JSON (max size from Snowflake seems to be a 20mb gzipped file for JSON/Arrow).
  defp get_s3_json_rows(chunks, key, md5, row_type, %{return_results: true} = options) do
    chunks
    |> Task.async_stream(
      &get_s3(&1, key, md5),
      timeout: 180_000,
      max_concurrency: options[:parallel_downloads]
    )
    |> Stream.flat_map(fn {:ok, json} ->
      json_decode!("[" <> json <> "]", options[:json_library] || Jason)
    end)
    |> Stream.map(&map_json_row(&1, row_type))
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
    |> Enum.map(&map_json_row(&1, row_type))
  end

  defp map_json_row(row, row_type) do
    row
    |> Enum.with_index()
    |> Enum.map(fn {rr, column_no} -> decode_json_column(Enum.at(row_type, column_no), rr) end)
  end

  # Decodes a column type of null to nil
  defp decode_json_column(%{"scale" => 0, "type" => "fixed", "byteLength" => nil}, nil), do: nil
  defp decode_json_column(_, nil), do: nil

  defp decode_json_column(%{"type" => "date"}, value) do
    unix_time = String.to_integer(value) * 86400

    case DateTime.from_unix(unix_time) do
      {:ok, time} -> DateTime.to_date(time)
      _ -> {:error, value}
    end
  end

  defp decode_json_column(%{"type" => "timestamp_tz"}, value) do
    # It's a timestamp nano with an offset in a space.
    # Looks like "1588969216076000000 1440"
    [timestamp_string, offset_string] = String.split(value, " ")
    offset = String.to_integer(offset_string)

    timestamp =
      timestamp_string
      |> String.replace(".", "")
      |> String.to_integer()

    # What we now do is take the offset, which should be an integer and minus 1440 from it.
    offset = offset - 1440

    # We now need to lookup all timezones with this offset, which will then be the actual timezone
    # Offset is in minutes. If it is 0, we'll assume UTC.
    timezone = ReqSnowflake.Timezone.convert(offset)

    # I can't find
    ReqSnowflake.Timezone.unix_timestamp(timestamp, timezone)
  end

  defp decode_json_column(%{"type" => timestamp}, value)
       when timestamp in ["timestamp_ntz", "timestamp_ltz"] do
    # The value before the dot is the unix timestamp, the value after is the nanoseconds
    value
    |> String.replace(".", "")
    |> String.to_integer()
    |> DateTime.from_unix!(:nanosecond)
  end

  # Decodes an integer column type
  defp decode_json_column(%{"scale" => 0, "type" => "fixed", "byteLength" => nil}, value) do
    case Integer.parse(value) do
      {num, ""} ->
        num

      _ ->
        value
    end
  end

  # for everything else, just return the value
  defp decode_json_column(_, value), do: value

  defp map_columns(row_type), do: Enum.map(row_type, fn %{"name" => name} -> name end)

  # Get the json part and deserialise it. We use a function to decode here as the user may want to use their choice
  # of library such as Jason, jiffy, Poison etc. Jiffy seems to have the best performance for this, where benchmarks
  # show a 2x speedup over Jason, and much better memory use. But Jiffy is also a C Binding, so some users might be
  # put off by this. It depends how much data you're downloading and decoding and your usecase.
  defp json_decode!(data, library) do
    case library do
      :jiffy -> :jiffy.decode(data, [:use_nil])
      x -> x.decode!(data)
    end
  end

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
  defp chunks(chunks, offset) do
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

  # Process JSON.
  # If the user has asked for rows back (return_results), return the rows here.
  defp build_result(
         %{
           "success" => true,
           "data" => %{
             "queryResultFormat" => "json",
             "rowset" => [],
             "rowtype" => row_type,
             "chunks" => chunks,
             "chunkHeaders" => %{
               "x-amz-server-side-encryption-customer-key" => key,
               "x-amz-server-side-encryption-customer-key-md5" => md5
             }
           }
         } = response,
         %{return_results: true} = options
       ) do
    rows = get_s3_json_rows(chunks, key, md5, row_type, options)

    result_params =
      result_params
      |> Map.put(:rows, rows)

    # get the results from the options
    struct(Result, result_params)
  end

  defp build_result(result_params, response, options) do
    struct(Result, result_params)
  end
end
