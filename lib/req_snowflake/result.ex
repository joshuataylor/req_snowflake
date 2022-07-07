defmodule ReqSnowflake.Result do
  @type t :: %__MODULE__{
          columns: nil | [String.t()],
          rows: [list()],
          dataframe: nil,
          total_rows: nil | integer,
          metadata: [map()],
          messages: [map()],
          success: boolean,
          format: nil | String.t(),
          query_id: nil | String.t(),
          dataframe: nil,
          chunks: [%ReqSnowflake.Chunk{}],
          chunk_data: %{md5: String.t(), key: String.t()} | nil,
          initial_rowset: nil | [list()]
        }

  defstruct columns: nil,
            rows: nil,
            total_rows: nil,
            metadata: nil,
            messages: nil,
            success: false,
            format: nil,
            query_id: nil,
            dataframe: nil,
            chunks: nil,
            initial_rowset: nil,
            chunk_data: nil

  defimpl Table.Reader do
    def init(
          %ReqSnowflake.Result{
            query_id: query_id,
            columns: columns,
            total_rows: total_rows,
            chunks: chunks
          } = result
        ) do
      meta = %{columns: columns, total_rows: total_rows, query_id: query_id}

      {:rows, meta, result}
    end
  end

  defimpl Enumerable do
    alias ReqSnowflake.FileCache

    def count(result), do: {:ok, result.total_rows}

    def member?(_result, _element), do: {:error, __MODULE__}

    # Arrow support
    def reduce(%ReqSnowflake.Result{format: "arrow"} = result, acc, fun) do
      result
      |> stream_chunks()
      |> Enumerable.reduce(acc, fun)
    end

    def slice(result) do
      slicing_fun = fn start, length ->
        # We have sets of data we need to combine, the initial rowset (if it exists)
        # and the chunks for this slice.
        stream1 = maybe_add_initial_rowset(result.initial_rowset, start, length)

        stream2 =
          chunks_for_slice(result.chunks, start, length)
          |> Stream.flat_map(fn %{url: url} ->
            get_chunk(
              result.query_id,
              result.format,
              url,
              result.chunk_data.key,
              result.chunk_data.md5
            )
          end)

        Stream.concat(stream1, stream2)
        |> Stream.drop(start)
        |> Enum.take(length)
      end

      {:ok, result.total_rows, slicing_fun}
    end

    defp maybe_add_initial_rowset(rowset, start, length)
         when length(rowset) > 0 and length(rowset) > start do
      rowset
    end

    defp maybe_add_initial_rowset(_, _, _), do: []

    @spec convert_to_rows(binary(), String.t()) :: list()
    defp convert_to_rows(chunked_data, "arrow") do
      SnowflakeArrow.convert_snowflake_arrow_stream(chunked_data)
      |> Enum.zip_with(& &1)
    end

    # Streaming chunks is the same for both JSON and Arrow.
    defp stream_chunks(%{
           format: format,
           query_id: query_id,
           chunks: chunks,
           chunk_data: %{md5: md5, key: key}
         }) do
      chunks
      # This step has been split so we can do parallel downloading in the future.
      |> Stream.flat_map(fn %{url: url} -> get_chunk(query_id, format, url, key, md5) end)
    end

    @spec chunks_for_slice(list(%ReqSnowflake.Chunk{}), integer, integer) ::
            list(%ReqSnowflake.Chunk{})
    defp chunks_for_slice(chunks, start, length) do
      Enum.filter(chunks, fn chunk -> chunk.row_from <= start end)
    end

    # Caching the chunks is the same for both JSON and Arrow.
    # We cache the output of the conversion, so we don't need to convert it again.
    @spec get_chunk(String.t(), String.t(), String.t(), String.t(), String.t()) :: list()
    defp get_chunk(query_id, format, url, key, md5) do
      data_file_name = url |> String.split("/") |> List.last()
      tmp_file = Path.join(System.tmp_dir!(), "#{query_id}_#{data_file_name}")

      with {:ok, file} <- ReqSnowflake.FileCache.read(tmp_file) do
        file
      else
        _ ->
          ReqSnowflake.get_s3(url, key, md5)
          |> convert_to_rows(format)
          |> ReqSnowflake.FileCache.maybe_write(tmp_file)
      end
    end
  end
end
