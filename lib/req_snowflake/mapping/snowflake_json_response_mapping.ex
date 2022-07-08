defmodule ReqSnowflake.JSONResponseMapping do
  # Get the json part and deserialise it. We use a function to decode here as the user may want to use their choice
  # of library such as Jason, jiffy, Poison etc. Jiffy seems to have the best performance for this, where benchmarks
  # show a 2x speedup over Jason, and much better memory use. But Jiffy is also a C Binding, so some users might be
  # put off by this. It depends how much data you're downloading and decoding and your usecase.
  def json_decode!(json, library) when is_binary(json) do
    case library do
      :jiffy -> :jiffy.decode("[" <> json <> "]", [:use_nil])
      x -> x.decode!("[" <> json <> "]")
    end
  end

  def json_decode!(json) when is_binary(json) do
    # @todo make this use json.decode below
    Jason.decode!("[" <> json <> "]")
  end

  def map_json_row(row, row_type) do
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
end
