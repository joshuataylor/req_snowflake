defmodule ReqSnowflake.Timezone do
  def convert(0) do
    %{
      zone_abbr: "UTC",
      time_zone: "Etc/UTC"
    }
  end

  def convert(timestamp_offset) do
    case :persistent_term.get({__MODULE__, timestamp_offset}, nil) do
      nil ->
        value =
          TzExtra.countries_time_zones()
          |> Enum.find(fn %{utc_offset: offset} -> offset == timestamp_offset end)

        :persistent_term.put({__MODULE__, timestamp_offset}, %{
          zone_abbr: value.zone_abbr,
          time_zone: value.time_zone
        })

      i ->
        i
    end
  end

  def unix_timestamp(timestamp, timezone) do
    case Calendar.ISO.from_unix(timestamp, :nanosecond) do
      {:ok, {year, month, day}, {hour, minute, second}, microsecond} ->
        %DateTime{
          year: year,
          month: month,
          day: day,
          hour: hour,
          minute: minute,
          second: second,
          microsecond: microsecond,
          std_offset: 0,
          utc_offset: 0,
          zone_abbr: timezone.zone_abbr,
          time_zone: timezone.time_zone
        }

      {:error, _} = error ->
        error
    end
  end
end
