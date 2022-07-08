defmodule ReqSnowflake.FileCache do
  # Converts the list using term_to_binary, then stores it as a file.
  # This is because conversion uses more memory than just reading the file.
  @spec maybe_write(list(), String.t()) :: list
  def maybe_write(data, file_name) do
    if Code.ensure_loaded?(:ezstd) do
      File.write(file_name, :ezstd.compress(:erlang.term_to_binary(data)))
    else
      File.write(file_name, :erlang.term_to_binary(data, compressed: 2))
    end

    data
  end

  # Try and read the data from the file. If it fails, just return nil.
  @spec read(String.t()) :: {:ok, list()} | {:error, nil}
  def read(path) do
    try do
      with {:ok, data} <- File.read(path) do
        {:ok,
         data
         |> maybe_decompress(Code.ensure_loaded?(:ezstd))
         |> :erlang.binary_to_term()}
      else
        _ -> {:error, nil}
      end
    rescue
      x -> {:error, nil}
    end
  end

  defp maybe_decompress(data, true), do: :ezstd.decompress(data)
  defp maybe_decompress(data, false), do: data
end
