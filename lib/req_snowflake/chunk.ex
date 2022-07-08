defmodule ReqSnowflake.Chunk do
  defstruct compressed_size: nil,
            row_count: nil,
            uncompressed_size: nil,
            url: nil,
            row_from: nil,
            row_to: nil
end
