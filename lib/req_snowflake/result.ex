defmodule ReqSnowflake.Result do
  @type t :: %__MODULE__{
          columns: nil | [String.t()],
          rows: [tuple],
          num_rows: integer,
          metadata: [map()],
          messages: [map()],
          success: boolean
        }

  defstruct columns: nil,
            rows: nil,
            num_rows: 0,
            metadata: [],
            messages: [],
            success: false
end

if Code.ensure_loaded?(Table.Reader) do
  defimpl Table.Reader, for: SnowflakeEx.Result do
    def init(%{columns: columns}) when columns in [nil, []] do
      {:rows, %{columns: []}, []}
    end

    def init(result) do
      {:rows, %{columns: result.columns}, result.rows}
    end
  end
end
