defmodule ArrowUnzip do
  for i <- 1..100 do
    def unquote(:zip)([
          unquote_splicing(Enum.map(1..i, &Macro.var(:"h#{&1}", nil)))
        ]) do
      do_hand_rolled(unquote_splicing(Enum.map(1..i, &Macro.var(:"h#{&1}", nil))), [])
    end

    def unquote(:do_hand_rolled)(
          unquote_splicing(Enum.map(1..i, fn _ -> [] end)),
          acc
        ),
        do: Enum.reverse(acc)

    def unquote(:do_hand_rolled)(
          unquote_splicing(
            Enum.map(1..i, fn x ->
              quote do: [unquote(Macro.var(:"h#{x}", nil)) | unquote(Macro.var(:"t#{x}", nil))]
            end)
          ),
          acc
        ) do
      do_hand_rolled(unquote_splicing(Enum.map(1..i, &Macro.var(:"t#{&1}", nil))), [
        [unquote_splicing(Enum.map(1..i, &Macro.var(:"h#{&1}", nil)))] | acc
      ])
    end
  end
end
