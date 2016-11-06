defmodule CQL.BatchQuery do
  @moduledoc """
  Represents a CQL batch statement subquery
  """

  import CQL.DataTypes.Encoder

  defstruct [:query, :values]

  @kind %{
    query: 0,
    prepared: 1,
  }

  @doc false
  def encode(%__MODULE__{query: %CQL.Result.Prepared{id: id} = prepared, values: values})
  when is_list(values)
  do
    with {:ok, zipped} <- ok(zip(prepared.metadata.column_types, values)),
         {:ok, encoded_values} <- ok(values(zipped))
    do
      Enum.join([
        byte(@kind[:prepared]),
        short_bytes(id),
        encoded_values,
      ])
    end
  end

  def encode(%__MODULE__{query: query, values: nil}) when is_binary(query) do
    Enum.join([
      byte(@kind[:query]),
      long_string(query),
      short(0),
    ])
  end

  def encode(_), do: :error
end
