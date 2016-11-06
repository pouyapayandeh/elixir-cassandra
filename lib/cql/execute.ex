defmodule CQL.Execute do
  @moduledoc """
  Represents a CQL execute statement
  """

  import CQL.DataTypes.Encoder

  alias CQL.{Request, QueryParams}
  alias CQL.Result.Prepared

  defstruct [
    :prepared,
    :params,
  ]

  defimpl Request do
    def encode(%CQL.Execute{prepared: %Prepared{id: id} = prepared, params: %QueryParams{} = params}) do
      with {:ok, zipped} <- ok(zip(prepared.metadata.column_types, params.values)),
           {:ok, encoded_params} <- ok(QueryParams.encode(%{params | values: zipped}))
      do
        {:EXECUTE, short_bytes(id) <> encoded_params}
      end
    end

    def encode(_), do: :error
  end
end
