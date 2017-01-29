defmodule CQL.Register do
  @moduledoc """
  Represents a CQL Register statement
  """

  import CQL.DataTypes.Encoder
  alias CQL.Request

  @types [
    "TOPOLOGY_CHANGE",
    "STATUS_CHANGE",
    "SCHEMA_CHANGE",
  ]

  defstruct [types: @types]

  defimpl Request do
    def encode(%CQL.Register{types: types}) do
      with {:ok, body} <- ok(string_list(types)) do
        {:REGISTER, body}
      end
    end

    def encode(_), do: CQL.Error.new("invalid request")
  end
end
