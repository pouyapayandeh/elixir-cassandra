defmodule CQL.Result.SetKeyspace do
  @moduledoc """
  Represents a CQL set keyspace result
  """

  import CQL.DataTypes.Decoder

  defstruct [:name]

  def decode(binary) do
    {keypace, ""} = string(binary)
    %__MODULE__{name: keypace}
  end
end

