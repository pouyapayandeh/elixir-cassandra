defmodule CQL.Result.SchemaChange do
  @moduledoc """
  Represents a CQL schema change result
  """

  import CQL.DataTypes.Decoder

  defstruct [
    :change_type,
    :target,
    :options,
  ]

  def decode(buffer) do
    {data, ""} = unpack buffer,
      change_type: :string,
      target:      :string,
      keyspace:    {:string, :target, &(&1 != "KEYSPACE")},
      name:        :string

    options = if data.target != "KEYSPACE" do
      {data.keyspace, data.name}
    else
      data.name
    end

    %__MODULE__{
      change_type: data.change_type,
      target: data.target,
      options: options,
    }
  end
end
