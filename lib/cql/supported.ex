defmodule CQL.Supported do
  @moduledoc """
  Represents a CQL supported response
  """

  import CQL.DataTypes.Decoder

  defstruct [options: %{}]

  def decode(body) do
    {options, ""} = string_multimap(body)

    %__MODULE__{options: options}
  end
end
