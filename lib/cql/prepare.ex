defmodule CQL.Prepare do
  @moduledoc """
  Represents a CQL prepare statement
  """

  import CQL.DataTypes.Encoder

  alias CQL.{Request, Prepare}

  defstruct [query: ""]

  defimpl Request do
    def encode(%Prepare{query: query}) do
      case long_string(query) do
        :error -> :error
        body   -> {:PREPARE, body}
      end
    end

    def encode(_), do: :error
  end
end
