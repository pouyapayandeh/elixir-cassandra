defmodule CQL.Prepare do
  @moduledoc """
  Represents a CQL prepare statement
  """

  import CQL.DataTypes.Encoder

  alias CQL.{Request, Prepare}

  defstruct [query: ""]

  defimpl Request do
    def encode(%Prepare{query: query}) do
      with {:ok, body} <- ok(long_string(query)) do
        {:PREPARE, body}
      end
    end

    def encode(_), do: CQL.Error.new("invalid request")
  end
end
