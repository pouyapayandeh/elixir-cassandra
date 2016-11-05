defmodule CQL.Options do
  @moduledoc """
  Represents a CQL options request statement
  """

  defstruct []

  defimpl CQL.Request do
    def encode(%CQL.Options{}) do
      {:OPTIONS, ""}
    end

    def encode(_), do: :error
  end
end
