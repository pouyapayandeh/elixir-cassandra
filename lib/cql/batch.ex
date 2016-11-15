defmodule CQL.Batch do
  @moduledoc """
  Represents a CQL batch statement
  """

  import CQL.DataTypes.Encoder

  require Bitwise

  alias CQL.{Request, Batch}

  defstruct [
    type: :logged,
    queries: [],
    consistency: :one,
    serial_consistency: nil,
    timestamp: nil,
  ]

  defimpl Request do
    @types %{
      logged: 0,
      unlogged: 1,
      counter: 2,
    }

    @flags %{
      :with_serial_consistency => 0x10,
      :with_default_timestamp  => 0x20,
      :with_names              => 0x40,
    }

    def encode(%Batch{} = b) do
      has_timestamp = is_integer(b.timestamp) and b.timestamp > 0

      flags =
        []
        |> prepend(:with_serial_consistency, b.serial_consistency)
        |> prepend(:with_default_timestamp, has_timestamp)
        |> names_to_flag(@flags)

      queries = Enum.map(b.queries, &CQL.BatchQuery.encode(&1))

      if Enum.any?(queries, &match?(:error, &1)) do
        :error
      else
        body =
          []
          |> prepend(byte(@types[b.type]))
          |> prepend(short(Enum.count(b.queries)))
          |> prepend(Enum.join(queries))
          |> prepend(consistency(b.consistency))
          |> prepend(byte(flags))
          |> prepend_not_nil(b.serial_consistency, :consistency)
          |> prepend(b.timestamp, has_timestamp)
          |> Enum.reverse
          |> Enum.join

        {:BATCH, body}
      end
    end

    def encode(_), do: :error
  end
end
