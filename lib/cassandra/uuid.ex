defmodule Cassandra.UUID do
  @moduledoc false

  defstruct [
    type: :uuid,
    value: nil,
  ]
end
