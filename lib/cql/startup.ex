defmodule CQL.Startup do
  @moduledoc false

  import CQL.DataTypes.Encoder

  alias CQL.{Request, Startup}

  defstruct [options: %{"CQL_VERSION" => "3.0.0"}]

  defimpl Request do
    def encode(%Startup{options: options}) do
      with {:ok, body} <- ok(string_map(options)) do
        {:STARTUP, body}
      end
    end
  end
end
