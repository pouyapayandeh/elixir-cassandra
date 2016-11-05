defmodule CQL.Result.Void do
  @moduledoc false

  defstruct []

  def decode("") do
    %__MODULE__{}
  end
end

