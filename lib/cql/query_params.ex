defmodule CQL.QueryParams do
  @moduledoc """
  Represents a CQL query/execute statements parameters
  """
  import CQL.DataTypes.Encoder

  require Bitwise

  defstruct [
    consistency: :one,
    values: nil,
    skip_metadata: false,
    page_size: nil,
    paging_state: nil,
    serial_consistency: nil,
    timestamp: nil,
  ]

  @valid_keys [
    :consistency,
    :values,
    :skip_metadata,
    :page_size,
    :paging_state,
    :serial_consistency,
    :timestamp,
  ]

  @flags %{
    :values                  => 0x01,
    :skip_metadata           => 0x02,
    :page_size               => 0x04,
    :with_paging_state       => 0x08,
    :with_serial_consistency => 0x10,
    :with_default_timestamp  => 0x20,
    :with_names              => 0x40,
  }

  def new(options) when is_list(options) do
    if Keyword.keyword?(options) do
      struct(__MODULE__, Keyword.take(options, @valid_keys))
    else
      struct(__MODULE__)
    end
  end

  def new(options) when is_map(options) do
    struct(__MODULE__, Map.take(options, @valid_keys))
  end

  def new(_) do
    struct(__MODULE__)
  end

  def encode(q = %__MODULE__{values: values}) when is_nil(values) do
    encode(q, false, false, nil)
  end

  def encode(q = %__MODULE__{values: values}) when is_list(values) or is_map(values) do
    if Enum.empty?(values) do
      encode(q, false, false, nil)
    else
      case values(values) do
        :error -> :error
        encoded -> encode(q, true, is_map(values), encoded)
      end
    end
  end

  def encode(_), do: :error

  defp encode(q, has_values, has_names, values) do
    has_timestamp = is_integer(q.timestamp) and q.timestamp > 0

    flags =
      []
      |> prepend(:values, has_values)
      |> prepend(:skip_metadata, q.skip_metadata)
      |> prepend(:page_size, q.page_size)
      |> prepend(:with_paging_state, q.paging_state)
      |> prepend(:with_serial_consistency, q.serial_consistency)
      |> prepend(:with_default_timestamp, has_timestamp)
      |> prepend(:with_names, has_names)
      |> names_to_flag(@flags)
      |> byte

    q.consistency
    |> consistency
    |> List.wrap
    |> prepend(flags)
    |> prepend(values, has_values)
    |> prepend_not_nil(q.page_size, :int)
    |> prepend_not_nil(q.paging_state, :bytes)
    |> prepend_not_nil(q.serial_consistency, :consistency)
    |> prepend(q.timestamp, has_timestamp)
    |> Enum.reverse
    |> Enum.join
  end
end
