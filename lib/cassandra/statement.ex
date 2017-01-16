defmodule Cassandra.Statement do
  defstruct [
    :cql,
    :request,
    :options,
    :keyspace,
    :partition_key,
    :has_values?,
    :connections,
    :profile,
  ]

  def new(query, options \\ [], keyspace \\ nil, pk_index \\ nil, prepare \\ true) do
    has_values = has_values?(options)

    request =
      if has_values and prepare do
        %CQL.Prepare{query: query}
      else
        %CQL.Query{query: query, params: CQL.QueryParams.new(options)}
      end

    %__MODULE__{
      cql: nil,
      request: request,
      options: options,
      keyspace: keyspace,
      partition_key: partition_key(options, pk_index),
      has_values?: has_values,
      connections: [],
      profile: %{},
    }
  end

  defp partition_key([], _), do: nil
  defp partition_key(_, nil), do: nil
  defp partition_key(options, pk_index) do
    get_in(options, [:values, Access.at(pk_index)])
  end

  defp has_values?(options) do
    values = Keyword.get(options, :values, [])
    if is_list(values) do
      not Enum.empty?(values)
    else
      raise ArgumentError.exception("invalid values option")
    end
  end
end
