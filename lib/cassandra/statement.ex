defmodule Cassandra.Statement do
  defstruct [
    :query,
    :options,
    :params,
    :prepared,
    :request,
    :response,
    :keyspace,
    :partition_key,
    :partition_key_picker,
    :values,
    :connections,
  ]

  def new(query, options) do
    %__MODULE__{
      query: query,
      options: Keyword.delete(options, :values),
    }
  end

  def put_values(statement, values) do
    partition_key = partition_key(statement, values)
    %__MODULE__{statement | partition_key: partition_key, values: values}
  end

  def put_prepared(statement, prepared) do
    %__MODULE__{statement | prepared: prepared}
    |> clean
    |> set_pk_picker
  end

  def clean(statement) do
    %__MODULE__{statement | request: nil, response: nil, connections: nil}
  end

  def set_pk_picker(%__MODULE__{partition_key_picker: picker} = statement)
  when is_function(picker)
  do
    statement
  end

  def set_pk_picker(%__MODULE__{prepared: %{metadata: %{global_spec: %{keyspace: keyspace}, pk_indices: [index]}}} = statement) do
    %__MODULE__{statement | partition_key_picker: &Enum.at(&1, index), keyspace: keyspace}
  end

  def set_pk_picker(statement), do: statement

  defp partition_key(%__MODULE__{partition_key_picker: picker}, values)
  when is_function(picker)
  do
    picker.(values)
  end
  defp partition_key(_, _), do: nil

  defimpl DBConnection.Query do
    alias Cassandra.Statement

    def encode(statement, values, options) do
      params =
        (statement.options || [])
        |> Keyword.merge(options)
        |> Keyword.put(:values, values)
        |> CQL.QueryParams.new

      execute = %CQL.Execute{prepared: statement.prepared, params: params}
      with {:ok, request} <- CQL.encode(execute) do
        request
      end
    end

    def decode(_statement, result, _options) do
      with {:ok, %CQL.Frame{body: body}} <- CQL.decode(result) do
        body
      end
    end

    def describe(statement, _options) do
      with {:ok, %CQL.Frame{body: %CQL.Result.Prepared{} = prepared}} <- CQL.decode(statement.response) do
        prepared
      end
    end

    def parse(statement, _options) do
      prepare = %CQL.Prepare{query: statement.query}
      with {:ok, request} <- CQL.encode(prepare) do
        %Statement{statement | request: request}
      end
    end
  end
end
