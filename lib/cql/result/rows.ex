defmodule CQL.Result.Rows do
  @moduledoc """
  Represents a CQL rows result
  """

  import CQL.DataTypes.Decoder, except: [decode: 2]

  defstruct [
    :columns,
    :columns_count,
    :column_types,
    :rows,
    :rows_count,
    :paging_state,
  ]

  @doc false
  def decode_meta(buffer) do
    with {:ok, %CQL.Frame{body: body, operation: :RESULT}} <- CQL.Frame.decode(buffer),
         {0x02, rest} <- int(body)
    do
      decode(rest, false)
    else
      _ -> CQL.Error.new("trying to decode meta from frame which is not a RESULT.ROWS")
    end
  end

  def decode_rows(%__MODULE__{} = r) do
    {rows, ""} = ntimes(r.rows_count, row_content(r.column_types, r.columns_count), r.rows)
    %{r | rows: rows}
  end

  @doc false
  def decode(buffer, decode_rows \\ true) do
    {meta, buffer} = unpack buffer,
      metadata:   &CQL.MetaData.decode/1,
      rows_count: :int

    columns_count = meta.metadata.columns_count
    {columns, column_types} = Enum.unzip(meta.metadata.column_types)

    rows =
      if decode_rows do
        {rows, ""} = ntimes(meta.rows_count, row_content(column_types, columns_count), buffer)
        rows
      else
        buffer
      end

    %__MODULE__{
      columns: columns,
      columns_count: columns_count,
      column_types: column_types,
      rows: rows,
      rows_count: meta.rows_count,
      paging_state: Map.get(meta.metadata, :paging_state),
    }
  end

  @doc """
  Joins a list of Rows, as they where result of a single query
  """
  def join(rows_list) do
    rows_list
    |> Enum.reduce(fn row, %{rows_count: n, rows: list} = acc ->
         %{acc | rows_count: n + row.rows_count, rows: list ++ row.rows}
       end)
    |> Map.put(:paging_state, nil)
  end

  @doc """
  Converts a Rows struct to a list of keyword lists with column names as keys
  """
  def to_keyword(%__MODULE__{columns: columns, rows: rows}) do
    Enum.map(rows, &zip_to(columns, &1, []))
  end

  @doc """
  Converts a Rows struct to a list of maps with column names as keys
  """
  def to_map(%__MODULE__{columns: columns, rows: rows}) do
    Enum.map(rows, &zip_to(columns, &1, %{}))
  end

  defp zip_to(keys, values, into) do
    keys
    |> Enum.zip(values)
    |> Enum.into(into)
  end

  defp row_content(types, count) do
    fn binary ->
      {row, rest} = ntimes(count, &bytes/1, binary)
      {parse(row, types), rest}
    end
  end

  defp parse(row_content, types) do
    types
    |> Enum.zip(row_content)
    |> Enum.map(&CQL.DataTypes.decode/1)
  end
end
