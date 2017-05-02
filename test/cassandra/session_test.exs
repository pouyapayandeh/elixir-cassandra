defmodule Cassandra.SessionTest do
  use Cassandra.SessionCase,
    table: "people",
    create: """
      id uuid,
      name varchar,
      age int,
      PRIMARY KEY (id)
    """

  @moduletag capture_log: true

  test "execute", %{session: session} do
    assert %CQL.Result.Rows{} = Session.execute(session, "SELECT * FROM system_schema.tables")
  end

  test "execute on default keyspace", %{session: session} do
    assert %CQL.Result.Rows{} = Session.execute(session, "SELECT * FROM people")
  end

  test "insert", %{session: session} do
    insert = "INSERT INTO #{@table} (id, name, age) VALUES (now(), :name, :age);"

    characters = [
      %{name: "Bilbo", age: 50},
      %{name: "Frodo", age: 33},
      %{name: "Gandolf", age: 2019},
    ]

    assert characters
      |> Enum.map(&Session.execute(session, insert, values: &1))
      |> Enum.all?(&match?(%CQL.Result.Void{}, &1))

    assert %CQL.Result.Rows{rows_count: 3, columns: ["name", "age"]} =
      rows = Session.execute(session, "SELECT name, age FROM #{@table};")

    for char <- characters do
      assert !is_nil(Enum.find(rows.rows, fn [name, age] -> name == char[:name] and age == char[:age] end))
    end

    assert [%CQL.Result.Rows{rows_count: 2}, %CQL.Result.Rows{rows_count: 1}] =
      Session.run_stream(session, "SELECT name, age FROM #{@table};", &Enum.to_list/1, page_size: 2)
  end
end
