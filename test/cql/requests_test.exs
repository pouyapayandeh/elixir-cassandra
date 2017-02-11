defmodule CQL.RequestsTest do
  use ExUnit.Case, async: true

  test "startup" do
    assert {:ok, frame} = CQL.encode(%CQL.Startup{})
    assert {:ok, %CQL.Frame{operation: :STARTUP}} = CQL.Frame.decode(frame)
  end

  test "options" do
    assert {:ok, frame} = CQL.encode(%CQL.Options{})
    assert {:ok, %CQL.Frame{operation: :OPTIONS, body: ""}} = CQL.Frame.decode(frame)
  end

  test "register" do
    assert {:ok, frame} = CQL.encode(%CQL.Register{})
    assert {:ok, %CQL.Frame{operation: :REGISTER}} = CQL.Frame.decode(frame)
    assert {:ok, _} = CQL.encode(%CQL.Register{types: ["STATUS_CHANGE"]})
    assert %CQL.Error{code: :invalid, info: info} = CQL.encode(%CQL.Register{types: "TEST"})
    assert info =~ "Expected a 'string_list'"
  end

  test "query" do
    assert {:ok, frame} = CQL.encode(%CQL.Query{query: "TEST"})
    assert {:ok, %CQL.Frame{operation: :QUERY}} = CQL.Frame.decode(frame)
    assert %CQL.Error{code: :invalid, message: "invalid query request"} = CQL.encode(%CQL.Query{query: "test", params: nil})
    assert %CQL.Error{code: :invalid, info: info} = CQL.encode(%CQL.Query{query: []})
    assert info =~ "Expected a 'long_string'"
  end

  test "prepare" do
    assert {:ok, frame} = CQL.encode(%CQL.Prepare{query: "TEST"})
    assert {:ok, %CQL.Frame{operation: :PREPARE}} = CQL.Frame.decode(frame)
  end

  test "execute" do
    assert {:ok, frame} = CQL.encode(%CQL.Execute{prepared: %CQL.Result.Prepared{metadata: %{column_types: []}}, params: %CQL.QueryParams{}})
    assert {:ok, %CQL.Frame{operation: :EXECUTE}} = CQL.Frame.decode(frame)
    assert %CQL.Error{code: :invalid, message: "invalid execute request"} = CQL.encode(%CQL.Execute{})
  end
end
