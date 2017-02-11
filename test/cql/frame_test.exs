defmodule CQL.FrameTest do
  use ExUnit.Case, async: true

  test "#body_length" do
    assert {:ok, 10} = CQL.Frame.body_length(<<0::40, 10::integer-32>>)
    assert %CQL.Error{message: "invalid frame header"} = CQL.Frame.body_length(<<0::40, 10::integer-32, "extra bytes">>)
    assert %CQL.Error{message: "invalid frame header"} = CQL.Frame.body_length(<<>>)
  end

  test "#decode_header" do
    assert %CQL.Error{message: "invalid frame header"} = CQL.Frame.decode_header(<<>>)
  end

  test "#decode" do
    assert %CQL.Error{message: "invalid frame"} = CQL.Frame.decode(<<>>)
  end

  test "#set_stream_id" do
    assert %CQL.Error{message: "invalid frame"} = CQL.Frame.set_stream_id(<<>>, 1)
  end
end
