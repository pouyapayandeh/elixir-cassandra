defmodule CQL.DataTypes.Timestamp do
  @moduledoc false

  @epoch :calendar.datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}})

  def decode(<<milliseconds::integer-64, rest::bytes>>) do
    seconds = div(milliseconds, 1000)
    micro = rem(milliseconds, 1000) * 1000

    {:ok, timestamp} =
      seconds + @epoch
      |> :calendar.gregorian_seconds_to_datetime
      |> NaiveDateTime.from_erl({micro, 3})

    {timestamp, rest}
  end

  def encode(%NaiveDateTime{microsecond: {microseconds, 3}} = timestamp) do
    seconds =
      timestamp
      |> NaiveDateTime.to_erl
      |> :calendar.datetime_to_gregorian_seconds

    milliseconds = div(microseconds, 1000)
    n = (seconds - @epoch) * 1000 + milliseconds
    <<n::integer-64>>
  end

  def encode(%NaiveDateTime{}) do
    raise ArgumentError, message: "timestamp requires milliseconds"
  end
end
