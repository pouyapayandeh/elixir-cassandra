defmodule CQL.DataTypes.Time do
  @moduledoc false

  def decode(<<nanoseconds::integer-64, rest::bytes>>) do
    seconds = nanoseconds |> div(1000_000_000)
    nano = nanoseconds |> rem(1000_000_000) |> div(1000)

    {:ok, time} =
      seconds
      |> :calendar.seconds_to_time
      |> Time.from_erl({nano, 6})

    {time, rest}
  end

  def encode(%Time{microsecond: {microseconds, _}} = time) do
    seconds =
      time
      |> Time.to_erl
      |> :calendar.time_to_seconds

    micro = seconds * 1000_000 + microseconds
    nano = micro * 1000
    <<nano::integer-64>>
  end

  def encode(value) do
    CQL.DataTypes.Encoder.invalid(:time, value)
  end
end
