# Cassandra

[![Build Status](https://travis-ci.org/cafebazaar/elixir-cassandra.svg?branch=master)](https://travis-ci.org/cafebazaar/elixir-cassandra)
[![Hex.pm](https://img.shields.io/hexpm/v/cassandra.svg?maxAge=2592000)](https://hex.pm/packages/cassandra)
[![Hex.pm](https://img.shields.io/hexpm/l/cassandra.svg?maxAge=2592000)](https://github.com/cafebazaar/elixir-cassandra/blob/master/LICENSE.md)
[![Coverage Status](https://coveralls.io/repos/github/cafebazaar/elixir-cassandra/badge.svg?branch=master)](https://coveralls.io/github/cafebazaar/elixir-cassandra?branch=master)

An Elixir driver for Apache Cassandra.

This driver works with Cassandra Query Language version 3 (CQL3) and Cassandra's native protocol v4.

## Features

* Automatic peer discovery
* Automatic connection managment (reconnect on connection loss and discover new nodes)
* Configurable load-balancing/reconnection policies
* Asynchronous execution through Tasks
* Prepared statements with named and position based values
* Token based load-balancing policy
* Automatic prepare and cache prepared statements per host

## Installation

Add `cassandra` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [{:cassandra, "~> 1.0.0-beta.5"}]
end
```

## Quick Start

```elixir
defmodule Repo do
  use Cassandra
end

{:ok, _} = Repo.start_link
# uses "127.0.0.1:9042" as contact point by default
# discovers other nodes on first connection

Repo.execute """
  CREATE KEYSPACE IF NOT EXISTS test
    WITH replication = {'class':'SimpleStrategy','replication_factor':1};
  """, consistency: :all

Repo.execute """
  CREATE TABLE IF NOT EXISTS test.users (
    id timeuuid,
    name varchar,
    age int,
    PRIMARY KEY (id)
  );
  """, consistency: :all

insert = "INSERT INTO test.users (id, name, age) VALUES (?, ?, ?);"

users = [
  %{name: "Bilbo", age: 50},
  %{name: "Frodo", age: 33},
  %{name: "Gandolf", age: 2019},
]

users
|> Task.async_stream(&Repo.execute(insert, values: [Cassandra.UUID.v1, &1.name, &1.age]))
|> Enum.to_list

Repo.execute("SELECT * FROM test.users;")

# %CQL.Result.Rows{
#   columns: ["id", "age", "name"],
#   rows_count: 3,
#   rows: [
#     ["831e5df2-a0e1-11e6-b9af-6d2c86545d91", 2019, "Gandolf"],
#     ["831e5df1-a0e1-11e6-b9af-6d2c86545d91", 33, "Frodo"],
#     ["831e5df0-a0e1-11e6-b9af-6d2c86545d91", 50, "Bilbo"]
#   ]
# }
```

## Todo

* [ ] Compression
* [ ] Batch statement
* [ ] Authentication and SSL encryption
* [ ] User Defined Types
* [ ] Use prepared `result_metadata` optimization

