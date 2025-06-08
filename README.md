# inserter-x 

(Documentation WIP) A simple (and fast) polars dataframe to database insertion utility.

Currently only supports Clickhouse. Implementing other databases will be prioritized if 
there is a need and there is a client that supports fast insertion.

## Clickhouse

The `ClickhouseInserter` provides an interface to table creation and insertion queries.
Insertion is supported via the ArrowStream format. See `tests/mod.rs` for examples.

## Acknowledgements

Name and concept inspired by [connector-x](https://github.com/sfu-db/connector-x).
