#!/usr/bin/env bash
export RUST_BACKTRACE=full
cargo run --bin datafusion-cli --profile release-nonlto -- -m 4G -c "COPY (SELECT * FROM 'benchmarks/data/hits.parquet' ORDER BY \"EventTime\") TO 'hits_sorted.parquet' STORED AS PARQUET;"
