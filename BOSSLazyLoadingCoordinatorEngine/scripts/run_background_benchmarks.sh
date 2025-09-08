#!/bin/bash

COMMAND="./Benchmarks --benchmark_out=./PARQUET_LIGHT_COMPRESSION_RESULTS/tpch_results_parquet_Q0_to_4_sf_3_to_5_run_RUN.json --benchmark_out_format=json"

for run in {1..6}; do
    command_to_run=${COMMAND//RUN/$run}
    $command_to_run &
done

# Wait for all background processes to complete
wait

echo "All benchmarks have completed."
