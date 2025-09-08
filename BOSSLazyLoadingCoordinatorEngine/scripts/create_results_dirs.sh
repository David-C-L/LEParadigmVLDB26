#!/bin/bash

# Define the base command
COMMAND_1="mkdir WISENT_COMPRESSION_RESULTS"
COMMAND_2="mkdir WISENT_COMPRESSION_RESULTS/QueryQUERY"
COMMAND_3="mkdir WISENT_COMPRESSION_RESULTS/QueryQUERY/SfSCALE"

# Loop through each scale factor, query id, and run
$COMMAND_1
for query in {0,1,2,3,4}; do
  ${COMMAND_2//QUERY/$query}  
  for sf in {3,4}; do
    command_to_run=${COMMAND_3//QUERY/$query}
    command_to_run=${command_to_run//SCALE/$sf}
    $command_to_run
  done
done

echo "Made all dirs."
