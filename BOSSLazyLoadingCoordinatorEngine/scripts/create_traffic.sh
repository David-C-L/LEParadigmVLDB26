#!/bin/bash

# Check for correct arguments
if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <URL1> <URL2> <NUMBER>"
    exit 1
fi

URL1=$1
URL2=$2
COUNT=$3

# Function to run curl commands in parallel
run_curl() {
    local URL=$1
    local NUM=$2

    echo "Starting $NUM processes for URL: $URL"
    for ((i=0; i<NUM; i++)); do
        curl -s -o /dev/null "$URL" &
    done
}

# Main loop
while true; do
    run_curl "$URL1" "$COUNT"

    # Wait for all background processes to complete
    wait
    echo "All processes have completed. Restarting..."
done
