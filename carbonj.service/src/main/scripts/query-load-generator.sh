#!/bin/bash

# Help menu function
print_help() {
  echo "Usage: $0 [N] [POD_IDS]"
  echo
  echo "  N         Number of times to run the test (default: 1)"
  echo "  POD_IDS   Comma-separated pod IDs or brace expansion (default: {pod262,pod321,pod324,pod328,pod329,pod228,pod267})"
  echo
  echo "Examples:"
  echo "  $0 5"
  echo "  $0 3 '{pod101,pod102}'"
  echo "  $0 -h"
  echo
  echo "This script runs a set of queries against a local CarbonJ instance and reports result file sizes."
}

# Show help if requested
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  print_help
  exit 0
fi

# Validate N (must be a positive integer if provided)
if [[ -n "$1" && ! "$1" =~ ^[0-9]+$ ]]; then
  echo "Error: N must be a positive integer."
  print_help
  exit 1
fi

# Number of times to run the test (default 1)
N=${1:-1}

# Calculate time range for the last 2 years
until=$(date +%s)
from=$((until - 730*24*60*60))

# Pod IDs: use 2nd argument if provided, else default
pod_ids=${2:-'{pod262,pod321,pod324,pod328,pod329,pod228,pod267}'}

urls=(
  "http://localhost:2001/render/?format=msgpack&local=1&noCache=1&target=${pod_ids}.ecom.*.*.*.*.healthmgr.service.*.*.*.*.*.*"
  "http://localhost:2001/render/?format=msgpack&local=1&noCache=1&target=${pod_ids}.ecom.*.*.*.*.ActiveDataDAO.cacheMap.domain.*"
  "http://localhost:2001/render/?format=msgpack&local=1&noCache=1&target=${pod_ids}.ecom.*.*.*.*.*.audit.*.*.count"
  "http://localhost:2001/render/?format=msgpack&local=1&noCache=1&target=${pod_ids}.ecom.*.*.*.*.sessions.*.*.*.*"
  "http://localhost:2001/render/?format=msgpack&local=1&noCache=1&target=${pod_ids}.ecom.*.*.*.*.healthmgr.{direct,service}.*.*.*.*.*"
)

human_readable() {
  local size=$1
  local units=("bytes" "KB" "MB" "GB" "TB")
  local i=0
  while ((size >= 1024 && i < ${#units[@]}-1)); do
    size=$((size / 1024))
    ((i++))
  done
  echo "${size} ${units[$i]}"
}

overall_start_epoch=$(date +%s)
overall_start_time=$(date)
echo "Overall start time: $overall_start_time"

for run in $(seq 1 $N); do
  echo "=============================="
  echo "Run #$run"
  echo "=============================="
  i=1
  declare -a pids
  start_time=$(date +%s)
  for url in "${urls[@]}"; do
    full_url="${url}&from=${from}&until=${until}"
    echo "Querying: $full_url"
    curl -s "$full_url" -o "result_${run}_${i}.msgpack" &
    pids+=("$!")
    i=$((i+1))
  done

  # Wait for all curl processes to finish
  for pid in "${pids[@]}"; do
    wait $pid
  done

  end_time=$(date +%s)
  duration=$((end_time - start_time))
  echo "All requests for run #$run completed in ${duration} seconds"
  echo "----------------------------------------"

  echo "Result file sizes for run #$run:"
  for f in result_${run}_*.msgpack; do
    if [[ -f "$f" ]]; then
      size=$(stat -c %s "$f")
      hr_size=$(human_readable "$size")
      echo "$f: $hr_size"
    fi
  done

done

overall_end_epoch=$(date +%s)
overall_end_time=$(date)
echo "Overall end time: $overall_end_time"
overall_duration=$((overall_end_epoch - overall_start_epoch))
echo "Total elapsed time for all $N runs: ${overall_duration} seconds"

# Delete all result files
rm -f result_*.msgpack 