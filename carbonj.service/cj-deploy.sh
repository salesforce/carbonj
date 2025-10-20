#!/bin/bash

# Shards 18-31 are part of the v2-B sidecar migration.
# We are deploying to them with a 11 min delay.
for i in {18..31}
do
  echo "Upgrading carbonj-p${i}..."
  helm upgrade -f ./values-prd-v2-shard${i}.yaml carbonj-p${i} ./ -n carbonj
  echo "Upgrade of carbonj-p${i} complete."
  if [ $i -lt 31 ]; then
    echo "Waiting for 11 minutes before next upgrade..."
    
    POD1="carbonj-p${i}-0"
    POD2="carbonj-p${i}-1"

    # Wait for approximately 11 minutes (22 * 30 seconds = 660 seconds), checking status periodically.
    for j in {1..22}; do
        echo "--- Status check ${j}/22 for shard p${i} ---"
        kubectl get pod ${POD1} -n carbonj
        kubectl get pod ${POD2} -n carbonj
        echo "------------------------------------------------"
        # If not the last check, sleep.
        if [ $j -lt 22 ]; then
            sleep 30
        fi
    done
    echo "Finished 11 minute wait for shard p${i}."
  fi
done

echo "All upgrades complete."