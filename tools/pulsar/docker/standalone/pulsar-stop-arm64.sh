#!/bin/bash

echo "Finding and stopping Pulsar container..."
docker ps -a | grep "kezhenxu94/pulsar" | awk '{print $1}' | xargs docker stop
echo "Container stopped - you can run 'pulsar-start-arm64.sh' when/if you want to restart it"
