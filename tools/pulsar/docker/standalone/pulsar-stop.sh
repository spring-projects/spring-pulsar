#!/bin/bash

echo "Finding and stopping Pulsar container..."
docker ps -a | grep "apachepulsar/pulsar" | awk '{print $1}' | xargs docker stop
echo "Container stopped - you can run 'pulsar-start.sh' when/if you want to restart it"
