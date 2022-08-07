#!/bin/bash

echo "Finding and removing Pulsar container..."
docker ps -a | grep "apachepulsar/pulsar" | awk '{print $1}' | xargs docker rm -f
echo "Removing Pulsar volumes..."
docker volume rm pulsardata pulsarconf
echo "All traces of Pulsar standalone removed"
