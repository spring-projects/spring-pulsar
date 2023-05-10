#!/bin/bash

mkdir connectors
cd connectors
wget https://archive.apache.org/dist/pulsar/pulsar-3.0.0/connectors/pulsar-io-cassandra-3.0.0.nar
wget https://archive.apache.org/dist/pulsar/pulsar-3.0.0/connectors/pulsar-io-rabbitmq-3.0.0.nar
cd ..
