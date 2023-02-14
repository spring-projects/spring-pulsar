#!/bin/bash

mkdir connectors
cd connectors
wget https://archive.apache.org/dist/pulsar/pulsar-2.11.0/connectors/pulsar-io-cassandra-2.11.0.nar
wget https://archive.apache.org/dist/pulsar/pulsar-2.11.0/connectors/pulsar-io-rabbitmq-2.11.0.nar
cd ..
