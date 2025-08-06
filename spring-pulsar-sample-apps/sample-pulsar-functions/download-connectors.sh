#!/bin/bash

mkdir connectors
cd connectors
wget https://archive.apache.org/dist/pulsar/pulsar-3.3.8/connectors/pulsar-io-cassandra-3.3.8.nar
wget https://archive.apache.org/dist/pulsar/pulsar-3.3.8/connectors/pulsar-io-rabbitmq-3.3.8.nar
cd ..
