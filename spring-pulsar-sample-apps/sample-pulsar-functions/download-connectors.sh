#!/bin/bash

mkdir connectors
cd connectors
wget https://archive.apache.org/dist/pulsar/pulsar-4.2.0/connectors/pulsar-io-cassandra-4.2.0.nar
wget https://archive.apache.org/dist/pulsar/pulsar-4.2.0/connectors/pulsar-io-rabbitmq-4.2.0.nar
cd ..
