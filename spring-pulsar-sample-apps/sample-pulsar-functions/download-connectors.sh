#!/bin/bash

mkdir connectors
cd connectors
wget https://archive.apache.org/dist/pulsar/pulsar-4.1.1/connectors/pulsar-io-cassandra-4.1.1.nar
wget https://archive.apache.org/dist/pulsar/pulsar-4.1.1/connectors/pulsar-io-rabbitmq-4.1.1.nar
cd ..
