#!/bin/bash

mkdir connectors
cd connectors
wget https://archive.apache.org/dist/pulsar/pulsar-3.1.2/connectors/pulsar-io-cassandra-3.1.2.nar
wget https://archive.apache.org/dist/pulsar/pulsar-3.1.2/connectors/pulsar-io-rabbitmq-3.1.2.nar
cd ..
