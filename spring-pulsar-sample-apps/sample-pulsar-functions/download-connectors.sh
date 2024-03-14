#!/bin/bash

mkdir connectors
cd connectors
wget https://archive.apache.org/dist/pulsar/pulsar-3.2.1/connectors/pulsar-io-cassandra-3.2.1.nar
wget https://archive.apache.org/dist/pulsar/pulsar-3.2.1/connectors/pulsar-io-rabbitmq-3.2.1.nar
cd ..
