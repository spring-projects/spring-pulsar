#!/bin/bash

mkdir connectors
cd connectors
wget https://archive.apache.org/dist/pulsar/pulsar-2.10.3/connectors/pulsar-io-cassandra-2.10.3.nar
wget https://archive.apache.org/dist/pulsar/pulsar-2.10.3/connectors/pulsar-io-rabbitmq-2.10.3.nar
cd ..
