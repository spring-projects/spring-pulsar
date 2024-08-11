#!/bin/bash

mkdir connectors
cd connectors
wget https://archive.apache.org/dist/pulsar/pulsar-3.2.4/connectors/pulsar-io-cassandra-3.2.4.nar
wget https://archive.apache.org/dist/pulsar/pulsar-3.2.4/connectors/pulsar-io-rabbitmq-3.2.4.nar
cd ..
