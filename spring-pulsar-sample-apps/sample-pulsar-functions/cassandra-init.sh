CQL="CREATE KEYSPACE IF NOT EXISTS sample_pulsar_functions_keyspace WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};
USE sample_pulsar_functions_keyspace;
CREATE TABLE customer_onboard (customer_email text PRIMARY KEY, customer_details text);"

until echo $CQL | cqlsh; do
  echo "cqlsh: Cassandra is unavailable to initialize - will retry later"
  sleep 20
done &

exec /usr/local/bin/docker-entrypoint.sh "$@"
