docker exec -it broker /usr/bin/kafka-topics \
 --create \
 --topic input\
 --bootstrap-server broker:9092 \
 --partitions 2


docker exec -it broker /usr/bin/kafka-topics \
 --create \
 --topic output \
 --bootstrap-server broker:9092 \
 --partitions 2 