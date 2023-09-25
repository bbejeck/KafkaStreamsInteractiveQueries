#!/bin/bash

function readiness_probe {
    nc -z -w 2 0.0.0.0 9092
}

echo "Starting broker"
docker-compose up -d

echo "Waiting for the broker to become available ..."

readiness_probe

while [[ $? != 0 ]]; do
    sleep 5
    readiness_probe
done

echo "Creating topics"
./create-topics-local.sh