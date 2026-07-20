#!/usr/bin/env bash
set -euo pipefail

broker=kafka:9092
kafka_topics=/opt/kafka/bin/kafka-topics.sh

for topic in github.repo.refresh.v1 github.repo.refresh.dlq.v1; do
	"$kafka_topics" --bootstrap-server "$broker" --delete --if-exists --topic "$topic"
done

exec /scripts/create-topics.sh
