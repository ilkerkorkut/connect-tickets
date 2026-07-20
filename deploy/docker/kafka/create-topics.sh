#!/usr/bin/env bash
set -euo pipefail

broker="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
kafka_topics=/opt/kafka/bin/kafka-topics.sh

ensure_topic() {
	local topic="$1"
	local expected_partitions="$2"
	local partitions

	if "$kafka_topics" --bootstrap-server "$broker" --describe --topic "$topic" >/tmp/topic-description 2>/dev/null; then
		partitions="$(awk -F'PartitionCount: ' '/PartitionCount:/ { split($2, fields, " "); print fields[1]; exit }' /tmp/topic-description)"
		if [ "$partitions" != "$expected_partitions" ]; then
			echo "topic $topic has $partitions partitions; expected $expected_partitions" >&2
			exit 1
		fi
		return
	fi

	"$kafka_topics" --bootstrap-server "$broker" --create --if-not-exists --topic "$topic" --partitions "$expected_partitions" --replication-factor 1
	"$kafka_topics" --bootstrap-server "$broker" --describe --topic "$topic" >/tmp/topic-description
	partitions="$(awk -F'PartitionCount: ' '/PartitionCount:/ { split($2, fields, " "); print fields[1]; exit }' /tmp/topic-description)"
	if [ "$partitions" != "$expected_partitions" ]; then
		echo "topic $topic initialization did not produce $expected_partitions partitions" >&2
		exit 1
	fi
}

ensure_topic github.repo.refresh.v1 5
ensure_topic github.repo.refresh.dlq.v1 1
