#!/bin/bash
# Runs at every boot of the Kafka VM (Container-Optimized OS).
# Rendered by Terraform templatefile(): ${kafka_ip} is replaced with the fixed internal IP.
set -e

# COS has a host firewall that blocks incoming connections except SSH -> open Kafka's port
iptables -w -C INPUT -p tcp --dport 9092 -j ACCEPT 2>/dev/null || iptables -w -A INPUT -p tcp --dport 9092 -j ACCEPT

# Persistent data directory (the stateful partition survives reboots); Kafka runs as uid 1000
DATA=/mnt/stateful_partition/kafka-data
mkdir -p "$DATA"
chown -R 1000:1000 "$DATA"

docker rm -f kafka >/dev/null 2>&1 || true
docker run -d --name kafka --restart unless-stopped --network host \
  --log-driver=gcplogs \
  -v "$DATA":/var/lib/kafka/data \
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://127.0.0.1:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${kafka_ip}:9092 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
  -e KAFKA_LOG_DIRS=/var/lib/kafka/data/logs \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_AUTO_CREATE_TOPICS_ENABLE=true \
  -e KAFKA_NUM_PARTITIONS=3 \
  -e KAFKA_LOG_RETENTION_HOURS=48 \
  -e KAFKA_HEAP_OPTS="-Xmx512m -Xms512m" \
  apache/kafka:3.8.0
