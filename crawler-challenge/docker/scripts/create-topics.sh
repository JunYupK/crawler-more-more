#!/bin/bash
# Kafka Topics Creation Script
# Usage: ./create-topics.sh [kafka-host:port]

KAFKA_BOOTSTRAP=${1:-"localhost:9092"}

echo "Creating Kafka topics on ${KAFKA_BOOTSTRAP}..."

# Function to create topic with retry
create_topic() {
    local topic=$1
    local partitions=$2
    local retention_ms=$3
    local description=$4

    echo "Creating topic: ${topic} (${description})"
    echo "  - Partitions: ${partitions}"
    echo "  - Retention: $((retention_ms / 3600000))h"

    kafka-topics --bootstrap-server ${KAFKA_BOOTSTRAP} \
        --create \
        --if-not-exists \
        --topic ${topic} \
        --partitions ${partitions} \
        --config retention.ms=${retention_ms} \
        --config cleanup.policy=delete \
        --config compression.type=lz4 \
        --config max.message.bytes=10485760

    if [ $? -eq 0 ]; then
        echo "  ✓ Created successfully"
    else
        echo "  ✗ Failed to create"
    fi
    echo ""
}

# Wait for Kafka to be ready
echo "Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server ${KAFKA_BOOTSTRAP} --list > /dev/null 2>&1; do
    echo "  Kafka not ready, waiting..."
    sleep 2
done
echo "Kafka is ready!"
echo ""

# ============================================
# Layer 1: Raw Pages (from Ingestor)
# ============================================
create_topic "raw.page" 12 21600000 "Crawled HTML pages (6h retention)"
create_topic "raw.page.dlq" 3 86400000 "Failed crawl attempts (24h retention)"

# ============================================
# Layer 2: Routed Pages (from Smart Router)
# ============================================
create_topic "process.fast" 8 7200000 "Static pages for BeautifulSoup (2h retention)"
create_topic "process.rich" 4 7200000 "Dynamic pages for Crawl4AI (2h retention)"

# ============================================
# Layer 3: Processed Results
# ============================================
create_topic "processed.final" 12 14400000 "Processed markdown results (4h retention)"
create_topic "processed.dlq" 3 86400000 "Failed processing attempts (24h retention)"

# ============================================
# Layer 4: Storage Events
# ============================================
create_topic "storage.saved" 6 3600000 "Storage confirmation events (1h retention)"
create_topic "storage.dlq" 3 86400000 "Failed storage attempts (24h retention)"

# ============================================
# Monitoring Topics
# ============================================
create_topic "metrics.throughput" 3 3600000 "Throughput metrics (1h retention)"
create_topic "metrics.errors" 3 86400000 "Error events (24h retention)"

echo "============================================"
echo "All topics created. Listing topics:"
echo "============================================"
kafka-topics --bootstrap-server ${KAFKA_BOOTSTRAP} --list

echo ""
echo "Topic details:"
echo "============================================"
kafka-topics --bootstrap-server ${KAFKA_BOOTSTRAP} --describe
