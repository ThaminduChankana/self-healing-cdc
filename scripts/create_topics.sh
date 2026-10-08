#!/usr/bin/env bash
# create_topics.sh — idempotently create the pipeline topics.
# Runs either inside the redpanda image (compose `topic-init` job, rpk on PATH)
# or on the host (uses `docker exec cdc-redpanda rpk`).
set -euo pipefail

BROKERS="${RPK_BROKERS:-redpanda:9092}"
if command -v rpk >/dev/null 2>&1; then
    RPK=(rpk)
else
    RPK=(docker exec cdc-redpanda rpk)
fi

MUTATIONS_TOPIC="${MUTATIONS_TOPIC:-cdc.mutations}"
VALIDATED_TOPIC="${VALIDATED_TOPIC:-cdc.validated}"
DLQ_TOPIC="${DLQ_TOPIC:-cdc.dlq}"
REPAIRED_TOPIC="${REPAIRED_TOPIC:-cdc.repaired}"
AUDIT_TOPIC="${AUDIT_TOPIC:-cdc.audit}"

echo "Waiting for Redpanda at ${BROKERS}..."
for i in $(seq 1 60); do
    if "${RPK[@]}" cluster info -X brokers="${BROKERS}" >/dev/null 2>&1; then
        echo "Redpanda is healthy."
        break
    fi
    [[ $i -eq 60 ]] && { echo "ERROR: Redpanda not healthy after 120s" >&2; exit 1; }
    sleep 2
done

# name partitions retention.ms
create_topic() {
    local topic=$1 partitions=$2 retention=$3
    if "${RPK[@]}" topic describe "$topic" -X brokers="${BROKERS}" >/dev/null 2>&1; then
        echo "  ✓ ${topic} (exists)"
    else
        "${RPK[@]}" topic create "$topic" -X brokers="${BROKERS}" -p "$partitions" -r 1 \
            -c retention.ms="$retention" -c cleanup.policy=delete >/dev/null
        echo "  ✓ ${topic} (created: ${partitions} partitions, retention ${retention} ms)"
    fi
}

DAY=86400000
# Single-node local cluster: replication factor 1. Mutations are keyed by
# primary key, so per-row ordering is preserved across partitions.
create_topic "$MUTATIONS_TOPIC" 3 $((7 * DAY))
create_topic "$VALIDATED_TOPIC" 3 $((7 * DAY))
create_topic "$REPAIRED_TOPIC"  3 $((7 * DAY))
create_topic "$DLQ_TOPIC"       1 $((30 * DAY))   # 1 partition: AI repairs are serial
create_topic "$AUDIT_TOPIC"     1 $((90 * DAY))

echo ""
"${RPK[@]}" topic list -X brokers="${BROKERS}"
