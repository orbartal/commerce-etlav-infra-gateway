# CommerceETLAV InfraGate

## Table of Contents

- [1. Purpose](#1-purpose)
- [2. Documentation](#2-documentation)
- [3. MVP Boundaries](#3-mvp-boundaries)
- [4. Shared Standards](#4-shared-standards)
- [5. Data Preparation](#5-data-preparation)
- [6. Error Response](#6-error-response)
- [7. Tester Integration](#7-tester-integration)
- [8. Implementation](#8-implementation)


## 1. Purpose

InfraGate provides CommerceETLAV Tester with a simple, consistent, synchronous HTTP REST interface for inspecting and cleaning test data in **Kafka** and **MinIO**. These systems contain the same Generator events at different stages:

```text
Generator → Kafka → Ingestor → MinIO
               ↑                  ↑
          InfraGate Kafka    InfraGate MinIO
               └────── Tester ────┘
```

Kafka holds events before ingestion. MinIO holds events persisted by Ingestor. Tester validates event identity, content, and expected delivery behavior across the two stages. InfraGate hides infrastructure-specific clients, offsets, object keys, and preparation indexes.

InfraGate is **one Java/Spring Boot microservice** with **one dedicated PostgreSQL instance**. Its Kafka and MinIO adapters have independent code, tables, and storage operations. They share application infrastructure, REST conventions, and a global preparation coordinator. PostgreSQL holds lookup indexes and preparation state, **not complete event payloads**.

## 2. Documentation

| Document | Contents |
|---|---|
| [Kafka adapter](docs/infra-gate-kafka.md) | Topic management, Kafka preparation, indexing, event lookup, and Kafka-specific failures |
| [MinIO adapter](docs/infra-gate-minio.md) | Bucket/dataset management, MinIO preparation, indexing, event lookup, and MinIO-specific failures |

This README defines the **shared contract**. Each adapter document defines its implementation details. The [Tester README](https://github.com/orbartal/commerce-etlav-tester/blob/main/README.md) is authoritative for the first E2E scenario; adapter documents contain summaries only.

## 3. MVP Boundaries

- One local Kafka broker, one target topic, **one partition**, and **one dataset ID per test/topic**.
- One Tester task at a time; Tester is the only InfraGate client.
- Kafka can retain multiple messages with the same `eventId`. MinIO stores one object per `eventId` and overwrites repeated writes.
- Tester ensures Generator/Ingestor processing has stopped and data is stable before preparation and during validation.
- Kafka retention preserves test records. MinIO bucket versioning is disabled for MVP.
- One global preparation at a time, across **both** adapters; no distributed coordination or multi-instance deployment.
- HTTP requests are synchronous. `POST /prepare` starts an **asynchronous worker** and returns `202 Accepted` without a body. Tester polls status.

## 4. Shared Standards

### 4.1 Adapter base paths

- Kafka: `/infragate/api/v1/kafka/topics/{topic}`
- MinIO: `/infragate/api/v1/minio/buckets/{bucket}/datasets/{datasetId}`

The following suffixes and response contracts are **identical** for both adapters:

| Operation | Method and suffix | Default page size | Preparation required |
|---|---|---:|---|
| Start indexing | `POST /prepare` | — | — |
| Check indexing | `GET /status` | — | — |
| All event IDs | `GET /events/summary/all?index=0&size=100` | 100 | `READY` |
| Matching event details | `GET /events/details/eventid/{eventId}?index=0&size=10` | 10 | `READY` |

Both event endpoints use **zero-based** `index`, a maximum `size` of **1000**, and the same page envelope:

```json
{
  "index": 0,
  "size": 100,
  "totalCount": 3,
  "responseCount": 3,
  "items": ["event-A", "event-B", "event-A"]
}
```

- `totalCount`: exact number of matching indexed records, across all pages.
- `responseCount`: number of items in this page; always equals `items.length`.
- Summary `items`: **event ID strings only**, including duplicate IDs in Kafka.
- Details `items`: complete normalized event objects matching the specified `eventId`, ordered by indexed position.
- A page past the end returns `200`, `responseCount: 0`, and `items: []` while preserving `totalCount`.
- An unknown `eventId` returns `404 EVENT_NOT_FOUND`, **not** an empty successful details response.
- Both summary and details require successful preparation (`READY`).

### 4.2 Event standards

Each details item contains exactly four top-level fields:

```json
{
  "eventId": "event-A",
  "datasetId": "dataset-D",
  "payload": { "example": "original Generator event payload" },
  "metadata": {
    "kafka": {
      "topic": "commerce.events",
      "partition": 0,
      "offset": 1050,
      "key": "event-A",
      "timestamp": "2026-11-07T19:00:00Z",
      "headers": {}
    }
  }
}
```

MinIO substitutes `metadata.minio` (for example, `bucket` and `objectKey`) for `metadata.kafka`. `payload` means the **original Generator event payload** in both adapters, enabling `Kafka.payload == MinIO.payload`. Ingestor-specific storage metadata must not contaminate payload equality. The exact extraction mapping from Ingestor's stored envelope is **deferred** until the cross-repository consistency review; do not invent a field path in implementation.

### 4.3 Duplicate events

Kafka's index stores **one row per message**, with a non-unique `eventId`. Summary pages retain repeated IDs. Details pages return all matching messages **with pagination**; the `totalCount` reflects the number of matching Kafka messages. In MVP, repeated messages with the same `eventId` are assumed to have identical Generator payloads.

MinIO stores `{datasetId}/{eventId}.json`, so repeated writes overwrite the same object. A details query normally returns one item, but still uses the **same pagination contract**. Tester compares distinct IDs and payloads, while validating expected Kafka duplicate counts separately.

## 5. Data Preparation

Each adapter builds its own PostgreSQL lookup index in a background preparation task. Both use `NOT_PREPARED`, `BUILDING`, `READY`, and `FAILED`. Only `READY` permits event reads. Invalid JSON or missing/invalid required event identifiers fail preparation; partial indexes must never be exposed.

A **single shared preparation coordinator** permits at most **one active preparation across Kafka and MinIO**. A conflicting `POST /prepare` returns `409 PREPARATION_ALREADY_RUNNING`. The worker releases its slot only after it has actually stopped, including on failure/timeout. On startup, interrupted `BUILDING` states in **both** adapters are reconciled to `FAILED` before new preparations are accepted.

Tester prepares Kafka and MinIO **sequentially**. The coordinator is shared; adapter-specific preparation logic and SQL tables remain independent.

## 6. Error Response

Common error response:

```json
{
  "code": "EVENT_NOT_FOUND",
  "message": "No event matches the requested eventId.",
  "timestamp": "2026-11-07T23:42:00Z"
}
```

| HTTP | Shared code | Meaning |
|---|---|---|
| 400 | `INVALID_REQUEST` | Invalid identifiers or paging parameters |
| 404 | `EVENT_NOT_FOUND` | No matching event ID |
| 409 | `RESOURCE_NOT_PREPARED` | No usable `READY` preparation |
| 409 | `PREPARATION_ALREADY_RUNNING` | Global preparation slot occupied |
| 409 | `INDEX_OUT_OF_DATE` | Detected change invalidates prepared data |
| 500 | `INTERNAL_ERROR` | Unexpected service error |
| 503 | `SERVICE_UNAVAILABLE` | Kafka/MinIO unavailable |
| 504 | `SERVICE_TIMEOUT` | Infrastructure operation timed out |

Adapter-specific codes (such as `TOPIC_NOT_FOUND` and `BUCKET_NOT_FOUND`) are allowed where meaningful. Detected changes after preparation invalidate the affected index and fail Tester's test; InfraGate must not return partial successful pages or silently rebuild.

## 7. Tester Integration

Below description of the first MVP test steps in Tester:
1. The first MVP test obtains Generator's fixed datasetId.
2. Stops events processing in Generator and Ingestor.
3. Cleans the test Kafka topic and MinIO dataset.
4. Enables Generator and Ingestor again.
5. Triggers Generator to create and publish events.
6. Waits 5 min for events processing to complete.
7. Stops events processing in Generator and Ingestor.
8. Tester prepares each adapter in InfraGate Kafka and MinIO.
9. Tester pages through events IDs and details in Kafka and MinIO.
10. Compares event identity and original payload between Kafka and MinIO.
11. Tester checks expected Kafka duplicates and the MinIO overwrite model
12. Tester reports are generated and test data is preserved for debugging.

**The Tester README owns the authoritative step-by-step scenario.** See the two adapter documents for API-specific integration details.

## 8. Implementation

- InfraGate is a microservice inside docker running with Docker Compose.
- The microservice own its DB and each of its two parts (Kafka and MinIO).
- Java 27, Spring Boot 4, Maven, PostgreSQL, lightweight SQL access and OpenAPI.
- Kafka uses the Apache Kafka Java clients.
- MinIO uses AWS SDK for Java v2 S3 APIs.
- SQL DB schema migrations is reproducible using a script.
