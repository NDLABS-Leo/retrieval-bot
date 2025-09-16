
# Retrieval Stats API

A lightweight HTTP service that aggregates retrieval success rates from MongoDB and exposes them via Redis-backed APIs.
This README documents environment variables, data flow, Redis data layout, and **all HTTP endpoints** served by this code.

> **Base URL (provided):** `http://203.160.84.158:58787`

---

## Contents

- [Overview](#overview)
- [Architecture & Data Flow](#architecture--data-flow)
- [Environment Variables](#environment-variables)
- [Build & Run](#build--run)
- [MongoDB Collection Expectations](#mongodb-collection-expectations)
- [Redis Keys & TTL](#redis-keys--ttl)
- [Cron Aggregations](#cron-aggregations)
- [HTTP API](#http-api)
  - [/miners](#get-miners)
  - [/clients](#get-clients)
  - [/details](#get-details)
- [HTTP Status Codes & Errors](#http-status-codes--errors)
- [Examples](#examples)
- [Operational Notes](#operational-notes)
- [License](#license)

---

## Overview

This service reads retrieval task results from MongoDB (collection `claims_task_result`), computes success rates, writes
aggregates into Redis, and serves paginated JSON endpoints for:
- Miner-level success rates (HTTP)
- Per-client view of miners interacted with
- Detailed task rows (HTTP module only)

The server also maintains a sorted set index in Redis for efficient miner ranking by HTTP success rate.

**Default listen address:** `:8787` (overridden by `BIND_ADDR`).

---

## Architecture & Data Flow

```
MongoDB (claims_task_result)
   │
   ├─ Aggregation (every 24h, plus immediate run on boot)
   │    ├─ Miner-level rates  → Redis String: stats:miner:<miner_id>
   │    │                      Redis ZSet   : idx:miners:http (score = success_rate_http)
   │    └─ Client×Miner list  → Redis String: stats:client:<client_addr> (JSON array)
   │
HTTP Server
   ├─ GET /miners    → ranked miners or single miner doc (from Redis)
   ├─ GET /clients   → client’s miner list (from Redis)
   └─ GET /details   → raw rows from MongoDB (module=http only, paginated)
```

---

## Environment Variables

| Name         | Default                         | Description |
|--------------|----------------------------------|-------------|
| `MONGO_URI`  | `mongodb://127.0.0.1:27017`      | MongoDB connection URI. |
| `MONGO_DB`   | `fil`                            | MongoDB database name. |
| `REDIS_ADDR` | `127.0.0.1:6379`                 | Redis address. |
| `REDIS_DB`   | `0`                              | Redis logical DB index. |
| `BIND_ADDR`  | `:8787`                          | HTTP listen address (e.g., `:58787`). |

> **Production base URL in your deployment**: `http://203.160.84.158:58787`

---

## Build & Run

```bash
# 1) Build
go build -o retrieval-stats-api ./

# 2) Configure env
export MONGO_URI="mongodb://127.0.0.1:27017"
export MONGO_DB="fil"
export REDIS_ADDR="127.0.0.1:6379"
export REDIS_DB="0"
export BIND_ADDR=":58787"  # your provided port

# 3) Run
./retrieval-stats-api
```

You should see logs like:
```
init ok. mongo=mongodb://127.0.0.1:27017 db=fil redis=127.0.0.1:6379 bind=:58787
[cron] client+miner agg ok
[cron] miner agg ok
listening on :58787
```

---

## MongoDB Collection Expectations

**Collection:** `claims_task_result`

The code reads the following fields (nested in documents):
- `task.module` — currently filtered to `"http"` only
- `task.metadata.client` — client address (string)
- `task.provider.id` — miner address (string)
- `task.content.cid` — content CID (string, used in `/details` output)
- `result.success` — boolean indicating success
- `result.error_code` — string return code (in `/details` output)
- `result.error_message` — string message (in `/details` output)
- `created_at` — timestamp for sorting/pagination in `/details`

> **Important:** Documents missing these fields may be ignored or lead to default values in outputs.

---

## Redis Keys & TTL

- **Miner doc:** `stats:miner:<miner_id>` → JSON object:
  ```json
  {
    "success_rate_http": 0.97,
    "success_rate_graphsync": 0.0,
    "success_rate_bitswap": 0.0
  }
  ```
- **Client list:** `stats:client:<client_addr>` → JSON array of items:
  ```json
  [
    {
      "client_addr": "f1...",
      "miner_addr": "f0...",
      "success_rate_http": 0.92,
      "success_rate_graphsync": 0.0,
      "success_rate_bitswap": 0.0
    }
  ]
  ```
- **Miner ranking ZSET:** `idx:miners:http` → member=`<miner_id>`, score=`success_rate_http`

**TTL:** all `stats:*` values are set with a 24h TTL and refreshed by the daily aggregation.

---

## Cron Aggregations

- Runs once at startup, then every **24h** (`statsPeriod = 24h`).
- **Client×Miner aggregation** groups by (`task.metadata.client`, `task.provider.id`) for `task.module="http"`.
  - Success rate = `ok / total` where `ok` counts `result.success=true`.
  - Writes a sorted (desc by HTTP success) JSON array per client to Redis key `stats:client:<client_addr>`.
- **Miner aggregation** groups by `task.provider.id` for `task.module="http"`.
  - Writes each miner’s JSON doc to `stats:miner:<miner_id>` and updates `idx:miners:http` ZSet with the success rate as score.
  - The ZSet is **rebuilt** on each aggregation run (`DEL` then `ZADD`).

---

## HTTP API

All responses are JSON. Percentages are formatted as strings with 2 decimals (e.g., `"97.50%"`).  
Default pagination: `page=1`, `page_size=15`, capped at `page_size<=200`.

### `GET /miners`

List miners ranked by HTTP success rate (desc), or fetch a single miner’s doc.

**Query Parameters:**

| Name         | Type   | Required | Description |
|--------------|--------|----------|-------------|
| `miner_addr` | string | no       | If set, returns **only** this miner (no pagination). |
| `page`       | int    | no       | Page number for ranked list (default 1). |
| `page_size`  | int    | no       | Items per page (default 15, max 200). |

**Responses:**

- **Single miner (`miner_addr` set):**
  ```json
  {
    "count": 1,
    "items": [
      {
        "miner_id": "f0...",
        "success_rate_http": "97.50%",
        "success_rate_graphsync": "0.00%",
        "success_rate_bitswap": "0.00%"
      }
    ]
  }
  ```

- **Ranked list:**
  ```json
  {
    "page": 1,
    "page_size": 15,
    "total": 1234,
    "items": [
      {
        "miner_id": "f01234",
        "success_rate_http": "99.10%",
        "success_rate_graphsync": "0.00%",
        "success_rate_bitswap": "0.00%"
      }
      // ...
    ]
  }
  ```

**Errors:**
- `500` if Redis ZSet or GET fails.
- Empty result returns `total=0` and `items=[]`.

---

### `GET /clients`

Fetch the miner list (with HTTP success rates) associated with a **specific client address**.

**Query Parameters:**

| Name          | Type   | Required | Description |
|---------------|--------|----------|-------------|
| `client_addr` | string | **yes**  | Client address key. |
| `page`        | int    | no       | Page number (default 1). |
| `page_size`   | int    | no       | Items per page (default 15, max 200). |

**Response:**
```json
{
  "page": 1,
  "page_size": 15,
  "total": 42,
  "items": [
    {
      "client_id": "f1...",
      "miner_id": "f0...",
      "success_rate_http": "92.00%",
      "success_rate_graphsync": "0.00%",
      "success_rate_bitswap": "0.00%"
    }
  ]
}
```

**Errors:**
- `400` if `client_addr` missing.
- `500` on Redis errors.
- If no data for the client, returns `{"count":0,"items":[]}`.

> Note: The list is stored sorted by HTTP success rate and re-sorted defensively on read.

---

### `GET /details`

Query raw task rows (module **http** only) directly from MongoDB.

**Query Parameters:**

| Name               | Type   | Required | Description |
|--------------------|--------|----------|-------------|
| `miner_addr`       | string | no       | Filter by miner address. |
| `client_addr`      | string | no       | Filter by client address. |
| `status`           | enum   | no       | `"0"` = **success** (`result.success=true`), `"1"` = **failure** (`false`). |
| `retrieval_method` | string | no       | Only `"http"` is supported; default `"http"`. |
| `page`             | int    | no       | Page number (default 1). |
| `page_size`        | int    | no       | Items per page (default 15, max 200). |

**Response:** (sorted by `created_at` desc)
```json
{
  "page": 1,
  "page_size": 15,
  "count": 15,
  "items": [
    {
      "miner_id": "f01234",
      "cid": "bafy...",
      "status": true,
      "return_code": "200",
      "response_message": "OK",
      "creation_time": "2025-09-12T10:22:33Z"
    }
  ]
}
```

**Errors:**
- `400` if `status` not in `{0,1}` or if non-http method is requested.
- `500` on MongoDB query/decoding errors.

---

## HTTP Status Codes & Errors

- `200 OK` – success with JSON body.
- `400 Bad Request` – missing/invalid query parameters.
- `500 Internal Server Error` – backend (Mongo/Redis) failures.

All error bodies are plain text or minimal JSON from `http.Error`/helpers.

---

## Examples

Assuming base: `http://203.160.84.158:58787`

### 1) Top miners by HTTP success (page 1, 20 per page)
```bash
curl "http://203.160.84.158:58787/miners?page=1&page_size=20"
```

### 2) Single miner doc
```bash
curl "http://203.160.84.158:58787/miners?miner_addr=f01234"
```

### 3) Client’s miner list (page 2)
```bash
curl "http://203.160.84.158:58787/clients?client_addr=f1client&page=2&page_size=10"
```

### 4) Recent HTTP success rows for a miner
```bash
curl "http://203.160.84.158:58787/details?miner_addr=f01234&status=0&page=1&page_size=50"
```

### 5) Recent HTTP failure rows for a client
```bash
curl "http://203.160.84.158:58787/details?client_addr=f1client&status=1"
```

> **Note:** `/details` currently has **no time window filter**; add one in Mongo filters if needed.

---

## Operational Notes

- Aggregation window: the code shows a commented time window in `$match` if you want rolling 24h stats; enable it to limit by `created_at >= now-24h`.
- Only `task.module = "http"` is aggregated today. `graphsync` and `bitswap` placeholders are present but always `0.00%` in responses.
- ZSet `idx:miners:http` is rebuilt each run (DEL + full repopulate). Consider **diff updates** for very large datasets.
- All `stats:*` keys have a 24h TTL; cron refresh keeps them alive.
- Percentages are formatted server-side to strings (e.g., `"97.50%"`).

---

## License

MIT (or your project’s chosen license).
