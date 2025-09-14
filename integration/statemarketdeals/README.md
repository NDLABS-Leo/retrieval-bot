# Claims Crawler

This project is a **Filecoin Claims Crawler** that periodically fetches **Verified Registry Claims** from a Lotus node and stores them in MongoDB.

---

## 🚀 Features

- Connects to a Lotus full node (via RPC API).
- Retrieves all miners and filters those with non-zero power.
- Fetches claims for each miner using `StateGetClaims`.
- Stores claims into MongoDB with a unique `(provider_id, claim_id)` index.
- Runs once at startup and then every 12 hours.

---

## ⚙️ Requirements

- **Go** ≥ 1.20 (recommended 1.22.x)
- **MongoDB** ≥ 5.0
- **Lotus full node** (fully synced)
- Linux / macOS (Ubuntu 20.04/22.04 recommended for production)

---

## 🔧 Environment Variables

Set the following environment variables before running:

```bash
# Lotus node RPC URL
export FULLNODE_API_URL="ws://127.0.0.1:1234/rpc/v1"

# Lotus API Token (optional if authentication is enabled)
export FULLNODE_API_TOKEN="your_lotus_token"

# MongoDB connection
export MONGO_URI="mongodb://127.0.0.1:27017"
export MONGO_DB="filstats"
export MONGO_CLAIMS_COLL="claims"
```

---

## 🔨 Build

Run the following in the project root:

```bash
go mod tidy
go build -o claims-crawler main.go
```

---

## ▶️ Run

```bash
./claims-crawler
```

Expected logs:

```
INFO    claims-crawler boot lotus=ws://127.0.0.1:1234/rpc/v1 mongo=mongodb://127.0.0.1:27017 db=filstats coll=claims interval=12h
INFO    claims-crawler miners listed: 4332
INFO    claims-crawler active miners (non-zero power): 2988
INFO    claims-crawler crawl finished upserts=12456
```

---

## 📦 Deploy with systemd

1. Create `/etc/systemd/system/claims-crawler.service`:

```ini
[Unit]
Description=Filecoin Claims Crawler
After=network.target

[Service]
WorkingDirectory=/home/ubuntu/claims-crawler
ExecStart=/home/ubuntu/claims-crawler/claims-crawler
Restart=always
Environment=FULLNODE_API_URL=ws://127.0.0.1:1234/rpc/v1
Environment=FULLNODE_API_TOKEN=your_lotus_token
Environment=MONGO_URI=mongodb://127.0.0.1:27017
Environment=MONGO_DB=filstats
Environment=MONGO_CLAIMS_COLL=claims

[Install]
WantedBy=multi-user.target
```

2. Reload and start the service:

```bash
sudo systemctl daemon-reload
sudo systemctl enable claims-crawler
sudo systemctl start claims-crawler
sudo systemctl status claims-crawler
```

---

## 📊 MongoDB Schema

Collection: `claims`

Example document:

```json
{
  "claim_id": 1,
  "provider_id": 1234,
  "client_id": 5678,
  "client_addr": "f1xxxx",
  "data_cid": "bafy...",
  "size": 34359738368,
  "term_min": 180,
  "term_max": 540,
  "term_start": 1234567,
  "sector": 12,
  "miner_addr": "f01234",
  "updated_at": "2025-09-12T08:23:45Z"
}
```

Indexes:
- Unique index on `(provider_id, claim_id)`

---

## 🧩 Troubleshooting

1. **`missing env FULLNODE_API_URL`**  
   → Ensure the Lotus RPC URL is set.

2. **MongoDB connection errors**  
   → Verify MongoDB is running and credentials are correct.

3. **`StateGetClaims` returns empty**  
   → The miner may not have any claims, which is expected.

---
