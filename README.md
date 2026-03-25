# Customer Data Pipeline

A data pipeline that fetches customer data from a mock API and ingests it into PostgreSQL.

## Architecture

- **Mock Server** (Flask) - Serves customer data from JSON on port 5000
- **Pipeline Service** (FastAPI) - Ingests data and serves from DB on port 8000
- **PostgreSQL** - Stores customer records

## Setup

```bash
docker-compose up -d
```

## Usage

### Fetch customers from mock server
```bash
curl http://localhost:5000/api/customers?page=1&limit=5
```

### Run ingestion
```bash
curl -X POST http://localhost:8000/api/ingest
```

### Get customers from DB
```bash
curl http://localhost:8000/api/customers?page=1&limit=5
```

### Get single customer
```bash
curl http://localhost:8000/api/customers/CUST001
```
