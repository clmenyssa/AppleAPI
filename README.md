# Apple Cloud Cost ETL Pipeline

An end-to-end ETL pipeline project to teach production data engineering concepts. This pipeline ingests Apple's cloud infrastructure costs from a mock API, validates and transforms the data through multiple quality gates, and publishes "CFO-grade" numbers to a PostgreSQL database.

## Overview

This project demonstrates real-world ETL patterns including:
- Data validation with Pydantic
- Currency conversion and aggregation
- Idempotent pipeline design
- Data quality gates and error handling
- Optional BigQuery export for dashboards

## Tech Stack

- **Python 3.11+**
- **FastAPI** - Mock API server
- **PostgreSQL 14** - Data warehouse
- **Docker & Docker Compose** - Containerization
- **Pandas** - Data manipulation
- **Pydantic v2** - Data validation

## Project Structure

```
AppleAPI/
├── Orchestration/
│   └── main.py              # Main ETL pipeline orchestrator
├── ETL_Stages/
│   ├── extract.py           # API data fetching & raw validation
│   ├── transform.py         # Currency conversion, gold schema validation
│   ├── load.py              # Upsert to gold table & verification
│   └── export_to_bigquery.py # Optional BigQuery export
├── Data_Layer/
│   ├── contracts.py         # Pydantic models
│   └── schema.sql           # PostgreSQL table definitions
├── Infrastructure/
│   ├── mock_api_server.py   # FastAPI mock server
│   ├── setup_database.sh    # Database initialization script
│   └── requirements.txt     # API server dependencies
├── requirements.txt         # Pipeline dependencies
├── docker-compose.yml       # Multi-container setup
└── Dockerfile.api           # Mock API container
```

## Prerequisites

- Python 3.11 or higher
- Docker and Docker Compose (recommended)
- OR PostgreSQL 14+ installed locally

## Installation

### Option 1: Docker Compose (Recommended)

1. **Clone the repository**
   ```bash
   cd AppleAPI
   ```

2. **Start all services**
   ```bash
   docker-compose up -d
   ```
   This starts:
   - PostgreSQL database (port 5432)
   - Mock API server (port 5001)

3. **Install Python dependencies**
In your virtual environment, run:
   ```bash
   pip install -r requirements.txt
   ```

4. **Verify setup**
   ```bash
   # Check containers are running
   docker-compose ps

   # Test API is responding
   curl http://localhost:5001/health

   # Test Pydantic validation
   python -m Data_Layer.contracts
   ```

### Option 2: Local Development

1. **Install Python dependencies**
   ```bash
   pip install -r requirements.txt
   ```

2. **Setup PostgreSQL database**
   ```bash
   bash Infrastructure/setup_database.sh
   ```

3. **Start the Mock API server**
   ```bash
   cd Infrastructure
   pip install -r requirements.txt
   python mock_api_server.py
   ```
   API runs on http://localhost:5001

## Configuration

Create a `.env` file or use the existing one:

```env
# Mock API Server
API_URL=http://localhost:5001/v1/cloud-costs

# PostgreSQL Database
DATABASE_URL=postgresql://student:learner2024@localhost:5432/apple_analytics

# BigQuery Export (Optional)
ENABLE_BIGQUERY_EXPORT=false
BIGQUERY_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET=apple_analytics
BIGQUERY_TABLE=gold_apple_daily_costs
```

## Running the Pipeline

**Important:** Run all commands from the project root directory (`AppleAPI/`).

### Full Pipeline Execution

```bash
python -m Orchestration.main
```

### Test Idempotency

Run the pipeline twice to verify it produces consistent results:

```bash
python -m Orchestration.main --test-idempotency
```

### Run Individual Stages

```bash
# Extract stage only
python -m ETL_Stages.extract

# Transform stage only
python -m ETL_Stages.transform

# Load stage only
python -m ETL_Stages.load
```

## Pipeline Workflow

```
[EXTRACT]
  │ Fetch data from API (30-day window)
  │ Validate against AppleCloudCostRaw schema
  │ Load to raw_apple_cloud_costs table
  ▼
[TRANSFORM]
  │ Convert currencies to USD
  │ Validate against AppleCloudCostGold schema
  │ Aggregate to daily grain
  ▼
[LOAD]
  │ Upsert to gold_apple_daily_costs table
  │ Verify data quality
  ▼
[EXPORT] (Optional)
  │ Export to BigQuery for dashboards
  ▼
SUCCESS
```

## Database Schema

### Raw Table (`raw_apple_cloud_costs`)
Landing zone for untrusted API data. All columns are TEXT type.

### Gold Table (`gold_apple_daily_costs`)
CFO-grade data with enforced types and constraints:
- Composite primary key: (cost_date, subscription_id, service_name)
- Non-negative costs
- Required team and cost_center fields

## API Documentation

When the mock API is running, access Swagger docs at:
http://localhost:5001/docs

## Stopping Services

```bash
# Stop all containers
docker-compose down

# Stop and remove volumes (reset database)
docker-compose down -v
```

## Troubleshooting

### Port 5001 Already in Use (macOS)
macOS uses port 5001 for AirPlay. The docker-compose is configured to handle this, but if you have issues:
```bash
# Check what's using the port
lsof -i :5001

# Use a different port in .env
API_URL=http://localhost:5002/v1/cloud-costs
```

### Database Connection Issues
```bash
# Verify PostgreSQL is running
docker-compose ps

# Check logs
docker-compose logs postgres
```

### Reset Everything
```bash
docker-compose down -v
docker-compose up -d
```
