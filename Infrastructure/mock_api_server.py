"""
Mock API Server for Apple Cloud Cost API (FastAPI version)
Simulates realistic data with realistic problems as described in the lecture
"""

from datetime import datetime, timedelta
import random as _random
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel


# Reproducible randomness (matches your Flask version pattern)
setrandom = _random.Random(42)
random = setrandom  # shadow-like alias to keep the rest unchanged


app = FastAPI(
    title="Apple Cloud Cost API (Mock)",
    version="1.0",
    description="This mock API simulates realistic data quality issues as described in the ETL pipeline lecture.",
)

# Realistic team names from Apple
TEAMS = [
    "Siri Infrastructure",
    "Apple Maps",
    "iCloud Services",
    "Apple Music",
    "App Store Backend",
    "FaceTime Infrastructure",
    "Apple Pay Systems",
    "Photos Infrastructure",
]

# Cloud services with realistic names
SERVICES = [
    "EC2 Compute",
    "S3 Storage",
    "RDS Database",
    "CloudFront CDN",
    "Lambda Functions",
    "EBS Volumes",
    "Data Transfer",
    "ElastiCache",
]

# Subscription IDs
SUBSCRIPTIONS = [
    "aws-prod-001",
    "aws-prod-002",
    "gcp-prod-001",
    "azure-prod-001",
    "aws-dev-001",
]

# Cost centers
COST_CENTERS = [
    "CC-4521",
    "CC-4522",
    "CC-4523",
    "CC-4524",
    "CC-4525",
]

# Currencies for different regions
CURRENCIES = ["USD", "EUR", "GBP", "JPY"]


class CostRecord(BaseModel):
    usage_date: str
    subscription_id: str
    service_name: str
    team: Optional[str] = None
    cost_center: Optional[str] = None
    currency: Optional[str] = None
    cost: Optional[str] = None  # optional because some injected-problem branches omit it (same as your original code)


def generate_realistic_cost_record(usage_date: datetime, inject_problem: bool = False) -> Dict[str, Any]:
    """Generate a single cost record with optional data quality issues"""

    record: Dict[str, Any] = {
        "usage_date": usage_date.strftime("%Y-%m-%d"),
        "subscription_id": random.choice(SUBSCRIPTIONS),
        "service_name": random.choice(SERVICES),
        "team": random.choice(TEAMS),
        "cost_center": random.choice(COST_CENTERS),
        "currency": random.choice(CURRENCIES),
    }

    # Generate realistic costs based on service type
    base_costs = {
        "EC2 Compute": (10000, 150010),
        "S3 Storage": (5001, 80000),
        "RDS Database": (8000, 120000),
        "CloudFront CDN": (3000, 50010),
        "Lambda Functions": (1000, 20000),
        "EBS Volumes": (2000, 30000),
        "Data Transfer": (4000, 60000),
        "ElastiCache": (6000, 90000),
    }

    min_cost, max_cost = base_costs[record["service_name"]]
    cost_value = random.uniform(min_cost, max_cost)

    # Inject realistic problems as described in the lecture
    if inject_problem:
        problem_type = random.randint(1, 6)

        if problem_type == 1:
            # Cost as string "N/A" when billing delayed
            record["cost"] = "N/A"
        elif problem_type == 2:
            # Cost as "pending"
            record["cost"] = "pending"
        elif problem_type == 3:
            # Missing team (null)
            record["team"] = None
        elif problem_type == 4:
            # Empty cost center
            record["cost_center"] = ""
        elif problem_type == 5:
            # Cost with commas (string formatting)
            record["cost"] = f"{cost_value:,.2f}"
        elif problem_type == 6:
            # Missing currency
            record["currency"] = None
    else:
        # Normal case: cost as string (as API does)
        record["cost"] = f"{cost_value:.2f}"

    return record


@app.get("/v1/cloud-costs", response_model=List[CostRecord])
def get_cloud_costs(
    start_date: Optional[str] = Query(default=None, description="YYYY-MM-DD (optional, defaults to 30 days ago)"),
    end_date: Optional[str] = Query(default=None, description="YYYY-MM-DD (optional, defaults to today)"),
):
    """
    Mock endpoint for Apple Cloud Cost API
    """

    # Parse query parameters
    try:
        if end_date:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end_dt = datetime.now()

        if start_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            start_dt = end_dt - timedelta(days=30)

    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    # (Optional sanity check; remove if you want to keep behavior ultra-minimal)
    if start_dt > end_dt:
        raise HTTPException(status_code=400, detail="start_date must be <= end_date")

    # Generate records for each day
    records: List[Dict[str, Any]] = []
    current_date = start_dt

    while current_date <= end_dt:
        # Generate 5-15 records per day (different services/subscriptions)
        num_records = random.randint(5, 15)

        for _ in range(num_records):
            # 10% chance of data quality issues (as mentioned in lecture)
            inject_problem = random.random() < 0.10
            record = generate_realistic_cost_record(current_date, inject_problem)
            records.append(record)

        current_date += timedelta(days=1)

    return records


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "Apple Cloud Cost API (Mock)"}


@app.get("/")
def index():
    """API documentation"""
    return {
        "service": "Apple Cloud Cost API (Mock)",
        "version": "1.0",
        "endpoints": {
            "/v1/cloud-costs": {
                "method": "GET",
                "description": "Get cloud cost records",
                "parameters": {"start_date": "YYYY-MM-DD (optional)", "end_date": "YYYY-MM-DD (optional)"},
            },
            "/health": {"method": "GET", "description": "Health check"},
        },
        "note": "This mock API simulates realistic data quality issues as described in the ETL pipeline lecture",
    }


if __name__ == "__main__":
    import uvicorn

    print("=" * 60)
    print("Apple Cloud Cost API (Mock Server) - FastAPI")
    print("=" * 60)
    print("Starting server on http://localhost:5001")
    print("\nEndpoints:")
    print("  GET /v1/cloud-costs?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD")
    print("  GET /health")
    print("\nSwagger UI:")
    print("  http://localhost:5001/docs")
    print("\nThis mock API includes realistic data quality issues:")
    print("  - Cost values as 'N/A' or 'pending'")
    print("  - Missing team or currency fields")
    print("  - String-formatted costs with commas")
    print("  - ~10% of records have problems (as in production)")
    print("=" * 60)

    uvicorn.run(app, host="0.0.0.0", port=5001, reload=True)
