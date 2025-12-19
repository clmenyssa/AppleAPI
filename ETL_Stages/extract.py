"""
Extract data from Apple Cloud Cost API and load to raw table
Demonstrates: API calls, Pydantic validation, failure thresholds, idempotent loading
"""

import requests
import psycopg
import os
from datetime import date, timedelta
from dotenv import load_dotenv
from Data_Layer.contracts import AppleCloudCostRaw

# Load environment variables from .env file
load_dotenv()

# Configuration from environment variables with defaults
API_URL = os.getenv("API_URL", "http://localhost:5001/v1/cloud-costs")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/apple_analytics")


def fetch_apple_cloud_costs(start_date: date, end_date: date) -> list[dict]:
    """Fetch cloud costs from Apple's internal API.

    In production, this would include:
    - OAuth token refresh
    - Retry logic with exponential backoff
    - Pagination handling for large date ranges

    For the workshop, the mock API handles these complexities.

    Args:
        start_date: First date to fetch (inclusive)
        end_date: Last date to fetch (inclusive)

    Returns:
        List of raw cost records as dictionaries

    Raises:
        requests.HTTPError: If API returns error status
        requests.Timeout: If API doesn't respond within timeout
    """
    params = {
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat()
    }

    print(f"Fetching from API: {API_URL}")
    print(f"Date range: {start_date} to {end_date}")

    try:
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"API returned {len(data)} records")
        return data

    except requests.exceptions.Timeout:
        print("ERROR: API request timed out after 30 seconds")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"ERROR: API returned error status: {e}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"ERROR: Failed to connect to API: {e}")
        raise


def validate_records(raw_data: list[dict]) -> list[AppleCloudCostRaw]:
    """Validate each record against the raw schema.

    Records that fail validation are logged and skipped.
    We do not stop the pipeline for individual bad records.
    We DO stop if the failure rate exceeds 10%.

    This threshold catches systemic problems (API schema changed)
    while allowing individual bad records (transient data issues).

    Args:
        raw_data: List of unvalidated records from API

    Returns:
        List of validated AppleCloudCostRaw objects

    Raises:
        RuntimeError: If failure rate exceeds 10%
    """
    valid_records = []
    failed_count = 0
    failed_examples = []

    for i, record in enumerate(raw_data):
        try:
            validated = AppleCloudCostRaw.model_validate(record)
            valid_records.append(validated)
        except Exception as e:
            failed_count += 1
            # Keep first 3 failures as examples for debugging
            if len(failed_examples) < 3:
                failed_examples.append(f"Record {i}: {e}")
            print(f"Record {i} failed validation: {e}")

    # Business rule: too many failures indicates a systemic problem
    failure_rate = failed_count / len(raw_data) if raw_data else 0

    print("\nValidation summary:")
    print(f"  Valid: {len(valid_records)}")
    print(f"  Failed: {failed_count}")
    print(f"  Failure rate: {failure_rate:.1%}")

    if failure_rate > 0.10:
        print("\n⚠️  VALIDATION FAILURE THRESHOLD EXCEEDED")
        print("Example failures:")
        for example in failed_examples:
            print(f"  - {example}")
        raise RuntimeError(
            f"Validation failure rate {failure_rate:.1%} exceeds 10% threshold. "
            f"API may have changed schema. Investigate before proceeding."
        )

    return valid_records


def load_to_raw(records: list[AppleCloudCostRaw]) -> int:
    """Insert validated records into raw table.

    Truncates before insert for idempotent reload.
    Running the pipeline twice produces the same result.

    This is a simple approach for learning. In production,
    you might use incremental loads with deduplication.

    Args:
        records: List of validated records to insert

    Returns:
        Number of records inserted

    Raises:
        psycopg.Error: If database operation fails
    """
    if not records:
        print("No records to load")
        return 0

    print("\nConnecting to database...")
    print(f"Database: {DATABASE_URL.split('@')[-1] if '@' in DATABASE_URL else DATABASE_URL}")

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                # Clear raw table for idempotent reload
                print("Truncating raw_apple_cloud_costs...")
                cur.execute("TRUNCATE TABLE raw_apple_cloud_costs RESTART IDENTITY;")

                # Insert each record
                print(f"Inserting {len(records)} records...")
                for record in records:
                    cur.execute("""
                        INSERT INTO raw_apple_cloud_costs
                            (usage_date, subscription_id, service_name,
                             cost, currency, team, cost_center)
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """, (
                        record.usage_date,
                        record.subscription_id,
                        record.service_name,
                        record.cost,
                        record.currency,
                        record.team,
                        record.cost_center
                    ))

            conn.commit()
            print("✓ Database commit successful")

    except psycopg.Error as e:
        print(f"✗ Database error: {e}")
        raise

    return len(records)


def main():
    """Run the extraction pipeline"""
    print("=" * 60)
    print("EXTRACT: Apple Cloud Cost Data")
    print("=" * 60)

    # Extract last 30 days of Apple cloud costs
    end_date = date.today()
    start_date = end_date - timedelta(days=30)

    try:
        # Step 1: Fetch from API
        print("\nStep 1: Fetching from API")
        print("-" * 60)
        raw_data = fetch_apple_cloud_costs(start_date, end_date)

        # Step 2: Validate with Pydantic
        print("\nStep 2: Validating records")
        print("-" * 60)
        valid_records = validate_records(raw_data)

        # Step 3: Load to raw table
        print("\nStep 3: Loading to raw table")
        print("-" * 60)
        count = load_to_raw(valid_records)

        print("\n" + "=" * 60)
        print(f"✓ EXTRACTION COMPLETE: {count} records loaded")
        print("=" * 60)

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ EXTRACTION FAILED: {e}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    main()
