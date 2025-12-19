"""
Main ETL Pipeline for Apple Cloud Cost Analytics
Orchestrates extract → transform → load → verify → (optional) export to BigQuery
"""

from dotenv import load_dotenv
from ETL_Stages.extract import fetch_apple_cloud_costs, validate_records, load_to_raw
from ETL_Stages.transform import read_raw_apple_costs, transform_to_gold, aggregate_daily_costs
from ETL_Stages.load import upsert_gold_apple_costs, verify_gold_data
from datetime import date, timedelta
import sys
import os

# Load environment variables from .env file
load_dotenv()

# Check if BigQuery export is enabled
ENABLE_BIGQUERY_EXPORT = os.getenv("ENABLE_BIGQUERY_EXPORT", "false").lower() == "true"

if ENABLE_BIGQUERY_EXPORT:
    from ETL_Stages.export_to_bigquery import read_gold_data, export_to_bigquery


def run_pipeline(start_date: date | None = None, end_date: date | None = None):
    """Run the complete ETL pipeline.

    Args:
        start_date: First date to extract (default: 30 days ago)
        end_date: Last date to extract (default: today)

    Returns:
        True if pipeline succeeded, False otherwise
    """
    # Configuration
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=30)

    print("=" * 60)
    print("APPLE CLOUD COST ETL PIPELINE")
    print("=" * 60)
    print(f"Date range: {start_date} to {end_date}")
    print("Pipeline: Extract → Transform → Load")
    print("=" * 60)

    try:
        # ============================================================
        # EXTRACT
        # ============================================================
        print("\n" + "=" * 60)
        print("STAGE 1: EXTRACT")
        print("Fetching data from Apple Cloud Cost API")
        print("=" * 60)

        raw_data = fetch_apple_cloud_costs(start_date, end_date)
        print(f"✓ Fetched {len(raw_data)} records from API")

        valid_records = validate_records(raw_data)
        print(f"✓ Validated {len(valid_records)} records (passed raw schema)")

        raw_count = load_to_raw(valid_records)
        print(f"✓ Loaded {raw_count} records to raw_apple_cloud_costs")

        # ============================================================
        # TRANSFORM
        # ============================================================
        print("\n" + "=" * 60)
        print("STAGE 2: TRANSFORM")
        print("Converting raw data to CFO-grade numbers")
        print("=" * 60)

        raw_df = read_raw_apple_costs()
        print(f"✓ Read {len(raw_df)} records from raw")

        gold_df = transform_to_gold(raw_df)
        print(f"✓ Transformed {len(gold_df)} records to gold schema")

        aggregated_df = aggregate_daily_costs(gold_df)
        print(f"✓ Aggregated to {len(aggregated_df)} daily cost records")

        # ============================================================
        # LOAD
        # ============================================================
        print("\n" + "=" * 60)
        print("STAGE 3: LOAD")
        print("Publishing to gold table (CFO's internal API)")
        print("=" * 60)

        gold_count = upsert_gold_apple_costs(aggregated_df)
        print(f"✓ Upserted {gold_count} records to gold_apple_daily_costs")

        # ============================================================
        # VERIFY
        # ============================================================
        verify_gold_data()

        # ============================================================
        # EXPORT TO BIGQUERY (Optional)
        # ============================================================
        if ENABLE_BIGQUERY_EXPORT:
            print("\n" + "=" * 60)
            print("STAGE 4: EXPORT TO BIGQUERY")
            print("Publishing to cloud data warehouse")
            print("=" * 60)

            bq_df = read_gold_data()
            bq_count = export_to_bigquery(bq_df)
            print(f"✓ Exported {bq_count} records to BigQuery")

        # ============================================================
        # SUCCESS
        # ============================================================
        print("\n" + "=" * 60)
        print("✓ PIPELINE COMPLETE")
        print("=" * 60)
        print(f"Records in pipeline: {len(raw_data)} → {len(valid_records)} → {len(gold_df)} → {gold_count}")

        if ENABLE_BIGQUERY_EXPORT:
            print(f"Exported to BigQuery: {bq_count} records")
            print("\nDashboard ready for CFO's Monday morning review")
            print("\nNext steps:")
            print("  1. Open Looker Studio: https://lookerstudio.google.com")
            print("  2. Create new data source → BigQuery")
            print("  3. Select your BigQuery table")
            print("  4. Build time series chart (cost_date vs cost_usd)")
            print("  5. Add team and service breakdown tables")
        else:
            print("\nDashboard ready for CFO's Monday morning review")
            print("\nNext steps:")
            print("  1. Enable BigQuery export (set ENABLE_BIGQUERY_EXPORT=true)")
            print("  2. Or connect Looker Studio to PostgreSQL directly")
        print("=" * 60)

        return True

    except Exception as e:
        print("\n" + "=" * 60)
        print("✗ PIPELINE FAILED")
        print("=" * 60)
        print(f"Error: {e}")
        print("\nTroubleshooting:")
        print("  1. Is the mock API server running? (python mock_api_server.py)")
        print("  2. Is PostgreSQL running?")
        print("  3. Does the database 'apple_analytics' exist?")
        print("  4. Did you run schema.sql to create tables?")
        print("  5. Is DATABASE_URL configured correctly?")
        print("=" * 60)

        return False


def test_idempotency():
    """Test that running pipeline twice produces same results.

    This is critical for production pipelines. If the CFO refreshes
    the dashboard, they should see the same numbers, not duplicates.
    """
    print("\n" + "=" * 60)
    print("TESTING IDEMPOTENCY")
    print("=" * 60)
    print("Running pipeline twice to verify idempotent behavior...")
    print("Expected: Same row count in gold table after both runs")
    print("=" * 60)

    # First run
    print("\nFirst run:")
    success1 = run_pipeline()

    if not success1:
        print("✗ First run failed")
        return False

    # Get row count after first run
    import psycopg
    import os
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/apple_analytics")

    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gold_apple_daily_costs;")
            count1 = cur.fetchone()[0]

    # Second run
    print("\n\nSecond run:")
    success2 = run_pipeline()

    if not success2:
        print("✗ Second run failed")
        return False

    # Get row count after second run
    with psycopg.connect(DATABASE_URL) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM gold_apple_daily_costs;")
            count2 = cur.fetchone()[0]

    # Verify idempotency
    print("\n" + "=" * 60)
    print("IDEMPOTENCY TEST RESULTS")
    print("=" * 60)
    print(f"First run:  {count1} records in gold")
    print(f"Second run: {count2} records in gold")

    if count1 == count2:
        print("\n✓ IDEMPOTENCY TEST PASSED")
        print("Running pipeline multiple times produces consistent results")
        return True
    else:
        print("\n✗ IDEMPOTENCY TEST FAILED")
        print("Row count changed between runs - check upsert logic")
        return False


if __name__ == "__main__":
    # Check command line arguments
    if len(sys.argv) > 1 and sys.argv[1] == "--test-idempotency":
        test_idempotency()
    else:
        run_pipeline()
