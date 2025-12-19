"""
Export gold table data to Google BigQuery
Demonstrates: Cloud data warehouse integration, production deployment patterns
"""

import psycopg
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://student:learner2024@localhost:5432/apple_analytics")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "apple_analytics")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE", "gold_apple_daily_costs")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


def create_bigquery_client():
    """Create BigQuery client with credentials.

    Returns:
        bigquery.Client: Authenticated BigQuery client

    Raises:
        ValueError: If credentials or project ID not configured
    """
    if not BIGQUERY_PROJECT_ID:
        raise ValueError(
            "BIGQUERY_PROJECT_ID not set. Add it to your .env file:\n"
            "BIGQUERY_PROJECT_ID=your-gcp-project-id"
        )

    if GOOGLE_APPLICATION_CREDENTIALS:
        # Use service account credentials
        credentials = service_account.Credentials.from_service_account_file(
            GOOGLE_APPLICATION_CREDENTIALS,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        client = bigquery.Client(credentials=credentials, project=BIGQUERY_PROJECT_ID)
    else:
        # Use default credentials (gcloud auth application-default login)
        client = bigquery.Client(project=BIGQUERY_PROJECT_ID)

    return client


def read_gold_data() -> pd.DataFrame:
    """Read all records from gold_apple_daily_costs table.

    Returns:
        DataFrame with gold table data
    """
    print("Reading data from PostgreSQL gold table...")

    with psycopg.connect(DATABASE_URL) as conn:
        df = pd.read_sql("""
            SELECT
                cost_date,
                subscription_id,
                service_name,
                team,
                cost_center,
                cost_usd
            FROM gold_apple_daily_costs
            ORDER BY cost_date DESC, cost_usd DESC;
        """, conn)

    # Convert date column to proper datetime format for BigQuery
    df['cost_date'] = pd.to_datetime(df['cost_date'])

    print(f"✓ Read {len(df)} records from PostgreSQL")
    return df


def create_bigquery_dataset(client: bigquery.Client):
    """Create BigQuery dataset if it doesn't exist.

    Args:
        client: BigQuery client
    """
    dataset_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}"

    try:
        client.get_dataset(dataset_id)
        print(f"✓ Dataset {dataset_id} already exists")
    except Exception:
        print(f"Creating dataset {dataset_id}...")
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"  # Change if needed
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"✓ Created dataset {dataset_id}")


def export_to_bigquery(df: pd.DataFrame) -> int:
    """Export DataFrame to BigQuery.

    Uses WRITE_TRUNCATE to replace table contents (idempotent).
    In production, you might use WRITE_APPEND with partitioning.

    Args:
        df: DataFrame to export

    Returns:
        Number of rows exported

    Raises:
        Exception: If BigQuery export fails
    """
    if df.empty:
        print("No data to export")
        return 0

    print(f"\nExporting {len(df)} records to BigQuery...")

    # Prepare data - convert datetime to string format for CSV
    df = df.copy()
    df['cost_date'] = df['cost_date'].dt.strftime('%Y-%m-%d')

    # Create client
    client = create_bigquery_client()
    print(f"✓ Connected to BigQuery project: {BIGQUERY_PROJECT_ID}")

    # Create dataset if needed
    create_bigquery_dataset(client)

    # Table reference
    table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

    # Configure load job for CSV format
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace table
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        schema=[
            bigquery.SchemaField("cost_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("subscription_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("service_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("team", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("cost_center", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("cost_usd", "NUMERIC", mode="REQUIRED"),
        ],
    )

    # Convert DataFrame to CSV in memory
    import io
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # Load data from CSV
    print(f"Loading to {table_id}...")
    job = client.load_table_from_file(csv_buffer, table_id, job_config=job_config)

    # Wait for job to complete
    job.result()

    # Verify
    table = client.get_table(table_id)
    print(f"✓ Loaded {table.num_rows} rows to BigQuery")
    print(f"✓ Table: {table_id}")

    return table.num_rows


def verify_bigquery_data():
    """Verify data in BigQuery and show sample queries.

    This demonstrates what the CFO will see in Looker Studio.
    """
    print("\n" + "=" * 60)
    print("Verifying BigQuery data")
    print("=" * 60)

    client = create_bigquery_client()
    table_id = f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"

    # Total records
    query = f"""
        SELECT COUNT(*) as total_records
        FROM `{table_id}`
    """
    result = client.query(query).result()
    total = list(result)[0].total_records
    print(f"\nTotal records in BigQuery: {total}")

    # Date range
    query = f"""
        SELECT
            MIN(cost_date) as earliest_date,
            MAX(cost_date) as latest_date
        FROM `{table_id}`
    """
    result = client.query(query).result()
    row = list(result)[0]
    print(f"Date range: {row.earliest_date} to {row.latest_date}")

    # Top teams by cost
    query = f"""
        SELECT
            team,
            ROUND(SUM(cost_usd), 2) as total_cost
        FROM `{table_id}`
        GROUP BY team
        ORDER BY total_cost DESC
        LIMIT 5
    """
    print("\nTop 5 teams by cost:")
    result = client.query(query).result()
    for row in result:
        print(f"  {row.team}: ${row.total_cost:,.2f}")

    # Sample Looker Studio query
    print("\n" + "=" * 60)
    print("Sample Looker Studio Query")
    print("=" * 60)
    print(f"""
SELECT
    cost_date,
    team,
    service_name,
    SUM(cost_usd) as total_cost
FROM `{table_id}`
WHERE cost_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY cost_date, team, service_name
ORDER BY cost_date DESC, total_cost DESC
    """)


def main():
    """Run the BigQuery export pipeline"""
    print("=" * 60)
    print("EXPORT TO BIGQUERY")
    print("=" * 60)

    try:
        # Step 1: Read from PostgreSQL
        print("\nStep 1: Reading from PostgreSQL")
        print("-" * 60)
        df = read_gold_data()

        # Step 2: Export to BigQuery
        print("\nStep 2: Exporting to BigQuery")
        print("-" * 60)
        count = export_to_bigquery(df)

        # Step 3: Verify
        print("\nStep 3: Verifying data")
        print("-" * 60)
        verify_bigquery_data()

        print("\n" + "=" * 60)
        print(f"✓ EXPORT COMPLETE: {count} records in BigQuery")
        print("=" * 60)
        print("\nNext steps:")
        print("  1. Open Looker Studio: https://lookerstudio.google.com")
        print("  2. Create new data source → BigQuery")
        print(f"  3. Select project: {BIGQUERY_PROJECT_ID}")
        print(f"  4. Select dataset: {BIGQUERY_DATASET}")
        print(f"  5. Select table: {BIGQUERY_TABLE}")
        print("  6. Build your dashboard!")
        print("=" * 60)

        return True

    except Exception as e:
        print("\n" + "=" * 60)
        print("✗ EXPORT FAILED")
        print("=" * 60)
        print(f"Error: {e}")
        print("\nTroubleshooting:")
        print("  1. Is BIGQUERY_PROJECT_ID set in .env?")
        print("  2. Are BigQuery credentials configured?")
        print("     - Service account: Set GOOGLE_APPLICATION_CREDENTIALS")
        print("     - Or run: gcloud auth application-default login")
        print("  3. Does the service account have BigQuery permissions?")
        print("     - BigQuery Data Editor")
        print("     - BigQuery Job User")
        print("=" * 60)

        return False


if __name__ == "__main__":
    main()
