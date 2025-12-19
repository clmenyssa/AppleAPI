"""
Load transformed data to gold table
Demonstrates: Upsert logic, idempotency, ON CONFLICT handling
"""

import pandas as pd
import psycopg
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/apple_analytics")


def upsert_gold_apple_costs(df: pd.DataFrame) -> int:
    """Upsert DataFrame to gold_apple_daily_costs.

    Uses ON CONFLICT to update existing rows.
    This makes the pipeline idempotent: run it twice,
    get the same result. The CFO sees consistent numbers
    regardless of how many times we rerun.

    The composite primary key (cost_date, subscription_id, service_name)
    determines what constitutes a "duplicate" record.

    Args:
        df: DataFrame with gold records to upsert

    Returns:
        Number of records upserted

    Raises:
        psycopg.Error: If database operation fails
    """
    if df.empty:
        print("No records to upsert")
        return 0

    print(f"Upserting {len(df)} records to gold_apple_daily_costs...")

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            with conn.cursor() as cur:
                inserted = 0
                updated = 0

                for _, row in df.iterrows():
                    # Check if row already exists before insert
                    cur.execute("""
                        SELECT cost_usd FROM gold_apple_daily_costs
                        WHERE cost_date = %s
                          AND subscription_id = %s
                          AND service_name = %s;
                    """, (
                        row['cost_date'],
                        row['subscription_id'],
                        row['service_name']
                    ))
                    existing = cur.fetchone()

                    # Insert or update
                    cur.execute("""
                        INSERT INTO gold_apple_daily_costs
                            (cost_date, subscription_id, service_name,
                             team, cost_center, cost_usd)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (cost_date, subscription_id, service_name)
                        DO UPDATE SET
                            team = EXCLUDED.team,
                            cost_center = EXCLUDED.cost_center,
                            cost_usd = EXCLUDED.cost_usd;
                    """, (
                        row['cost_date'],
                        row['subscription_id'],
                        row['service_name'],
                        row['team'],
                        row['cost_center'],
                        row['cost_usd']
                    ))

                    if existing:
                        updated += 1
                    else:
                        inserted += 1

            conn.commit()

        print(f"\nLoad summary:")
        print(f"  Inserted: {inserted} new records")
        print(f"  Updated: {updated} existing records")
        print(f"  Total: {len(df)} records")
        print("✓ Database commit successful")

    except psycopg.Error as e:
        print(f"✗ Database error: {e}")
        raise

    return len(df)


def verify_gold_data():
    """Verify gold table contains expected data.

    Runs business queries to check data quality.
    """
    print("\n" + "=" * 60)
    print("Verifying gold table data")
    print("=" * 60)

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            # Total record count
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM gold_apple_daily_costs;")
                count = cur.fetchone()[0]
                print(f"\nTotal records in gold: {count}")

            # Date range
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT MIN(cost_date), MAX(cost_date)
                    FROM gold_apple_daily_costs;
                """)
                min_date, max_date = cur.fetchone()
                print(f"Date range: {min_date} to {max_date}")

            # Total cost by team (CFO's first question)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT team, SUM(cost_usd) as total_cost
                    FROM gold_apple_daily_costs
                    GROUP BY team
                    ORDER BY total_cost DESC
                    LIMIT 5;
                """)
                print("\nTop 5 teams by cost:")
                for team, cost in cur.fetchall():
                    print(f"  {team}: ${cost:,.2f}")

            # Cost by service (CFO's second question)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT service_name, SUM(cost_usd) as total_cost
                    FROM gold_apple_daily_costs
                    GROUP BY service_name
                    ORDER BY total_cost DESC
                    LIMIT 5;
                """)
                print("\nTop 5 services by cost:")
                for service, cost in cur.fetchall():
                    print(f"  {service}: ${cost:,.2f}")

            # Recent daily trend
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT cost_date, SUM(cost_usd) as daily_cost
                    FROM gold_apple_daily_costs
                    GROUP BY cost_date
                    ORDER BY cost_date DESC
                    LIMIT 7;
                """)
                print("\nLast 7 days total cost:")
                for date, cost in cur.fetchall():
                    print(f"  {date}: ${cost:,.2f}")

    except psycopg.Error as e:
        print(f"✗ Verification failed: {e}")


def main():
    """Run the load pipeline with verification"""
    print("=" * 60)
    print("LOAD: Gold Table")
    print("=" * 60)
    print("\n⚠️  This script requires transformed data.")
    print("Run the full pipeline with main.py instead.")
    print("\nTo verify existing data:")
    verify_gold_data()


if __name__ == "__main__":
    main()
