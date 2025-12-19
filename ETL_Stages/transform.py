"""
Transform raw data to gold schema
Demonstrates: Currency conversion, Pydantic validation, aggregation, pandas operations
"""

import pandas as pd
import psycopg
import os
from decimal import Decimal
from dotenv import load_dotenv
from Data_Layer.contracts import AppleCloudCostGold

# Load environment variables from .env file
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://localhost:5432/apple_analytics")

# Exchange rates for currency conversion
# In production, these would come from a real-time forex API
# For the workshop, we use simplified static rates
EXCHANGE_RATES = {
    'USD': Decimal('1.0'),
    'EUR': Decimal('1.08'),  # 1 EUR = 1.08 USD
    'GBP': Decimal('1.27'),  # 1 GBP = 1.27 USD
    'JPY': Decimal('0.0067'),  # 1 JPY = 0.0067 USD
}


def read_raw_apple_costs() -> pd.DataFrame:
    """Read all records from raw_apple_cloud_costs.

    Returns:
        DataFrame with raw cost records

    Raises:
        psycopg.Error: If database connection fails
    """
    print("Reading from raw_apple_cloud_costs...")

    try:
        with psycopg.connect(DATABASE_URL) as conn:
            df = pd.read_sql("""
                SELECT usage_date, subscription_id, service_name,
                       cost, currency, team, cost_center
                FROM raw_apple_cloud_costs;
            """, conn)

        print(f"Read {len(df)} records from raw table")
        return df

    except psycopg.Error as e:
        print(f"✗ Database error: {e}")
        raise


def convert_to_usd(cost_str: str, currency: str | None) -> Decimal:
    """Convert cost to USD using exchange rates.

    Business rule: All costs reported in USD for consistency.
    The CFO compares across regions; mixed currencies confuse.

    Args:
        cost_str: Cost as string (may have commas, may be "N/A")
        currency: Currency code (USD, EUR, GBP, JPY) or None

    Returns:
        Cost in USD as Decimal

    Raises:
        ValueError: If cost cannot be parsed
        InvalidOperation: If Decimal conversion fails
    """
    # Clean and parse the cost string
    cleaned = cost_str.strip().replace(',', '')

    # Check for invalid values
    if cleaned in ('', 'N/A', 'null', 'None', 'pending'):
        raise ValueError(f"Invalid cost value: '{cost_str}'")

    base_cost = Decimal(cleaned)

    # Default to USD if no currency specified
    curr = (currency or 'USD').upper()

    # Get exchange rate (default to 1.0 for unknown currencies)
    rate = EXCHANGE_RATES.get(curr, Decimal('1.0'))

    return base_cost * rate


def transform_to_gold(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Convert raw records to gold schema.

    Each row passes through AppleCloudCostGold validation.
    Invalid rows are logged and skipped.

    This is the boundary where untrusted data (raw) becomes
    trusted data (gold). Pydantic enforces business rules.

    Args:
        raw_df: DataFrame with raw records

    Returns:
        DataFrame with validated gold records

    Raises:
        No exceptions raised - invalid rows are logged and skipped
    """
    gold_records = []
    failed_count = 0

    print(f"\nTransforming {len(raw_df)} raw records to gold...")

    for idx, row in raw_df.iterrows():
        try:
            # Convert currency before validation
            cost_usd = convert_to_usd(row['cost'], row['currency'])

            # Pydantic validates business rules
            gold_record = AppleCloudCostGold(
                usage_date=row['usage_date'],
                subscription_id=row['subscription_id'],
                service_name=row['service_name'],
                team=row['team'],
                cost_center=row['cost_center'],
                cost_usd=cost_usd
            )

            gold_records.append({
                'cost_date': gold_record.usage_date,
                'subscription_id': gold_record.subscription_id,
                'service_name': gold_record.service_name,
                'team': gold_record.team,
                'cost_center': gold_record.cost_center,
                'cost_usd': float(gold_record.cost_usd)
            })

        except Exception as e:
            failed_count += 1
            if failed_count <= 5:  # Show first 5 failures
                print(f"  Row {idx} failed gold validation: {e}")

    print("\nTransformation summary:")
    print(f"  Valid: {len(gold_records)}")
    print(f"  Failed: {failed_count}")
    print(f"  Success rate: {len(gold_records) / len(raw_df):.1%}")

    return pd.DataFrame(gold_records)


def aggregate_daily_costs(gold_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate to daily grain by subscription and service.

    Business rule: If multiple raw records exist for the same
    date/subscription/service, sum the costs. This happens when
    the API reports hourly data that we roll up to daily.

    Args:
        gold_df: DataFrame with validated gold records

    Returns:
        DataFrame aggregated to daily grain
    """
    if gold_df.empty:
        print("No records to aggregate")
        return gold_df

    print(f"\nAggregating {len(gold_df)} records to daily grain...")

    aggregated = (
        gold_df
        .groupby(
            ['cost_date', 'subscription_id', 'service_name', 'team', 'cost_center'],
            as_index=False
        )
        .agg({'cost_usd': 'sum'})
    )

    print(f"Aggregated to {len(aggregated)} daily records")

    # Show sample of what we're loading
    if len(aggregated) > 0:
        print("\nSample of aggregated data:")
        print(aggregated.head(3).to_string(index=False))

    return aggregated


def main():
    """Run the transformation pipeline"""
    print("=" * 60)
    print("TRANSFORM: Raw to Gold")
    print("=" * 60)

    try:
        # Step 1: Read raw data
        print("\nStep 1: Reading raw data")
        print("-" * 60)
        raw_df = read_raw_apple_costs()

        if raw_df.empty:
            print("⚠️  No raw data found. Run extract.py first.")
            return

        # Step 2: Transform to gold schema
        print("\nStep 2: Transforming to gold schema")
        print("-" * 60)
        gold_df = transform_to_gold(raw_df)

        if gold_df.empty:
            print("⚠️  No valid records after transformation")
            return

        # Step 3: Aggregate to daily grain
        print("\nStep 3: Aggregating to daily grain")
        print("-" * 60)
        aggregated_df = aggregate_daily_costs(gold_df)

        print("\n" + "=" * 60)
        print(f"✓ TRANSFORMATION COMPLETE: {len(aggregated_df)} records ready")
        print("=" * 60)

        return aggregated_df

    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ TRANSFORMATION FAILED: {e}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    main()
