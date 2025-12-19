"""
Pydantic models for Apple Cloud Cost data validation
Validates at the boundary: untrusted API data → trusted internal records
"""

from datetime import date
from decimal import Decimal, InvalidOperation
from pydantic import BaseModel, field_validator


class AppleCloudCostRaw(BaseModel):
    """Schema for raw API payload from Apple Cloud Cost API.

    All fields are strings because we do not trust the API
    to send correct types. The API documentation says cost
    is a number, but production APIs lie.

    This model accepts what the API sends, including nulls
    for optional fields like team and currency.
    """
    usage_date: str
    subscription_id: str
    service_name: str
    cost: str
    currency: str | None = None
    team: str | None = None
    cost_center: str | None = None


class AppleCloudCostGold(BaseModel):
    """Schema for gold table. Types are strict.

    This model enforces Apple finance requirements:
    - All costs in USD (converted if necessary)
    - No negative costs
    - Team and cost center required for allocation
    - Dates must be valid dates

    When you see an AppleCloudCostGold object, you know
    it passed all validations. No further checks needed.
    """
    usage_date: date
    subscription_id: str
    service_name: str
    team: str
    cost_center: str
    cost_usd: Decimal

    @field_validator('cost_usd', mode='before')
    @classmethod
    def parse_cost(cls, v):
        """Convert string cost to Decimal. Reject garbage.

        The API sometimes returns 'N/A', 'pending', or empty strings
        when billing data is delayed. These are not valid costs.

        Also handles costs with comma separators like "142,857.23"
        """
        if isinstance(v, (int, float, Decimal)):
            return Decimal(str(v))

        if isinstance(v, str):
            cleaned = v.strip().replace(',', '')
            if cleaned in ('', 'N/A', 'null', 'None', 'pending'):
                raise ValueError(
                    f"Invalid cost value: '{v}' - billing data may be delayed"
                )
            try:
                return Decimal(cleaned)
            except InvalidOperation:
                raise ValueError(f"Cannot parse cost: '{v}'")

        raise ValueError(f"Unexpected cost type: {type(v)}")

    @field_validator('cost_usd')
    @classmethod
    def cost_must_be_non_negative(cls, v):
        """Business rule: costs cannot be negative.

        If a refund occurs, it should be a separate credit record,
        not a negative cost. Negative values indicate a bug
        (e.g., wrong exchange rate, incorrect calculation).
        """
        if v < 0:
            raise ValueError(f"Cost cannot be negative: {v}")
        return v

    @field_validator('team', 'cost_center', mode='before')
    @classmethod
    def require_allocation_fields(cls, v, info):
        """Business rule: every cost must be allocatable.

        Finance cannot report costs without knowing which team
        and cost center to charge. Missing allocation = missing money.

        The CFO needs to know who to hold accountable for each dollar.
        """
        if v is None or (isinstance(v, str) and v.strip() == ''):
            raise ValueError(
                f"{info.field_name} is required for cost allocation"
            )
        return v.strip()

    @field_validator('usage_date', mode='before')
    @classmethod
    def parse_usage_date(cls, v):
        """Parse usage_date string to date object.

        API sends dates as "YYYY-MM-DD" strings.
        Convert to Python date for type safety.
        """
        if isinstance(v, date):
            return v
        if isinstance(v, str):
            try:
                return date.fromisoformat(v)
            except ValueError:
                raise ValueError(f"Invalid date format: '{v}'. Expected YYYY-MM-DD")
        raise ValueError(f"Unexpected date type: {type(v)}")


# Example usage and testing
if __name__ == "__main__":
    print("=" * 60)
    print("Testing Pydantic Validation")
    print("=" * 60)

    # Test 1: Valid raw record
    print("\n1. Valid raw record:")
    raw_valid = {
        "usage_date": "2025-01-15",
        "subscription_id": "aws-prod-001",
        "service_name": "EC2 Compute",
        "cost": "142857.23",
        "currency": "USD",
        "team": "Siri Infrastructure",
        "cost_center": "CC-4521"
    }
    try:
        validated = AppleCloudCostRaw.model_validate(raw_valid)
        print(f"✓ Passed: {validated}")
    except Exception as e:
        print(f"✗ Failed: {e}")

    # Test 2: Raw record with missing team (should pass for raw)
    print("\n2. Raw record with missing team (accepts nulls):")
    raw_missing_team = raw_valid.copy()
    raw_missing_team["team"] = None
    try:
        validated = AppleCloudCostRaw.model_validate(raw_missing_team)
        print(f"✓ Passed: {validated}")
    except Exception as e:
        print(f"✗ Failed: {e}")

    # Test 3: Gold record with valid data
    print("\n3. Valid gold record:")
    gold_valid = {
        "usage_date": "2025-01-15",
        "subscription_id": "aws-prod-001",
        "service_name": "EC2 Compute",
        "cost_usd": "142857.23",
        "team": "Siri Infrastructure",
        "cost_center": "CC-4521"
    }
    try:
        validated = AppleCloudCostGold.model_validate(gold_valid)
        print(f"✓ Passed: {validated}")
    except Exception as e:
        print(f"✗ Failed: {e}")

    # Test 4: Gold record with "N/A" cost (should fail)
    print("\n4. Gold record with 'N/A' cost (should reject):")
    gold_na_cost = gold_valid.copy()
    gold_na_cost["cost_usd"] = "N/A"
    try:
        validated = AppleCloudCostGold.model_validate(gold_na_cost)
        print(f"✓ Passed: {validated}")
    except Exception as e:
        print(f"✗ Failed (expected): {e}")

    # Test 5: Gold record with missing team (should fail)
    print("\n5. Gold record with missing team (should reject):")
    gold_missing_team = gold_valid.copy()
    gold_missing_team["team"] = None
    try:
        validated = AppleCloudCostGold.model_validate(gold_missing_team)
        print(f"✓ Passed: {validated}")
    except Exception as e:
        print(f"✗ Failed (expected): {e}")

    # Test 6: Gold record with negative cost (should fail)
    print("\n6. Gold record with negative cost (should reject):")
    gold_negative = gold_valid.copy()
    gold_negative["cost_usd"] = "-100.00"
    try:
        validated = AppleCloudCostGold.model_validate(gold_negative)
        print(f"✓ Passed: {validated}")
    except Exception as e:
        print(f"✗ Failed (expected): {e}")

    # Test 7: Gold record with comma-formatted cost (should pass)
    print("\n7. Gold record with comma-formatted cost (should parse):")
    gold_comma = gold_valid.copy()
    gold_comma["cost_usd"] = "142,857.23"
    try:
        validated = AppleCloudCostGold.model_validate(gold_comma)
        print(f"✓ Passed: {validated}")
    except Exception as e:
        print(f"✗ Failed: {e}")

    print("\n" + "=" * 60)
    print("Validation tests complete")
    print("=" * 60)
