#!/bin/bash
# Quick database setup script for Apple Cloud Cost ETL Pipeline

set -e

echo "============================================================"
echo "Apple Cloud Cost Pipeline - Database Setup"
echo "============================================================"

# Check if PostgreSQL is installed
if ! command -v psql &> /dev/null; then
    echo "❌ PostgreSQL not found. Please install PostgreSQL first."
    echo ""
    echo "macOS:   brew install postgresql"
    echo "Ubuntu:  sudo apt install postgresql postgresql-contrib"
    echo "Fedora:  sudo dnf install postgresql postgresql-server"
    exit 1
fi

echo "✓ PostgreSQL found"

# Database configuration
DB_NAME="apple_analytics"

# Auto-detect PostgreSQL user (macOS uses current user, Linux uses postgres)
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS: use current user by default
    DB_USER="${POSTGRES_USER:-$USER}"
else
    # Linux: use postgres by default
    DB_USER="${POSTGRES_USER:-postgres}"
fi

echo ""
echo "Database configuration:"
echo "  Name: $DB_NAME"
echo "  User: $DB_USER"
echo ""

# Check if database exists
if psql -U "$DB_USER" -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "⚠️  Database '$DB_NAME' already exists."
    read -p "Drop and recreate? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Dropping database..."
        psql -U "$DB_USER" -c "DROP DATABASE $DB_NAME;"
        echo "Creating database..."
        psql -U "$DB_USER" -c "CREATE DATABASE $DB_NAME;"
    fi
else
    echo "Creating database..."
    psql -U "$DB_USER" -c "CREATE DATABASE $DB_NAME;"
fi

echo "✓ Database ready"

# Run schema
echo ""
echo "Creating tables..."
# Get the script directory and find schema.sql in Data_Layer
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SCHEMA_FILE="$SCRIPT_DIR/../Data_Layer/schema.sql"

if [ ! -f "$SCHEMA_FILE" ]; then
    echo "❌ Schema file not found at: $SCHEMA_FILE"
    exit 1
fi

psql -U "$DB_USER" -d "$DB_NAME" -f "$SCHEMA_FILE"

echo "✓ Tables created"

# Verify tables
echo ""
echo "Verifying tables..."
TABLE_COUNT=$(psql -U "$DB_USER" -d "$DB_NAME" -tAc "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public';")

if [ "$TABLE_COUNT" -ge 2 ]; then
    echo "✓ Found $TABLE_COUNT tables:"
    psql -U "$DB_USER" -d "$DB_NAME" -c "\dt"
else
    echo "❌ Tables not found. Check schema.sql for errors."
    exit 1
fi

# Set up .env if it doesn't exist
PROJECT_ROOT="$SCRIPT_DIR/.."
ENV_FILE="$PROJECT_ROOT/.env"
ENV_EXAMPLE="$SCRIPT_DIR/.env.example"

if [ ! -f "$ENV_FILE" ]; then
    echo ""
    echo "Creating .env file..."
    cp "$ENV_EXAMPLE" "$ENV_FILE"

    # Update DATABASE_URL in .env
    if [[ "$OSTYPE" == "darwin"* ]]; then
        # macOS
        sed -i '' "s|DATABASE_URL=.*|DATABASE_URL=postgresql://$DB_USER@localhost:5432/$DB_NAME|" "$ENV_FILE"
    else
        # Linux
        sed -i "s|DATABASE_URL=.*|DATABASE_URL=postgresql://$DB_USER@localhost:5432/$DB_NAME|" "$ENV_FILE"
    fi

    echo "✓ .env file created at project root"
    echo ""
    echo "⚠️  Update .env with your database password if needed"
fi

echo ""
echo "============================================================"
echo "✓ DATABASE SETUP COMPLETE"
echo "============================================================"
echo ""
echo "Next steps:"
echo "  1. Start mock API:    cd Infrastructure && python mock_api_server.py"
echo "  2. Run pipeline:      python -m Orchestration.main"
echo "  3. Test validation:   python -m Data_Layer.contracts"
echo ""
echo "Connection string: postgresql://$DB_USER@localhost:5432/$DB_NAME"
echo "============================================================"
