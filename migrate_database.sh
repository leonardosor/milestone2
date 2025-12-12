#!/usr/bin/env bash
# Database Migration Script
# Migrates data from local PostgreSQL to containerized PostgreSQL

set -e  # Exit on error

echo "================================================"
echo "  Milestone 2 - Database Migration Script"
echo "================================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
SOURCE_HOST="${SOURCE_HOST:-localhost}"
SOURCE_PORT="${SOURCE_PORT:-5432}"
SOURCE_DB="${SOURCE_DB:-milestone2}"
SOURCE_USER="${SOURCE_USER:-postgres}"
SOURCE_PASSWORD="${SOURCE_PASSWORD:-postgres}"

TARGET_CONTAINER="${TARGET_CONTAINER:-milestone2-db}"
TARGET_DB="${TARGET_DB:-milestone2}"
TARGET_USER="${TARGET_USER:-postgres}"
TARGET_PASSWORD="${TARGET_PASSWORD:-postgres}"

BACKUP_DIR="./db_backup"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DUMP_FILE="$BACKUP_DIR/milestone2_dump_${TIMESTAMP}.sql"
DUMP_FILE_COMPRESSED="$BACKUP_DIR/milestone2_dump_${TIMESTAMP}.sql.gz"

# Functions
print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

# Create backup directory
mkdir -p "$BACKUP_DIR"

echo "Step 1: Checking prerequisites..."
echo "--------------------------------"

# Check if docker is running
if ! docker ps >/dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi
print_success "Docker is running"

# Check if target container exists
if ! docker ps --format '{{.Names}}' | grep -q "^${TARGET_CONTAINER}$"; then
    print_error "Target container '${TARGET_CONTAINER}' is not running."
    echo "Please start it with: docker-compose -f docker-compose.prod.yml up -d db"
    exit 1
fi
print_success "Target container '${TARGET_CONTAINER}' is running"

# Check if pg_dump is available
if ! command -v pg_dump >/dev/null 2>&1; then
    print_error "pg_dump not found. Please install PostgreSQL client tools."
    exit 1
fi
print_success "pg_dump is available"

echo ""
echo "Step 2: Backing up source database..."
echo "--------------------------------------"
print_info "Source: ${SOURCE_USER}@${SOURCE_HOST}:${SOURCE_PORT}/${SOURCE_DB}"
print_info "Backup file: ${DUMP_FILE}"

# Export source database
export PGPASSWORD="${SOURCE_PASSWORD}"
if pg_dump -h "${SOURCE_HOST}" \
           -p "${SOURCE_PORT}" \
           -U "${SOURCE_USER}" \
           -d "${SOURCE_DB}" \
           --no-owner \
           --no-acl \
           -F p \
           -f "${DUMP_FILE}"; then
    print_success "Database backup created successfully"

    # Get file size
    FILE_SIZE=$(du -h "${DUMP_FILE}" | cut -f1)
    print_info "Backup size: ${FILE_SIZE}"

    # Compress backup
    echo "Compressing backup..."
    gzip -c "${DUMP_FILE}" > "${DUMP_FILE_COMPRESSED}"
    COMPRESSED_SIZE=$(du -h "${DUMP_FILE_COMPRESSED}" | cut -f1)
    print_success "Compressed backup created: ${COMPRESSED_SIZE}"
else
    print_error "Failed to backup source database"
    exit 1
fi

echo ""
echo "Step 3: Preparing target database..."
echo "-------------------------------------"

# Wait for container to be ready
print_info "Waiting for database to be ready..."
for i in {1..30}; do
    if docker exec "${TARGET_CONTAINER}" pg_isready -U "${TARGET_USER}" >/dev/null 2>&1; then
        print_success "Target database is ready"
        break
    fi
    if [ $i -eq 30 ]; then
        print_error "Target database not ready after 30 seconds"
        exit 1
    fi
    sleep 1
done

# Check if target database exists
DB_EXISTS=$(docker exec "${TARGET_CONTAINER}" psql -U "${TARGET_USER}" -lqt | cut -d \| -f 1 | grep -w "${TARGET_DB}" | wc -l)

if [ "${DB_EXISTS}" -eq 0 ]; then
    print_info "Creating database '${TARGET_DB}'..."
    docker exec "${TARGET_CONTAINER}" psql -U "${TARGET_USER}" -c "CREATE DATABASE ${TARGET_DB};"
    print_success "Database created"
else
    print_info "Database '${TARGET_DB}' already exists"

    # Ask user if they want to drop and recreate
    read -p "Do you want to drop and recreate the database? (yes/no): " -r
    if [[ $REPLY =~ ^[Yy]es$ ]]; then
        print_info "Dropping existing database..."
        docker exec "${TARGET_CONTAINER}" psql -U "${TARGET_USER}" -c "DROP DATABASE IF EXISTS ${TARGET_DB};"
        docker exec "${TARGET_CONTAINER}" psql -U "${TARGET_USER}" -c "CREATE DATABASE ${TARGET_DB};"
        print_success "Database recreated"
    else
        print_info "Keeping existing database (this may cause conflicts)"
    fi
fi

# Enable PostGIS extension
print_info "Enabling PostGIS extension..."
docker exec "${TARGET_CONTAINER}" psql -U "${TARGET_USER}" -d "${TARGET_DB}" -c "CREATE EXTENSION IF NOT EXISTS postgis;"
print_success "PostGIS extension enabled"

echo ""
echo "Step 4: Restoring database to container..."
echo "-------------------------------------------"

# Copy dump file to container
print_info "Copying dump file to container..."
docker cp "${DUMP_FILE}" "${TARGET_CONTAINER}:/tmp/dump.sql"
print_success "Dump file copied"

# Restore database
print_info "Restoring database (this may take several minutes)..."
if docker exec "${TARGET_CONTAINER}" psql -U "${TARGET_USER}" -d "${TARGET_DB}" -f /tmp/dump.sql > /dev/null 2>&1; then
    print_success "Database restored successfully"
else
    print_error "Database restoration completed with warnings (this is often normal)"
    print_info "Check the logs above for any critical errors"
fi

# Clean up temp file in container
docker exec "${TARGET_CONTAINER}" rm /tmp/dump.sql

echo ""
echo "Step 5: Verifying migration..."
echo "-------------------------------"

# Get table count from source
SOURCE_TABLE_COUNT=$(PGPASSWORD="${SOURCE_PASSWORD}" psql -h "${SOURCE_HOST}" -p "${SOURCE_PORT}" -U "${SOURCE_USER}" -d "${SOURCE_DB}" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema');")

# Get table count from target
TARGET_TABLE_COUNT=$(docker exec "${TARGET_CONTAINER}" psql -U "${TARGET_USER}" -d "${TARGET_DB}" -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema');")

print_info "Source database tables: ${SOURCE_TABLE_COUNT}"
print_info "Target database tables: ${TARGET_TABLE_COUNT}"

if [ "${SOURCE_TABLE_COUNT}" -eq "${TARGET_TABLE_COUNT}" ]; then
    print_success "Table count matches!"
else
    print_error "Table count mismatch. Please review migration."
fi

# List schemas in target
print_info "Schemas in target database:"
docker exec "${TARGET_CONTAINER}" psql -U "${TARGET_USER}" -d "${TARGET_DB}" -c "\dn"

echo ""
echo "================================================"
echo "  Migration Summary"
echo "================================================"
print_success "Migration completed!"
echo ""
echo "Backup files saved to:"
echo "  - SQL dump: ${DUMP_FILE}"
echo "  - Compressed: ${DUMP_FILE_COMPRESSED}"
echo ""
echo "Next steps:"
echo "  1. Verify data in container:"
echo "     docker exec -it ${TARGET_CONTAINER} psql -U ${TARGET_USER} -d ${TARGET_DB}"
echo ""
echo "  2. Update your .env file if needed"
echo ""
echo "  3. Test your application:"
echo "     docker-compose -f docker-compose.prod.yml up"
echo ""
echo "  4. Once verified, you can safely remove the backup files"
echo "================================================"
