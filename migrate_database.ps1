# Database Migration Script for Windows (PowerShell)
# Migrates data from local PostgreSQL to containerized PostgreSQL

param(
    [string]$SourceHost = "localhost",
    [string]$SourcePort = "5432",
    [string]$SourceDB = "milestone2",
    [string]$SourceUser = "postgres",
    [string]$SourcePassword = "postgres",
    [string]$TargetContainer = "milestone2-db",
    [string]$TargetDB = "milestone2",
    [string]$TargetUser = "postgres",
    [string]$TargetPassword = "postgres"
)

Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Milestone 2 - Database Migration Script" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Host ""

# Configuration
$BackupDir = ".\db_backup"
$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$DumpFile = "$BackupDir\milestone2_dump_$Timestamp.sql"

# Create backup directory
if (-not (Test-Path $BackupDir)) {
    New-Item -ItemType Directory -Path $BackupDir | Out-Null
}

# Functions
function Write-Success {
    param([string]$Message)
    Write-Host "✓ $Message" -ForegroundColor Green
}

function Write-Error-Custom {
    param([string]$Message)
    Write-Host "✗ $Message" -ForegroundColor Red
}

function Write-Info {
    param([string]$Message)
    Write-Host "ℹ $Message" -ForegroundColor Yellow
}

# Step 1: Check prerequisites
Write-Host "Step 1: Checking prerequisites..." -ForegroundColor Cyan
Write-Host "--------------------------------" -ForegroundColor Cyan

# Check if docker is running
try {
    docker ps | Out-Null
    Write-Success "Docker is running"
} catch {
    Write-Error-Custom "Docker is not running. Please start Docker first."
    exit 1
}

# Check if target container exists
$containerExists = docker ps --format "{{.Names}}" | Select-String -Pattern "^$TargetContainer$"
if (-not $containerExists) {
    Write-Error-Custom "Target container '$TargetContainer' is not running."
    Write-Host "Please start it with: docker-compose -f docker-compose.prod.yml up -d db"
    exit 1
}
Write-Success "Target container '$TargetContainer' is running"

# Check if pg_dump is available
$pgDumpPath = Get-Command pg_dump -ErrorAction SilentlyContinue
if (-not $pgDumpPath) {
    Write-Error-Custom "pg_dump not found. Please install PostgreSQL client tools."
    Write-Host "Download from: https://www.postgresql.org/download/windows/"
    exit 1
}
Write-Success "pg_dump is available"

Write-Host ""
Write-Host "Step 2: Backing up source database..." -ForegroundColor Cyan
Write-Host "--------------------------------------" -ForegroundColor Cyan
Write-Info "Source: $SourceUser@$SourceHost:$SourcePort/$SourceDB"
Write-Info "Backup file: $DumpFile"

# Set password environment variable
$env:PGPASSWORD = $SourcePassword

# Export source database
try {
    pg_dump -h $SourceHost `
            -p $SourcePort `
            -U $SourceUser `
            -d $SourceDB `
            --no-owner `
            --no-acl `
            -F p `
            -f $DumpFile

    Write-Success "Database backup created successfully"

    # Get file size
    $FileSize = (Get-Item $DumpFile).Length / 1MB
    Write-Info "Backup size: $([math]::Round($FileSize, 2)) MB"
} catch {
    Write-Error-Custom "Failed to backup source database: $_"
    exit 1
}

Write-Host ""
Write-Host "Step 3: Preparing target database..." -ForegroundColor Cyan
Write-Host "-------------------------------------" -ForegroundColor Cyan

# Wait for container to be ready
Write-Info "Waiting for database to be ready..."
$ready = $false
for ($i = 1; $i -le 30; $i++) {
    try {
        docker exec $TargetContainer pg_isready -U $TargetUser | Out-Null
        Write-Success "Target database is ready"
        $ready = $true
        break
    } catch {
        Start-Sleep -Seconds 1
    }
}

if (-not $ready) {
    Write-Error-Custom "Target database not ready after 30 seconds"
    exit 1
}

# Check if target database exists
$dbExists = docker exec $TargetContainer psql -U $TargetUser -lqt | Select-String -Pattern "\b$TargetDB\b"

if (-not $dbExists) {
    Write-Info "Creating database '$TargetDB'..."
    docker exec $TargetContainer psql -U $TargetUser -c "CREATE DATABASE $TargetDB;"
    Write-Success "Database created"
} else {
    Write-Info "Database '$TargetDB' already exists"

    # Ask user if they want to drop and recreate
    $response = Read-Host "Do you want to drop and recreate the database? (yes/no)"
    if ($response -eq "yes" -or $response -eq "y") {
        Write-Info "Dropping existing database..."
        docker exec $TargetContainer psql -U $TargetUser -c "DROP DATABASE IF EXISTS $TargetDB;"
        docker exec $TargetContainer psql -U $TargetUser -c "CREATE DATABASE $TargetDB;"
        Write-Success "Database recreated"
    } else {
        Write-Info "Keeping existing database (this may cause conflicts)"
    }
}

# Enable PostGIS extension
Write-Info "Enabling PostGIS extension..."
docker exec $TargetContainer psql -U $TargetUser -d $TargetDB -c "CREATE EXTENSION IF NOT EXISTS postgis;"
Write-Success "PostGIS extension enabled"

Write-Host ""
Write-Host "Step 4: Restoring database to container..." -ForegroundColor Cyan
Write-Host "-------------------------------------------" -ForegroundColor Cyan

# Copy dump file to container
Write-Info "Copying dump file to container..."
docker cp $DumpFile "$TargetContainer:/tmp/dump.sql"
Write-Success "Dump file copied"

# Restore database
Write-Info "Restoring database (this may take several minutes)..."
try {
    docker exec $TargetContainer psql -U $TargetUser -d $TargetDB -f /tmp/dump.sql 2>&1 | Out-Null
    Write-Success "Database restored successfully"
} catch {
    Write-Error-Custom "Database restoration completed with warnings"
    Write-Info "This is often normal. Check for any critical errors above."
}

# Clean up temp file in container
docker exec $TargetContainer rm /tmp/dump.sql

Write-Host ""
Write-Host "Step 5: Verifying migration..." -ForegroundColor Cyan
Write-Host "-------------------------------" -ForegroundColor Cyan

# Get table count from source
$env:PGPASSWORD = $SourcePassword
$sourceTableCount = psql -h $SourceHost -p $SourcePort -U $SourceUser -d $SourceDB -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema');"

# Get table count from target
$targetTableCount = docker exec $TargetContainer psql -U $TargetUser -d $TargetDB -t -c "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema');"

Write-Info "Source database tables: $($sourceTableCount.Trim())"
Write-Info "Target database tables: $($targetTableCount.Trim())"

if ($sourceTableCount.Trim() -eq $targetTableCount.Trim()) {
    Write-Success "Table count matches!"
} else {
    Write-Error-Custom "Table count mismatch. Please review migration."
}

# List schemas in target
Write-Info "Schemas in target database:"
docker exec $TargetContainer psql -U $TargetUser -d $TargetDB -c "\dn"

Write-Host ""
Write-Host "================================================" -ForegroundColor Cyan
Write-Host "  Migration Summary" -ForegroundColor Cyan
Write-Host "================================================" -ForegroundColor Cyan
Write-Success "Migration completed!"
Write-Host ""
Write-Host "Backup file saved to:" -ForegroundColor Yellow
Write-Host "  $DumpFile" -ForegroundColor White
Write-Host ""
Write-Host "Next steps:" -ForegroundColor Yellow
Write-Host "  1. Verify data in container:" -ForegroundColor White
Write-Host "     docker exec -it $TargetContainer psql -U $TargetUser -d $TargetDB" -ForegroundColor Gray
Write-Host ""
Write-Host "  2. Update your .env file if needed" -ForegroundColor White
Write-Host ""
Write-Host "  3. Test your application:" -ForegroundColor White
Write-Host "     docker-compose -f docker-compose.prod.yml up" -ForegroundColor Gray
Write-Host ""
Write-Host "  4. Once verified, you can safely remove the backup file" -ForegroundColor White
Write-Host "================================================" -ForegroundColor Cyan

# Clear password from environment
$env:PGPASSWORD = ""
