#!/usr/bin/env pwsh
# Launch Taipy Application

Write-Host "🚀 Starting Taipy Census & Education Data Platform..." -ForegroundColor Cyan
Write-Host ""

# Check if .env file exists
if (-not (Test-Path ".env")) {
    Write-Host "⚠️  Warning: .env file not found!" -ForegroundColor Yellow
    Write-Host "   Make sure to create a .env file with your database credentials." -ForegroundColor Yellow
    Write-Host ""
}

# Run the Taipy application
python app/taipy_app.py
