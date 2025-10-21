# Load environment variables from .env
if (Test-Path .env) {
    Get-Content .env | ForEach-Object {
        if ($_ -match "^\s*([^#][^=]+)=(.*)$") {
            $name = $matches[1].Trim()
            $value = $matches[2].Trim()
            [System.Environment]::SetEnvironmentVariable($name, $value)
        }
    }
}

# Change to dbt project directory, run dbt, and then return to original directory
$originalLocation = Get-Location
try {
    Set-Location "dbt_project"
    Write-Host "Running dbt in $(Get-Location)" -ForegroundColor Cyan
    dbt run --profiles-dir . --select "path:models/dev"
}
finally {
    Set-Location $originalLocation
    Write-Host "Returned to original directory: $(Get-Location)" -ForegroundColor Green
}
