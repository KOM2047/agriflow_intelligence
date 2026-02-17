<#
.SYNOPSIS
    Simulates a Daily Data Dump for AgriFlow Intelligence.
.DESCRIPTION
    Runs the Python data generators every 60 seconds to simulate a new day's worth of data arriving.
    This acts as the "Source System" in your architecture.
#>

# 1. Configuration
$PythonPath = ".\venv\Scripts\python.exe"
$HarvestScript = "scripts\generators\generate_harvests.py"
$MarketScript = "scripts\generators\mock_market_api.py"
$IntervalSeconds = 60

Write-Host "🚜 AgriFlow Simulation Engine Started..." -ForegroundColor Green
Write-Host "----------------------------------------"

# 2. The Infinite Loop (The "Scheduler")
while ($true) {
    $Timestamp = Get-Date -Format "HH:mm:ss"
    Write-Host "[$Timestamp] ☀️  New Day Started. Generating data..." -ForegroundColor Cyan

    # 3. Run Harvest Generator
    try {
        & $PythonPath $HarvestScript
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ Harvest Logs Generated." -ForegroundColor Gray
        }
        else {
            Write-Host "   ❌ Error generating Harvest Logs." -ForegroundColor Red
        }
    }
    catch {
        Write-Host "   ❌ Failed to execute Python script." -ForegroundColor Red
    }

    # 4. Run Market Data Generator
    try {
        & $PythonPath $MarketScript
        if ($LASTEXITCODE -eq 0) {
            Write-Host "   ✅ Market Prices Fetched." -ForegroundColor Gray
        }
    }
    catch {
        Write-Host "   ❌ Failed to fetch Market Prices." -ForegroundColor Red
    }

    # 5. Wait for next "Day"
    Write-Host "[$Timestamp] 💤 Sleeping for $IntervalSeconds seconds..." -ForegroundColor DarkGray
    Write-Host "----------------------------------------"
    
    Start-Sleep -Seconds $IntervalSeconds
}