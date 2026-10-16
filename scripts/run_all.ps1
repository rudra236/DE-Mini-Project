# ==============================================================================
# BankFlow Sentinel - Master Launch Script (PowerShell)
# Starts Docker infra, verifies health, initializes DB, and launches services
# ==============================================================================

$ErrorActionPreference = "Stop"

Write-Host ""
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host "      BANKFLOW SENTINEL - INITIALIZING SYSTEM STACK     " -ForegroundColor Cyan
Write-Host "========================================================" -ForegroundColor Cyan
Write-Host ""

# 1. Start Docker Containers
Write-Host "Step 1: Starting Docker Compose services (Postgres, Mongo, Kafka)..." -ForegroundColor Yellow
docker compose up -d

# 2. Wait for Container Health
Write-Host ""
Write-Host "Step 2: Waiting for infrastructure services to be healthy..." -ForegroundColor Yellow
$maxRetries = 30
$retryCount = 0
$stackHealthy = $false

while (-not $stackHealthy -and $retryCount -lt $maxRetries) {
    $retryCount++
    Start-Sleep -Seconds 2
    try {
        $checkOutput = python scripts/check_stack.py 2>&1
        if ($LASTEXITCODE -eq 0) {
            $stackHealthy = $true
            Write-Host "   [OK] All infrastructure services (Postgres, Mongo, Kafka) are healthy!" -ForegroundColor Green
        } else {
            Write-Host "   Waiting for services... (Attempt $retryCount/$maxRetries)" -ForegroundColor Gray
        }
    } catch {
        Write-Host "   Waiting for services... (Attempt $retryCount/$maxRetries)" -ForegroundColor Gray
    }
}

if (-not $stackHealthy) {
    Write-Host ""
    Write-Host "[ERROR] Infrastructure stack failed to become healthy within timeout." -ForegroundColor Red
    exit 1
}

# 3. Initialize Schemas and Indexes
Write-Host ""
Write-Host "Step 3: Initializing database schemas and NoSQL collections..." -ForegroundColor Yellow
python db/init_db.py

# 4. Launch Services via decoupled Python process manager
Write-Host ""
Write-Host "Step 4: Launching background microservices..." -ForegroundColor Yellow
python scripts/launch_services.py

Start-Sleep -Seconds 3

# 5. Print Active Service URLs
Write-Host ""
Write-Host "========================================================" -ForegroundColor Green
Write-Host "      [OK] BANKFLOW SENTINEL STACK IS LIVE AND ACTIVE   " -ForegroundColor Green
Write-Host "========================================================" -ForegroundColor Green
Write-Host " Operations Console : http://localhost:8501" -ForegroundColor Cyan
Write-Host " FastAPI Gateway    : http://127.0.0.1:8000" -ForegroundColor Cyan
Write-Host " API Documentation  : http://127.0.0.1:8000/docs" -ForegroundColor Cyan
Write-Host " API Health Probe   : http://127.0.0.1:8000/health" -ForegroundColor Cyan
Write-Host "--------------------------------------------------------" -ForegroundColor Gray
Write-Host " Telemetry logs are streaming to logs/ directory." -ForegroundColor Gray
Write-Host " To cleanly terminate all processes: .\scripts\stop_all.ps1" -ForegroundColor Yellow
Write-Host ""
