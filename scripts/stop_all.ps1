# ==============================================================================
# BankFlow Sentinel - Master Teardown Script (PowerShell)
# Terminates background Python microservices and cleans up process locks
# ==============================================================================

Write-Host ""
Write-Host "Stopping BankFlow Sentinel services..." -ForegroundColor Yellow

python scripts/terminate_services.py

# Safety check for listeners on 8000 and 8501
$ports = @(8000, 8501)
foreach ($port in $ports) {
    $conns = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
    if ($conns) {
        foreach ($conn in $conns) {
            try {
                Stop-Process -Id $conn.OwningProcess -Force -ErrorAction SilentlyContinue
                Write-Host "   [OK] Freed port $port (terminated PID $($conn.OwningProcess))" -ForegroundColor Green
            } catch {}
        }
    }
}

Write-Host ""
Write-Host "[OK] All application microservices stopped cleanly." -ForegroundColor Green
Write-Host "Note: Docker containers remain running. To stop them: docker compose down" -ForegroundColor Gray
Write-Host ""
