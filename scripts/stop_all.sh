#!/usr/bin/env bash
# ==============================================================================
# BankFlow Sentinel - Master Teardown Script (Bash / POSIX)
# Terminates background Python microservices and cleans up process locks
# ==============================================================================

echo ""
echo "Stopping BankFlow Sentinel services..."

if [ -d ".run" ]; then
    for pidfile in .run/*.pid; do
        if [ -f "$pidfile" ]; then
            pid=$(cat "$pidfile")
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null || true
                echo "   ✓ Stopped process with PID $pid ($(basename "$pidfile" .pid))"
            fi
            rm -f "$pidfile"
        fi
    done
fi

# Fallback cleanup by command line match
pkill -f "uvicorn api.main:app" 2>/dev/null || true
pkill -f "streaming.consumer" 2>/dev/null || true
pkill -f "streamlit run dashboard/app.py" 2>/dev/null || true

echo ""
echo "✓ All application microservices stopped cleanly."
echo "Note: Docker containers remain running. To stop them: docker compose down"
echo ""
