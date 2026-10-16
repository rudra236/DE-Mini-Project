#!/usr/bin/env bash
# ==============================================================================
# BankFlow Sentinel - Master Launch Script (Bash / POSIX)
# Starts Docker infra, verifies health, initializes DB, and launches services
# ==============================================================================

set -e

echo ""
echo "========================================================"
echo "      BANKFLOW SENTINEL - INITIALIZING SYSTEM STACK     "
echo "========================================================"
echo ""

# 1. Start Docker Containers
echo "Step 1: Starting Docker Compose services (Postgres, Mongo, Kafka)..."
docker compose up -d

# 2. Wait for Container Health
echo "Step 2: Waiting for infrastructure services to be healthy..."
max_retries=30
retry_count=0
stack_healthy=false

while [ "$stack_healthy" = false ] && [ $retry_count -lt $max_retries ]; do
    retry_count=$((retry_count + 1))
    sleep 2
    if python scripts/check_stack.py > /dev/null 2>&1; then
        stack_healthy=true
        echo "   ✓ All infrastructure services (Postgres, Mongo, Kafka) are healthy!"
    else
        echo "   Waiting for services... (Attempt $retry_count/$max_retries)"
    fi
done

if [ "$stack_healthy" = false ]; then
    echo "❌ Error: Infrastructure stack failed to become healthy within timeout."
    exit 1
fi

# 3. Initialize Schemas & Indexes
echo ""
echo "Step 3: Initializing database schemas & NoSQL collections..."
python db/init_db.py

# 4. Prepare logs and run directory
mkdir -p logs
mkdir -p .run

# 5. Launch Services in Separate Background Processes
echo ""
echo "Step 4: Launching background microservices..."

# A. FastAPI Gateway (Port 8000)
echo "   - Starting FastAPI Gateway on http://127.0.0.1:8000..."
nohup python -m uvicorn api.main:app --host 127.0.0.1 --port 8000 > logs/uvicorn.log 2>&1 &
echo $! > .run/uvicorn.pid

# B. Kafka Streaming Consumer
echo "   - Starting Kafka Real-Time Ingestion Consumer..."
nohup python -m streaming.consumer > logs/streaming.log 2>&1 &
echo $! > .run/consumer.pid

# C. Streamlit Operations Console (Port 8501)
echo "   - Starting Streamlit Operations Console on http://localhost:8501..."
nohup python -m streamlit run dashboard/app.py --server.headless true --server.port 8501 > logs/streamlit.log 2>&1 &
echo $! > .run/streamlit.pid

sleep 3

# 6. Print Active Service URLs
echo ""
echo "========================================================"
echo "      ✓ BANKFLOW SENTINEL STACK IS LIVE AND ACTIVE      "
echo "========================================================"
echo " 🏦 Operations Console : http://localhost:8501"
echo " ⚡ FastAPI Gateway    : http://127.0.0.1:8000"
echo " 📖 API Documentation  : http://127.0.0.1:8000/docs"
echo " 🔍 API Health Probe   : http://127.0.0.1:8000/health"
echo "--------------------------------------------------------"
echo " Telemetry logs are streaming to logs/ directory."
echo " To cleanly terminate all processes: ./scripts/stop_all.sh"
echo ""
