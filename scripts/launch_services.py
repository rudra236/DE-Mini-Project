"""
BankFlow Sentinel - Background Service Launcher
Spawns FastAPI, Kafka Consumer, and Streamlit as fully decoupled daemon processes.
"""

import sys
import os
import subprocess
import time
from pathlib import Path

# Fix Windows console UTF-8
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

ROOT_DIR = Path(__file__).resolve().parent.parent
LOGS_DIR = ROOT_DIR / "logs"
RUN_DIR = ROOT_DIR / ".run"

LOGS_DIR.mkdir(parents=True, exist_ok=True)
RUN_DIR.mkdir(parents=True, exist_ok=True)

SERVICES = [
    {
        "name": "uvicorn",
        "cmd": [sys.executable, "-m", "uvicorn", "api.main:app", "--host", "127.0.0.1", "--port", "8000"],
        "stdout": LOGS_DIR / "uvicorn.log",
        "stderr": LOGS_DIR / "uvicorn_err.log",
        "pid_file": RUN_DIR / "uvicorn.pid"
    },
    {
        "name": "consumer",
        "cmd": [sys.executable, "-m", "streaming.consumer"],
        "stdout": LOGS_DIR / "streaming.log",
        "stderr": LOGS_DIR / "streaming_err.log",
        "pid_file": RUN_DIR / "consumer.pid"
    },
    {
        "name": "streamlit",
        "cmd": [sys.executable, "-m", "streamlit", "run", "dashboard/app.py", "--server.headless", "true", "--server.port", "8501"],
        "stdout": LOGS_DIR / "streamlit.log",
        "stderr": LOGS_DIR / "streamlit_err.log",
        "pid_file": RUN_DIR / "streamlit.pid"
    }
]

def launch():
    print("Launching BankFlow Sentinel decoupled microservices...")
    
    creation_flags = 0
    if sys.platform == "win32":
        creation_flags = (
            subprocess.CREATE_NEW_PROCESS_GROUP
            | subprocess.DETACHED_PROCESS
            | 0x08000000  # CREATE_NO_WINDOW
        )

    for svc in SERVICES:
        out_f = open(svc["stdout"], "a", encoding="utf-8")
        err_f = open(svc["stderr"], "a", encoding="utf-8")

        proc = subprocess.Popen(
            svc["cmd"],
            cwd=str(ROOT_DIR),
            stdout=out_f,
            stderr=err_f,
            creationflags=creation_flags,
            close_fds=True
        )

        with open(svc["pid_file"], "w", encoding="ascii") as f:
            f.write(str(proc.pid))

        print(f"   [OK] Started {svc['name']} (PID: {proc.pid})")

    time.sleep(2)
    print("All services spawned successfully.\n")

if __name__ == "__main__":
    launch()
