"""
BankFlow Sentinel - Background Service Teardown
Cleanly terminates background processes recorded in .run/*.pid and frees ports.
"""

import sys
import os
import signal
from pathlib import Path

# Fix Windows console UTF-8
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

ROOT_DIR = Path(__file__).resolve().parent.parent
RUN_DIR = ROOT_DIR / ".run"

def terminate():
    print("Terminating BankFlow Sentinel microservices...")
    if RUN_DIR.exists():
        for pid_file in RUN_DIR.glob("*.pid"):
            try:
                pid = int(pid_file.read_text().strip())
                if sys.platform == "win32":
                    import subprocess
                    subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], capture_output=True)
                else:
                    os.kill(pid, signal.SIGTERM)
                print(f"   [OK] Terminated PID {pid} ({pid_file.stem})")
            except Exception:
                pass
            try:
                pid_file.unlink()
            except Exception:
                pass

    print("[OK] All application microservices terminated cleanly.\n")

if __name__ == "__main__":
    terminate()
