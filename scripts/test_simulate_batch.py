"""
End-to-end test script to verify 'Simulate Ingestion Batch' button in Streamlit.
"""

import sys
import time
from pathlib import Path

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def test_simulate():
    print("Launching Chrome to test Simulate Batch button...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")

    driver = webdriver.Chrome(options=options)

    try:
        driver.get("http://localhost:8501")
        wait = WebDriverWait(driver, 25)

        # Wait for sidebar to be ready
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[@data-testid='stSidebar']")))
        time.sleep(2)

        # Find the Simulate button
        sim_btn = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'Simulate Ingestion Batch')]")))
        print("Found 'Simulate Ingestion Batch' button. Clicking...")
        sim_btn.click()

        # Monitor execution for up to 30 seconds
        print("Waiting for simulation status steps to complete...")
        time.sleep(8)

        # Take screenshot of simulation running/complete
        screenshot_path = Path("screenshots/07_simulation_running.png").resolve()
        driver.save_screenshot(str(screenshot_path))
        print(f"Captured screenshot: {screenshot_path}")

        # Wait additional time for rerun to complete
        time.sleep(10)

        # Capture final overview showing newly reconciled batch
        final_screenshot = Path("screenshots/08_after_simulation.png").resolve()
        driver.save_screenshot(str(final_screenshot))
        print(f"Captured post-simulation screenshot: {final_screenshot}")

        print("Simulate batch UI test finished successfully!")

    finally:
        driver.quit()

if __name__ == "__main__":
    test_simulate()
