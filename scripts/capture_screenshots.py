"""
Captures high-resolution screenshots of all 6 dashboard views using headless Chrome.
"""

import os
import sys
import time
from pathlib import Path

# Ensure UTF-8 stdout on Windows console
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

SCREENSHOT_DIR = Path("screenshots").resolve()
SCREENSHOT_DIR.mkdir(parents=True, exist_ok=True)

PAGES = [
    ("📊 Overview", "01_overview.png"),
    ("⚖️ Reconciliation", "02_reconciliation.png"),
    ("⚡ Pipeline Monitoring", "03_pipeline_monitoring.png"),
    ("🚨 Incident Management", "04_incident_management.png"),
    ("📈 Historical Analytics", "05_historical_analytics.png"),
    ("🛡️ Data Quality", "06_data_quality.png")
]

def capture_all_pages():
    print("Launching headless Chrome for dashboard screenshot capture...")
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    try:
        url = "http://localhost:8501"
        print(f"Navigating to {url}...")
        driver.get(url)

        # Wait for Streamlit to initialize
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.XPATH, "//*[@data-testid='stSidebar']")))
        time.sleep(3)  # Allow initial query caching and rendering

        for page_label, filename in PAGES:
            print(f"Navigating to page: '{page_label}'...")
            
            # Find and click radio option
            try:
                # Find label containing text
                radio_elem = driver.find_element(By.XPATH, f"//label[contains(., '{page_label}')]")
                radio_elem.click()
            except Exception as e:
                # Fallback: search by input text
                print(f"Warning clicking '{page_label}': {e}. Retrying via javascript...")
                driver.execute_script(f"""
                    const labels = Array.from(document.querySelectorAll('label'));
                    const target = labels.find(el => el.textContent.includes('{page_label}'));
                    if (target) target.click();
                """)

            # Wait for content to settle and charts to load
            time.sleep(3.5)

            out_path = SCREENSHOT_DIR / filename
            driver.save_screenshot(str(out_path))
            print(f"Saved: {out_path} ({os.path.getsize(out_path) / 1024:.1f} KB)")

        print("\nAll 6 page screenshots captured successfully!")
    finally:
        driver.quit()

if __name__ == "__main__":
    capture_all_pages()
