#!/usr/bin/env python3
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import os

def capture_page(url, output_file="capture.png"):
    options = Options()
    options.add_argument("--headless")  # Pas d'affichage graphique
    options.add_argument("--window-size=1920,1080")

    # Lancer Chrome avec le driver automatique
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    print(f"📸 Capture de : {url}")
    driver.get(url)
    time.sleep(3)  # Attente du chargement complet

    driver.save_screenshot(output_file)
    driver.quit()
    print(f"✅ Capture enregistrée : {os.path.abspath(output_file)}")

if __name__ == "__main__":
    #capture_page("https://example.com", "example_capture.png")
    capture_page("https://www.wikipedia.org", "wikipedia.png")

