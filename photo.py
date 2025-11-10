#!/usr/bin/env python3
"""
Capture de dashboards Grafana avec Playwright
Méthode fiable qui fonctionne même si l'API Rendering est désactivée
"""

from playwright.sync_api import sync_playwright
from datetime import datetime
import os
import time

# ==========================================
# CONFIGURATION
# ==========================================
GRAFANA_URL = "https://orchestrator-dashboard.group.echonet"
API_KEY = "glsa_EghQphwQdMxZoVQca5aLLRXAQRRNJQ61_9t087d84"
DASHBOARD_UID = "debe5b0zgerr4b"
OUTPUT_DIR = "grafana_screenshots"

# ==========================================
# FONCTIONS
# ==========================================

def capture_dashboard_playwright(
    dashboard_uid: str,
    time_from: str = "now-24h",
    time_to: str = "now",
    org_id: int = 1,
    width: int = 1920,
    height: int = 1080,
    filename: str = None,
    wait_time: int = 8000
):
    """
    Capture un dashboard Grafana avec Playwright
    
    Args:
        dashboard_uid: UID du dashboard
        time_from: Début période (ex: 'now-24h', 'now-7d')
        time_to: Fin période (ex: 'now')
        org_id: ID de l'organisation Grafana
        width: Largeur viewport
        height: Hauteur viewport
        filename: Nom du fichier (auto si None)
        wait_time: Temps d'attente en ms pour le chargement
    
    Returns:
        Chemin du fichier sauvegardé
    """
    # Créer le dossier de sortie
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Construire l'URL
    url = f"{GRAFANA_URL}/d/{dashboard_uid}?orgId={org_id}&from={time_from}&to={time_to}&kiosk"
    
    # Générer le nom de fichier
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"dashboard_{dashboard_uid}_{timestamp}.png"
    
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    print(f"📸 Capture du dashboard avec Playwright...")
    print(f"   URL: {url}")
    print(f"   Dimensions: {width}x{height}px")
    
    try:
        with sync_playwright() as pw:
            # Lancer le navigateur
            browser = pw.chromium.launch(
                headless=True,  # Mettre False pour voir le navigateur
                args=['--ignore-certificate-errors']  # Pour le SSL
            )
            
            # Créer un contexte avec authentification
            context = browser.new_context(
                viewport={'width': width, 'height': height},
                extra_http_headers={
                    'Authorization': f'Bearer {API_KEY}'
                },
                ignore_https_errors=True
            )
            
            # Nouvelle page
            page = context.new_page()
            
            print(f"   Chargement de la page...")
            page.goto(url, wait_until='networkidle', timeout=60000)
            
            # Attendre que les panels se chargent
            print(f"   Attente du rendu ({wait_time/1000}s)...")
            page.wait_for_timeout(wait_time)
            
            # Capturer la page complète
            print(f"   Capture en cours...")
            page.screenshot(path=filepath, full_page=True)
            
            # Nettoyer
            browser.close()
            
            print(f"✅ Capture sauvegardée: {filepath}")
            file_size = os.path.getsize(filepath) / 1024
            print(f"   Taille: {file_size:.2f} KB")
            
            return filepath
            
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return None


def capture_panel_playwright(
    dashboard_uid: str,
    panel_id: int,
    time_from: str = "now-24h",
    time_to: str = "now",
    org_id: int = 1,
    width: int = 1200,
    height: int = 600,
    filename: str = None,
    wait_time: int = 5000
):
    """
    Capture un panel spécifique avec Playwright
    
    Args:
        dashboard_uid: UID du dashboard
        panel_id: ID du panel
        time_from: Début période
        time_to: Fin période
        org_id: ID organisation
        width: Largeur
        height: Hauteur
        filename: Nom du fichier
        wait_time: Temps d'attente en ms
    
    Returns:
        Chemin du fichier sauvegardé
    """
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # URL pour un panel en mode solo
    url = f"{GRAFANA_URL}/d-solo/{dashboard_uid}?orgId={org_id}&from={time_from}&to={time_to}&panelId={panel_id}"
    
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"panel_{panel_id}_{timestamp}.png"
    
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    print(f"📸 Capture du panel {panel_id}...")
    
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=['--ignore-certificate-errors']
            )
            
            context = browser.new_context(
                viewport={'width': width, 'height': height},
                extra_http_headers={'Authorization': f'Bearer {API_KEY}'},
                ignore_https_errors=True
            )
            
            page = context.new_page()
            page.goto(url, wait_until='networkidle', timeout=60000)
            page.wait_for_timeout(wait_time)
            page.screenshot(path=filepath)
            
            browser.close()
            
            print(f"✅ Panel capturé: {filepath}")
            return filepath
            
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return None


def capture_multiple_periods_playwright(dashboard_uid: str):
    """
    Capture un dashboard sur plusieurs périodes avec Playwright
    
    Args:
        dashboard_uid: UID du dashboard
    """
    periods = {
        "14j": "now-14d",
        "2m": "now-60d",
        "3m": "now-90d"
    }
    
    print(f"\n📊 Capture du dashboard sur plusieurs périodes...\n")
    
    for period_name, time_from in periods.items():
        filename = f"dashboard_{dashboard_uid}_{period_name}.png"
        capture_dashboard_playwright(
            dashboard_uid=dashboard_uid,
            time_from=time_from,
            time_to="now",
            filename=filename
        )
        print()


# ==========================================
# PROGRAMME PRINCIPAL
# ==========================================
if __name__ == "__main__":
    
    print("=" * 70)
    print("CAPTURE AUTOMATIQUE GRAFANA AVEC PLAYWRIGHT")
    print("=" * 70)
    print()
    
    # EXEMPLE 1: Dashboard complet (dernières 24h)
    print("📌 EXEMPLE 1: Dashboard complet (24h)")
    capture_dashboard_playwright(
        dashboard_uid=DASHBOARD_UID,
        time_from="now-24h",
        time_to="now",
        width=1920,
        height=1080
    )
    print()
    
    # EXEMPLE 2: Panel spécifique
    print("📌 EXEMPLE 2: Panel spécifique (ID 12)")
    capture_panel_playwright(
        dashboard_uid=DASHBOARD_UID,
        panel_id=12,
        time_from="now-7d",
        time_to="now"
    )
    print()
    
    # EXEMPLE 3: Plusieurs périodes (14j, 2m, 3m)
    print("📌 EXEMPLE 3: Plusieurs périodes")
    capture_multiple_periods_playwright(DASHBOARD_UID)
    
    print("=" * 70)
    print("✅ TERMINÉ - Captures dans:", OUTPUT_DIR)
    print("=" * 70)
    
    
    
    #!/usr/bin/env python3
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import os

GRAFANA_URL = "https://orchestrator-dashboard.group.echonet"
API_KEY = "glsa_EghQphwQdMxZoVQca5aLLRXAQRRNJQ61_9t087d84"
DASHBOARD_UID = "debe5b0zgerr4b"

def capture_with_selenium(time_from="now-24h"):
    url = f"{GRAFANA_URL}/d/{DASHBOARD_UID}?orgId=1&from={time_from}&to=now&kiosk"
    
    # Configuration Chrome
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--window-size=1920,1080')
    options.add_argument(f'--authorization=Bearer {API_KEY}')
    
    # Lancer Chrome
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    
    driver.get(url)
    time.sleep(8)  # Attendre le chargement
    
    filename = f"dashboard_{time_from.replace('-', '_')}.png"
    driver.save_screenshot(filename)
    print(f"✅ Capture: {filename}")
    
    driver.quit()

# Utilisation
capture_with_selenium("now-24h")
capture_with_selenium("now-14d")
capture_with_selenium("now-90d")



# Remplacez cette ligne :
browser = pw.chromium.launch(
    headless=True,
    args=['--ignore-certificate-errors']
)

# Par celle-ci :
browser = pw.chromium.launch(
    headless=True,
    channel="chrome",  # ← Utilise Chrome installé
    args=['--ignore-certificate-errors']
)