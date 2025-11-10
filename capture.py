#!/usr/bin/env python3
"""
Script simple pour capturer automatiquement des dashboards Grafana
"""

import requests
import urllib3
from datetime import datetime
import os

# Désactiver les warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ==========================================
# CONFIGURATION
# ==========================================
GRAFANA_URL = "https://orchestrator-dashboard.group.echonet"
API_KEY = "xxx"

# UID de votre dashboard (exemple: 'debe5b0zgerr4b')
DASHBOARD_UID = "debe5b0zgerr4b"

# Dossier où sauvegarder les captures
OUTPUT_DIR = "grafana_screenshots"


# ==========================================
# FONCTION PRINCIPALE
# ==========================================
def capture_dashboard(
    dashboard_uid: str,
    time_from: str = "now-24h",
    time_to: str = "now",
    width: int = 1920,
    height: int = 1080,
    filename: str = None
):
    """
    Capture un screenshot d'un dashboard Grafana complet
    
    Args:
        dashboard_uid: UID du dashboard
        time_from: Période de début (ex: 'now-24h', 'now-7d')
        time_to: Période de fin (ex: 'now')
        width: Largeur de l'image en pixels
        height: Hauteur de l'image en pixels
        filename: Nom du fichier (auto si None)
    
    Returns:
        Chemin du fichier sauvegardé
    """
    # Créer le dossier de sortie s'il n'existe pas
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # URL de l'API Rendering de Grafana
    url = f"{GRAFANA_URL}/render/d/{dashboard_uid}"
    
    # Paramètres de la requête
    params = {
        'from': time_from,
        'to': time_to,
        'width': width,
        'height': height,
        'theme': 'light'  # ou 'dark'
    }
    
    # Headers avec authentification
    headers = {
        'Authorization': f'Bearer {API_KEY}'
    }
    
    # Générer le nom de fichier si non fourni
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"dashboard_{dashboard_uid}_{timestamp}.png"
    
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    print(f"📸 Capture du dashboard {dashboard_uid}...")
    print(f"   Période: {time_from} → {time_to}")
    print(f"   Dimensions: {width}x{height}px")
    
    try:
        # Faire la requête
        response = requests.get(
            url,
            params=params,
            headers=headers,
            verify=False,
            timeout=60
        )
        response.raise_for_status()
        
        # Sauvegarder l'image
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        print(f"✅ Capture sauvegardée: {filepath}")
        print(f"   Taille: {len(response.content) / 1024:.2f} KB")
        
        return filepath
        
    except Exception as e:
        print(f"❌ Erreur lors de la capture: {e}")
        return None


def capture_panel(
    dashboard_uid: str,
    panel_id: int,
    time_from: str = "now-24h",
    time_to: str = "now",
    width: int = 1000,
    height: int = 500,
    filename: str = None
):
    """
    Capture un screenshot d'un panel spécifique
    
    Args:
        dashboard_uid: UID du dashboard
        panel_id: ID du panel (numéro visible dans l'URL Grafana)
        time_from: Période de début
        time_to: Période de fin
        width: Largeur de l'image
        height: Hauteur de l'image
        filename: Nom du fichier (auto si None)
    
    Returns:
        Chemin du fichier sauvegardé
    """
    # Créer le dossier de sortie
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # URL pour un panel spécifique
    url = f"{GRAFANA_URL}/render/d-solo/{dashboard_uid}"
    
    params = {
        'panelId': panel_id,
        'from': time_from,
        'to': time_to,
        'width': width,
        'height': height,
        'theme': 'light'
    }
    
    headers = {
        'Authorization': f'Bearer {API_KEY}'
    }
    
    # Générer le nom de fichier
    if filename is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"panel_{panel_id}_{timestamp}.png"
    
    filepath = os.path.join(OUTPUT_DIR, filename)
    
    print(f"📸 Capture du panel {panel_id}...")
    
    try:
        response = requests.get(
            url,
            params=params,
            headers=headers,
            verify=False,
            timeout=60
        )
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            f.write(response.content)
        
        print(f"✅ Panel capturé: {filepath}")
        return filepath
        
    except Exception as e:
        print(f"❌ Erreur: {e}")
        return None


def capture_multiple_periods(dashboard_uid: str):
    """
    Capture un dashboard sur plusieurs périodes
    
    Args:
        dashboard_uid: UID du dashboard
    """
    periods = {
        "24h": "now-24h",
        "7d": "now-7d",
        "14d": "now-14d",
        "30d": "now-30d"
    }
    
    print(f"\n📊 Capture du dashboard {dashboard_uid} sur plusieurs périodes...\n")
    
    for period_name, time_from in periods.items():
        filename = f"dashboard_{dashboard_uid}_{period_name}.png"
        capture_dashboard(
            dashboard_uid=dashboard_uid,
            time_from=time_from,
            time_to="now",
            filename=filename
        )
        print()


# ==========================================
# EXEMPLES D'UTILISATION
# ==========================================
if __name__ == "__main__":
    
    print("=" * 60)
    print("CAPTURE AUTOMATIQUE DE DASHBOARDS GRAFANA")
    print("=" * 60)
    print()
    
    # EXEMPLE 1: Capturer un dashboard complet (dernières 24h)
    print("📌 EXEMPLE 1: Dashboard complet")
    capture_dashboard(
        dashboard_uid=DASHBOARD_UID,
        time_from="now-24h",
        time_to="now",
        width=1920,
        height=1080
    )
    print()
    
    # EXEMPLE 2: Capturer un panel spécifique
    print("📌 EXEMPLE 2: Panel spécifique (ID 12)")
    capture_panel(
        dashboard_uid=DASHBOARD_UID,
        panel_id=12,
        time_from="now-7d",
        time_to="now"
    )
    print()
    
    # EXEMPLE 3: Capturer sur plusieurs périodes
    print("📌 EXEMPLE 3: Plusieurs périodes")
    capture_multiple_periods(DASHBOARD_UID)
    
    print("=" * 60)
    print("✅ TERMINÉ - Toutes les captures sont dans:", OUTPUT_DIR)
    print("=" * 60)