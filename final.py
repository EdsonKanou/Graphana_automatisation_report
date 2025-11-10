#!/usr/bin/env python3
"""
Système automatisé de génération de rapports Grafana
Génère des tableaux HTML avec les métriques de plusieurs dashboards sur différentes périodes
"""

import requests
import urllib3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import subprocess
import tempfile
import os

# Désactiver les warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('grafana_reporter.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ==========================================
# CONFIGURATION GRAFANA
# ==========================================
GRAFANA_CONFIG = {
    "base_url": "https://orchestrator-dashboard.group.echonet",
    "api_key": "xx",
    "datasource_uid": "cebe2fax1rieob",
    "datasource_type": "grafana-postgresql-datasource"
}


# ==========================================
# CONFIGURATION DES DASHBOARDS ET PANELS
# ==========================================
DASHBOARDS = {
    "Dashboard Orchestrator": {
        "panels": [
            {
                "name": "Taux de succès global",
                "query": """
                    SELECT 
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100 
                    FROM m_demand_statistics dem 
                    WHERE dem.product IN (
                        'cos','cos.bucket','kafka.cluster','kafka.topic','lb.roksfast14',
                        'lb.rokshttps','lb.vsifastl4','lb.vsihttps','mongodb.enterprise',
                        'mongodb.standard','mqaas.queue_manager','mqaas.reserved_capacity',
                        'mqaas.reserved_deployment','oradbaas.cdb','oradbaas.cluster_mgr',
                        'oradbaas.db_home','oradbaas.pdb','oradbaas.vm_cluster',
                        'package.dmzrc_open','postgres','realm','realm.apcode',
                        'roks','roks.ns','server.vsi'
                    ) 
                    AND dem.action IN ('delete','update','create') 
                    AND dem.status IN ('SUCCESS','ON_ERROR') 
                    AND dem.create_date >= '{time_from}' 
                    AND dem.end_date <= '{time_to}' 
                    AND dem.requestor_uid LIKE 'ServiceId%'
                """,
                "unit": "%"
            },
            # Ajoutez d'autres panels ici
            # {
            #     "name": "Nombre total de requêtes",
            #     "query": "SELECT COUNT(*) FROM m_demand_statistics WHERE ...",
            #     "unit": ""
            # }
        ]
    },
    # Ajoutez d'autres dashboards ici
    # "Dashboard Performance": {
    #     "panels": [...]
    # }
}


# ==========================================
# PÉRIODES À ANALYSER
# ==========================================
PERIODS = {
    "14 jours": timedelta(days=14),
    "2 mois": timedelta(days=60),
    "3 mois": timedelta(days=90)
}


# ==========================================
# CONFIGURATION EMAIL
# ==========================================
EMAIL_CONFIG = {
    "recipients": [
        "destinataire1@example.com",
        "destinataire2@example.com"
    ],
    "subject_template": "[Rapport Grafana] - {date}",
    "introduction": """
    <p>Bonjour,</p>
    <p>Veuillez trouver ci-dessous le rapport automatique des métriques Grafana pour les périodes suivantes : 14 jours, 2 mois et 3 mois.</p>
    """
}


# ==========================================
# FONCTIONS PRINCIPALES
# ==========================================

def execute_query(query: str, time_from: datetime, time_to: datetime) -> Optional[float]:
    """
    Exécute une requête Grafana et retourne la valeur
    
    Args:
        query: Requête SQL avec placeholders {time_from} et {time_to}
        time_from: Date de début
        time_to: Date de fin
    
    Returns:
        Valeur numérique ou None si erreur
    """
    url = f"{GRAFANA_CONFIG['base_url']}/api/ds/query"
    headers = {
        'Authorization': f"Bearer {GRAFANA_CONFIG['api_key']}",
        'Content-Type': 'application/json'
    }
    
    # Formater les dates
    time_from_str = time_from.strftime('%Y-%m-%dT%H:%M:%SZ')
    time_to_str = time_to.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    # Remplacer les placeholders dans la requête
    formatted_query = query.format(time_from=time_from_str, time_to=time_to_str)
    
    payload = {
        "queries": [{
            "refId": "A",
            "datasource": {
                "type": GRAFANA_CONFIG['datasource_type'],
                "uid": GRAFANA_CONFIG['datasource_uid']
            },
            "rawSql": formatted_query,
            "format": "table"
        }],
        "from": time_from_str,
        "to": time_to_str
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, verify=False, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Extraire la valeur
        value = data['results']['A']['frames'][0]['data']['values'][0][0]
        return value
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution de la requête: {e}")
        return None


def collect_metrics() -> Dict[str, Dict[str, Dict[str, Any]]]:
    """
    Collecte toutes les métriques pour tous les dashboards et périodes
    
    Returns:
        Structure: {dashboard_name: {panel_name: {period_name: value}}}
    """
    logger.info("Début de la collecte des métriques...")
    results = {}
    time_to = datetime.utcnow()
    
    for dashboard_name, dashboard_config in DASHBOARDS.items():
        logger.info(f"Traitement du dashboard: {dashboard_name}")
        results[dashboard_name] = {}
        
        for panel in dashboard_config['panels']:
            panel_name = panel['name']
            panel_query = panel['query']
            panel_unit = panel.get('unit', '')
            
            logger.info(f"  Traitement du panel: {panel_name}")
            results[dashboard_name][panel_name] = {'unit': panel_unit}
            
            for period_name, period_delta in PERIODS.items():
                time_from = time_to - period_delta
                logger.info(f"    Période: {period_name}")
                
                value = execute_query(panel_query, time_from, time_to)
                results[dashboard_name][panel_name][period_name] = value
    
    logger.info("Collecte des métriques terminée")
    return results


def format_value(value: Optional[float], unit: str = "") -> str:
    """
    Formate une valeur pour l'affichage
    
    Args:
        value: Valeur numérique ou None
        unit: Unité de mesure (%, etc.)
    
    Returns:
        Chaîne formatée
    """
    if value is None:
        return "N/A"
    
    # Formater avec 2 décimales
    formatted = f"{value:.2f}"
    
    if unit:
        formatted += f" {unit}"
    
    return formatted


def generate_html_report(metrics: Dict[str, Dict[str, Dict[str, Any]]]) -> str:
    """
    Génère le rapport HTML avec CSS
    
    Args:
        metrics: Données collectées
    
    Returns:
        Code HTML complet
    """
    logger.info("Génération du rapport HTML...")
    
    # En-tête HTML avec CSS
    html = """
    <!DOCTYPE html>
    <html lang="fr">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Rapport Grafana</title>
        <style>
            body {
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                line-height: 1.6;
                color: #333;
                max-width: 1200px;
                margin: 0 auto;
                padding: 20px;
                background-color: #f5f5f5;
            }
            .header {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                border-radius: 10px;
                margin-bottom: 30px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.1);
            }
            .header h1 {
                margin: 0;
                font-size: 28px;
            }
            .header .date {
                margin-top: 10px;
                opacity: 0.9;
                font-size: 14px;
            }
            .introduction {
                background: white;
                padding: 20px;
                border-radius: 8px;
                margin-bottom: 30px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            }
            .dashboard-section {
                background: white;
                padding: 25px;
                border-radius: 8px;
                margin-bottom: 30px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            }
            .dashboard-title {
                font-size: 22px;
                font-weight: bold;
                color: #667eea;
                margin-bottom: 20px;
                border-bottom: 3px solid #667eea;
                padding-bottom: 10px;
            }
            table {
                width: 100%;
                border-collapse: collapse;
                margin-top: 15px;
            }
            th {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 12px;
                text-align: left;
                font-weight: 600;
                font-size: 14px;
            }
            th:first-child {
                border-radius: 8px 0 0 0;
            }
            th:last-child {
                border-radius: 0 8px 0 0;
            }
            td {
                padding: 12px;
                border-bottom: 1px solid #e0e0e0;
                font-size: 14px;
            }
            tr:hover {
                background-color: #f8f9fa;
            }
            tr:last-child td {
                border-bottom: none;
            }
            .metric-name {
                font-weight: 500;
                color: #333;
            }
            .metric-value {
                text-align: center;
                font-weight: 500;
            }
            .na-value {
                color: #dc3545;
                font-style: italic;
            }
            .footer {
                text-align: center;
                color: #666;
                font-size: 12px;
                margin-top: 30px;
                padding-top: 20px;
                border-top: 1px solid #ddd;
            }
        </style>
    </head>
    <body>
        <div class="header">
            <h1>📊 Rapport de Métriques Grafana</h1>
            <div class="date">Généré le {date}</div>
        </div>
        
        <div class="introduction">
            {introduction}
        </div>
    """.format(
        date=datetime.now().strftime('%d/%m/%Y à %H:%M'),
        introduction=EMAIL_CONFIG['introduction']
    )
    
    # Générer les tableaux pour chaque dashboard
    for dashboard_name, panels in metrics.items():
        html += f"""
        <div class="dashboard-section">
            <div class="dashboard-title">{dashboard_name}</div>
            <table>
                <thead>
                    <tr>
                        <th>Métrique</th>
        """
        
        # En-têtes des colonnes (périodes)
        for period_name in PERIODS.keys():
            html += f"<th style='text-align: center;'>{period_name}</th>"
        
        html += """
                    </tr>
                </thead>
                <tbody>
        """
        
        # Lignes des métriques
        for panel_name, panel_data in panels.items():
            unit = panel_data.get('unit', '')
            html += f"""
                    <tr>
                        <td class="metric-name">{panel_name}</td>
            """
            
            for period_name in PERIODS.keys():
                value = panel_data.get(period_name)
                formatted_value = format_value(value, unit)
                css_class = "na-value" if value is None else ""
                
                html += f'<td class="metric-value {css_class}">{formatted_value}</td>'
            
            html += "</tr>"
        
        html += """
                </tbody>
            </table>
        </div>
        """
    
    # Footer
    html += """
        <div class="footer">
            <p>Rapport généré automatiquement par le système de monitoring Grafana</p>
        </div>
    </body>
    </html>
    """
    
    logger.info("Rapport HTML généré avec succès")
    return html


def send_email_via_outlook(html_content: str, recipients: List[str], subject: str):
    """
    Envoie un email via Outlook sur Mac en ouvrant un nouveau message
    
    Args:
        html_content: Contenu HTML du rapport
        recipients: Liste des destinataires
        subject: Objet du mail
    """
    logger.info("Préparation de l'envoi d'email via Outlook...")
    
    # Sauvegarder le HTML temporairement
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False, encoding='utf-8') as f:
        f.write(html_content)
        temp_file = f.name
    
    try:
        # Préparer les destinataires
        recipients_str = ", ".join(recipients)
        
        # Script AppleScript pour ouvrir Outlook avec le message pré-rempli
        applescript = f'''
        tell application "Microsoft Outlook"
            activate
            set newMessage to make new outgoing message with properties {{subject:"{subject}", content:""}}
            tell newMessage
                make new recipient at newMessage with properties {{email address:{{address:"{recipients_str}"}}}}
            end tell
            open newMessage
        end tell
        '''
        
        # Exécuter AppleScript
        subprocess.run(['osascript', '-e', applescript], check=True)
        
        logger.info(f"Email préparé dans Outlook. Fichier HTML sauvegardé: {temp_file}")
        logger.info("⚠️  IMPORTANT: Copiez le contenu du fichier HTML et collez-le dans le corps du message Outlook")
        logger.info(f"📁 Fichier HTML: {temp_file}")
        
        # Ouvrir le fichier HTML dans le navigateur pour faciliter le copier-coller
        subprocess.run(['open', temp_file])
        
    except Exception as e:
        logger.error(f"Erreur lors de la préparation de l'email: {e}")
        logger.info(f"Vous pouvez toujours ouvrir manuellement le fichier: {temp_file}")


def main():
    """Fonction principale d'exécution du rapport"""
    logger.info("=" * 50)
    logger.info("DÉBUT DU PROCESSUS DE GÉNÉRATION DE RAPPORT")
    logger.info("=" * 50)
    
    try:
        # 1. Collecter les métriques
        metrics = collect_metrics()
        
        # 2. Générer le HTML
        html_report = generate_html_report(metrics)
        
        # 3. Préparer le sujet de l'email
        subject = EMAIL_CONFIG['subject_template'].format(
            date=datetime.now().strftime('%d/%m/%Y')
        )
        
        # 4. Envoyer l'email
        send_email_via_outlook(
            html_content=html_report,
            recipients=EMAIL_CONFIG['recipients'],
            subject=subject
        )
        
        logger.info("=" * 50)
        logger.info("PROCESSUS TERMINÉ AVEC SUCCÈS")
        logger.info("=" * 50)
        
    except Exception as e:
        logger.error(f"Erreur critique: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()