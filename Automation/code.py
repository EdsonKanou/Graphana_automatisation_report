################################################################################
# FICHIER : .env
# Description : Configuration sensible (credentials)
################################################################################

# Grafana Configuration
GRAFANA_URL=https://orchestrator-dashboard.group.echonet
GRAFANA_API_KEY=glsa_EahQphwQdMy7oV0ca5aLLRXAORRNJ061_9f087d84

# SMTP Configuration
SMTP_SERVER=smtp.office365.com
SMTP_PORT=587
SMTP_USERNAME=votre.email@entreprise.com
SMTP_PASSWORD=votre_mot_de_passe
SMTP_USE_TLS=True

# Email Configuration
EMAIL_FROM=votre.email@entreprise.com
EMAIL_TO=destinataire1@entreprise.com,destinataire2@entreprise.com
EMAIL_SUBJECT=Rapport Grafana - Métriques de Performance


################################################################################
# FICHIER : config.py
# Description : Configuration des dashboards et panels
################################################################################

"""
Configuration des dashboards et panels Grafana
"""

# Périodes à analyser
TIME_PERIODS = [
    {"label": "14 derniers jours", "days": 14, "grafana_range": "14d"},
    {"label": "2 derniers mois", "days": 60, "grafana_range": "60d"},
    {"label": "3 derniers mois", "days": 90, "grafana_range": "90d"}
]

# Configuration des dashboards et leurs panels
DASHBOARDS = [
    {
        "name": "Toolchain Performance",
        "uid": "cebe2faxirjeob",
        "panels": [
            {
                "name": "Demand Success Rate",
                "panel_id": "1",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product IN (
                        'cos', 'cos.bucket', 'kafka.cluster', 'kafka.topic', 'lb.roksfast14',
                        'lb.rokshttps', 'lb.vsifast14', 'lb.vsihttps', 'mongodb.enterprise',
                        'mongodb.standard', 'mqaas.queue_manager', 'mqaas.reserved_capacity',
                        'mqaas.reserved_deployment', 'oradbaas.cdb', 'oradbaas.cluster_mgr',
                        'oradbaas.db_home', 'oradbaas.pdb', 'oradbaas.vm_cluster',
                        'package.dmzre_open', 'postgres', 'realm', 'realm.apcode',
                        'roks', 'roks.ns', 'server.vsi'
                    )
                    AND dem.action IN ('delete', 'update', 'create')
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                    AND dem.requestor_uid LIKE 'Serviceld%'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            },
            {
                "name": "Total Demands",
                "panel_id": "2",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT COUNT(*) as total
                    FROM m_demand_statistics dem
                    WHERE dem.product IN (
                        'cos', 'cos.bucket', 'kafka.cluster', 'kafka.topic', 'lb.roksfast14',
                        'lb.rokshttps', 'lb.vsifast14', 'lb.vsihttps', 'mongodb.enterprise',
                        'mongodb.standard', 'mqaas.queue_manager', 'mqaas.reserved_capacity',
                        'mqaas.reserved_deployment', 'oradbaas.cdb', 'oradbaas.cluster_mgr',
                        'oradbaas.db_home', 'oradbaas.pdb', 'oradbaas.vm_cluster',
                        'package.dmzre_open', 'postgres', 'realm', 'realm.apcode',
                        'roks', 'roks.ns', 'server.vsi'
                    )
                    AND dem.action IN ('delete', 'update', 'create')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "",
                "thresholds": {"warning": 1000, "critical": 500}
            },
            {
                "name": "Average Processing Time",
                "panel_id": "3",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT AVG(EXTRACT(EPOCH FROM (dem.end_date - dem.create_date))) as avg_time
                    FROM m_demand_statistics dem
                    WHERE dem.product IN (
                        'cos', 'postgres', 'mongodb.enterprise', 'roks'
                    )
                    AND dem.status = 'SUCCESS'
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "avg",
                "unit": "s",
                "thresholds": {"warning": 300, "critical": 600}
            }
        ]
    },
    {
        "name": "Infrastructure Monitoring",
        "uid": "infra-dashboard-uid",
        "panels": [
            {
                "name": "Server VSI Success Rate",
                "panel_id": "10",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product = 'server.vsi'
                    AND dem.action IN ('create', 'delete', 'update')
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            },
            {
                "name": "ROKS Cluster Success Rate",
                "panel_id": "11",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product IN ('roks', 'roks.ns')
                    AND dem.action IN ('create', 'delete')
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            }
        ]
    },
    {
        "name": "Database Services",
        "uid": "db-services-uid",
        "panels": [
            {
                "name": "PostgreSQL Provisioning Rate",
                "panel_id": "20",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product = 'postgres'
                    AND dem.action = 'create'
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            },
            {
                "name": "MongoDB Operations Success",
                "panel_id": "21",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product IN ('mongodb.enterprise', 'mongodb.standard')
                    AND dem.action IN ('create', 'delete', 'update')
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            },
            {
                "name": "Oracle DB Success Rate",
                "panel_id": "22",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "cebe2faxirjeob"
                },
                "sql_query": """
                    SELECT
                        (SUM(CASE WHEN dem.status = 'SUCCESS' THEN 1 ELSE 0 END) * 1.0 / COUNT(*)) * 100
                    FROM m_demand_statistics dem
                    WHERE dem.product IN ('oradbaas.cdb', 'oradbaas.pdb', 'oradbaas.db_home')
                    AND dem.action IN ('create', 'delete', 'update')
                    AND dem.status IN ('SUCCESS', 'ON_ERROR')
                    AND dem.create_date >= '{time_from}'
                    AND dem.end_date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            }
        ]
    }
]


################################################################################
# FICHIER : logger_config.py
# Description : Configuration du système de logging
################################################################################

"""
Configuration du système de logging
"""
import logging
import os
from datetime import datetime
from pathlib import Path


def setup_logger(name: str = "grafana_reporter") -> logging.Logger:
    """
    Configure et retourne un logger avec rotation de fichiers
    
    Args:
        name: Nom du logger
        
    Returns:
        Logger configuré
    """
    # Créer le dossier logs s'il n'existe pas
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Nom du fichier avec timestamp
    log_filename = log_dir / f"grafana_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configuration du logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Éviter les duplications si le logger existe déjà
    if logger.handlers:
        return logger
    
    # Format détaillé pour les logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler pour fichier (tout)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Handler pour console (INFO et plus)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    
    # Ajouter les handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logger initialisé - fichier: {log_filename}")
    
    return logger


def cleanup_old_logs(days: int = 30):
    """
    Supprime les logs plus vieux que X jours
    
    Args:
        days: Nombre de jours de rétention
    """
    log_dir = Path("logs")
    if not log_dir.exists():
        return
    
    cutoff_time = datetime.now().timestamp() - (days * 86400)
    deleted_count = 0
    
    for log_file in log_dir.glob("grafana_report_*.log"):
        if log_file.stat().st_mtime < cutoff_time:
            log_file.unlink()
            deleted_count += 1
    
    if deleted_count > 0:
        logger = logging.getLogger("grafana_reporter")
        logger.info(f"Nettoyage: {deleted_count} fichier(s) log supprimé(s)")


################################################################################
# FICHIER : grafana_client.py
# Description : Client pour interroger l'API Grafana
################################################################################

"""
Client pour interroger l'API Grafana
"""
import requests
import urllib3
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import logging

# Désactiver les warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger("grafana_reporter")


class GrafanaClient:
    """Client pour interagir avec l'API Grafana"""
    
    def __init__(self, base_url: str, api_key: str):
        """
        Initialise le client Grafana
        
        Args:
            base_url: URL de base de Grafana
            api_key: Clé API d'authentification
        """
        self.base_url = base_url.rstrip('/')
        self.api_url = f"{self.base_url}/api/ds/query"
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        logger.info(f"Client Grafana initialisé pour {self.base_url}")
    
    def query_panel(
        self,
        panel_config: Dict[str, Any],
        time_from: datetime,
        time_to: datetime,
        grafana_range: str
    ) -> Optional[float]:
        """
        Exécute une requête pour un panel spécifique
        
        Args:
            panel_config: Configuration du panel (SQL, datasource, etc.)
            time_from: Date de début
            time_to: Date de fin
            grafana_range: Range Grafana (ex: "14d")
            
        Returns:
            Valeur extraite ou None en cas d'erreur
        """
        panel_name = panel_config.get('name', 'Unknown')
        logger.debug(f"Requête panel '{panel_name}' sur période {grafana_range}")
        
        # Formater les dates en ISO 8601
        time_from_str = time_from.strftime('%Y-%m-%dT%H:%M:%SZ')
        time_to_str = time_to.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        # Préparer la requête SQL avec les dates
        sql_query = panel_config['sql_query'].format(
            time_from=time_from_str,
            time_to=time_to_str
        )
        
        # Construire le payload
        payload = {
            "queries": [{
                "refId": "A",
                "datasource": panel_config['datasource'],
                "rawSql": sql_query,
                "format": panel_config.get('format', 'table')
            }],
            "from": f"now-{grafana_range}",
            "to": "now"
        }
        
        try:
            # Exécuter la requête
            response = requests.post(
                self.api_url,
                headers=self.headers,
                json=payload,
                verify=False,
                timeout=30
            )
            response.raise_for_status()
            data = response.json()
            
            # Extraire la valeur
            try:
                value = data['results']['A']['frames'][0]['data']['values'][0][0]
                logger.info(f"✓ Panel '{panel_name}' ({grafana_range}): {value}")
                return value
            except (KeyError, IndexError, TypeError) as e:
                logger.warning(f"Impossible d'extraire la valeur pour '{panel_name}': {e}")
                
                # Vérifier s'il y a une erreur SQL
                if 'error' in data.get('results', {}).get('A', {}):
                    error_msg = data['results']['A']['error']
                    logger.error(f"Erreur SQL pour '{panel_name}': {error_msg}")
                
                return None
                
        except requests.exceptions.Timeout:
            logger.error(f"Timeout lors de la requête pour '{panel_name}'")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur de requête pour '{panel_name}': {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.debug(f"Détails de la réponse: {e.response.text}")
            return None
    
    def query_dashboard(
        self,
        dashboard_config: Dict[str, Any],
        time_periods: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Interroge tous les panels d'un dashboard pour toutes les périodes
        
        Args:
            dashboard_config: Configuration complète du dashboard
            time_periods: Liste des périodes à interroger
            
        Returns:
            Dictionnaire avec les résultats par panel et période
        """
        dashboard_name = dashboard_config['name']
        logger.info(f"=== Interrogation du dashboard '{dashboard_name}' ===")
        
        results = {
            'name': dashboard_name,
            'uid': dashboard_config['uid'],
            'panels': []
        }
        
        for panel in dashboard_config['panels']:
            panel_name = panel['name']
            panel_results = {
                'name': panel_name,
                'unit': panel.get('unit', ''),
                'thresholds': panel.get('thresholds', {}),
                'periods': []
            }
            
            for period in time_periods:
                time_to = datetime.utcnow()
                time_from = time_to - timedelta(days=period['days'])
                
                value = self.query_panel(
                    panel,
                    time_from,
                    time_to,
                    period['grafana_range']
                )
                
                panel_results['periods'].append({
                    'label': period['label'],
                    'days': period['days'],
                    'value': value
                })
            
            results['panels'].append(panel_results)
        
        logger.info(f"Dashboard '{dashboard_name}' terminé")
        return results
    
    def query_all_dashboards(
        self,
        dashboards: List[Dict[str, Any]],
        time_periods: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Interroge tous les dashboards configurés
        
        Args:
            dashboards: Liste des configurations de dashboards
            time_periods: Liste des périodes à interroger
            
        Returns:
            Liste des résultats pour chaque dashboard
        """
        logger.info("🚀 Début de l'interrogation de tous les dashboards")
        all_results = []
        
        for dashboard in dashboards:
            try:
                result = self.query_dashboard(dashboard, time_periods)
                all_results.append(result)
            except Exception as e:
                logger.error(f"Erreur lors du traitement du dashboard '{dashboard['name']}': {e}")
                # Continuer avec les autres dashboards
                continue
        
        logger.info(f"✅ Interrogation terminée - {len(all_results)} dashboard(s) traité(s)")
        return all_results


################################################################################
# FICHIER : email_generator.py
# Description : Génération d'emails HTML pour les rapports
################################################################################

"""
Générateur d'emails HTML pour les rapports Grafana
"""
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger("grafana_reporter")


class EmailGenerator:
    """Génère des emails HTML formatés pour les rapports"""
    
    @staticmethod
    def get_status_color(value: Optional[float], thresholds: Dict[str, float], unit: str) -> str:
        """
        Détermine la couleur selon les seuils
        
        Args:
            value: Valeur à évaluer
            thresholds: Seuils warning/critical
            unit: Unité de mesure
            
        Returns:
            Code couleur hexadécimal
        """
        if value is None:
            return "#95a5a6"  # Gris pour valeur manquante
        
        # Pour les pourcentages, plus c'est haut, mieux c'est
        if unit == "%":
            if value >= thresholds.get('warning', 95):
                return "#27ae60"  # Vert
            elif value >= thresholds.get('critical', 90):
                return "#f39c12"  # Orange
            else:
                return "#e74c3c"  # Rouge
        else:
            # Pour les autres métriques (temps, count), inverser la logique si nécessaire
            if value <= thresholds.get('warning', 1000):
                return "#27ae60"  # Vert
            elif value <= thresholds.get('critical', 2000):
                return "#f39c12"  # Orange
            else:
                return "#e74c3c"  # Rouge
    
    @staticmethod
    def format_value(value: Optional[float], unit: str) -> str:
        """
        Formate une valeur avec son unité
        
        Args:
            value: Valeur à formater
            unit: Unité de mesure
            
        Returns:
            Chaîne formatée
        """
        if value is None:
            return "N/A"
        
        if unit == "%":
            return f"{value:.2f}%"
        elif unit == "s":
            if value < 60:
                return f"{value:.1f}s"
            elif value < 3600:
                return f"{value/60:.1f}min"
            else:
                return f"{value/3600:.1f}h"
        elif unit == "ms":
            return f"{value:.0f}ms"
        else:
            # Formatage intelligent pour les grands nombres
            if value >= 1000000:
                return f"{value/1000000:.1f}M"
            elif value >= 1000:
                return f"{value/1000:.1f}K"
            else:
                return f"{int(value)}"
    
    @staticmethod
    def calculate_trend(periods: List[Dict[str, Any]]) -> Optional[float]:
        """
        Calcule l'évolution entre la période la plus récente et la plus ancienne
        
        Args:
            periods: Liste des périodes avec leurs valeurs
            
        Returns:
            Pourcentage d'évolution ou None
        """
        if len(periods) < 2:
            return None
        
        # Première période (la plus récente)
        first_value = periods[0].get('value')
        # Dernière période (la plus ancienne)
        last_value = periods[-1].get('value')
        
        if first_value is None or last_value is None or last_value == 0:
            return None
        
        trend = ((first_value - last_value) / last_value) * 100
        return trend
    
    def generate_html(self, dashboards_data: List[Dict[str, Any]]) -> str:
        """
        Génère le HTML complet de l'email
        
        Args:
            dashboards_data: Données de tous les dashboards
            
        Returns:
            Code HTML de l'email
        """
        logger.info("Génération du HTML de l'email")
        
        # CSS inline pour compatibilité email
        css = """
        <style>
            body {
                font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
                background-color: #f5f7fa;
                margin: 0;
                padding: 20px;
                color: #2c3e50;
            }
            .container {
                max-width: 1200px;
                margin: 0 auto;
                background-color: #ffffff;
                border-radius: 8px;
                box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                overflow: hidden;
            }
            .header {
                background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                color: white;
                padding: 30px;
                text-align: center;
            }
            .header h1 {
                margin: 0;
                font-size: 28px;
                font-weight: 600;
            }
            .header p {
                margin: 10px 0 0 0;
                opacity: 0.9;
                font-size: 14px;
            }
            .dashboard {
                margin: 30px;
                border-left: 4px solid #667eea;
                padding-left: 20px;
            }
            .dashboard-title {
                font-size: 22px;
                font-weight: 600;
                color: #2c3e50;
                margin-bottom: 20px;
            }
            .panels-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(350px, 1fr));
                gap: 20px;
                margin-top: 15px;
            }
            .panel-card {
                background: #f8f9fa;
                border-radius: 8px;
                padding: 20px;
                border: 1px solid #e9ecef;
                transition: box-shadow 0.3s;
            }
            .panel-card:hover {
                box-shadow: 0 4px 12px rgba(0,0,0,0.1);
            }
            .panel-name {
                font-size: 16px;
                font-weight: 600;
                color: #495057;
                margin-bottom: 15px;
                display: flex;
                align-items: center;
            }
            .panel-name::before {
                content: "📊";
                margin-right: 8px;
            }
            .periods-container {
                display: flex;
                gap: 10px;
                flex-wrap: wrap;
            }
            .period-badge {
                flex: 1;
                min-width: 100px;
                background: white;
                border-radius: 6px;
                padding: 12px;
                text-align: center;
                border: 2px solid #e9ecef;
            }
            .period-label {
                font-size: 11px;
                color: #6c757d;
                text-transform: uppercase;
                letter-spacing: 0.5px;
                margin-bottom: 5px;
            }
            .period-value {
                font-size: 20px;
                font-weight: 700;
                margin: 5px 0;
            }
            .trend {
                font-size: 12px;
                margin-top: 5px;
            }
            .trend-up {
                color: #27ae60;
            }
            .trend-down {
                color: #e74c3c;
            }
            .trend-neutral {
                color: #95a5a6;
            }
            .footer {
                background-color: #f8f9fa;
                padding: 20px;
                text-align: center;
                color: #6c757d;
                font-size: 12px;
                border-top: 1px solid #dee2e6;
            }
            .error-message {
                background-color: #fff3cd;
                border: 1px solid #ffeaa7;
                border-radius: 6px;
                padding: 12px;
                margin: 10px 0;
                color: #856404;
            }
            @media only screen and (max-width: 600px) {
                .periods-container {
                    flex-direction: column;
                }
                .panels-grid {
                    grid-template-columns: 1fr;
                }
            }
        </style>
        """
        
        # En-tête
        generation_date = datetime.now().strftime("%d/%m/%Y à %H:%M")
        html = f"""
        <!DOCTYPE html>
        <html lang="fr">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Rapport Grafana</title>
            {css}
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>📈 Rapport de Performance Grafana</h1>
                    <p>Généré le {generation_date}</p>
                </div>
        """
        
        # Contenu des dashboards
        if not dashboards