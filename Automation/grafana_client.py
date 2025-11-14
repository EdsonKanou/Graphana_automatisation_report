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