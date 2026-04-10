"""
Module de connexion et récupération des données depuis Grafana
Gère l'authentification, les appels API et l'extraction des métriques
"""

import requests
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GrafanaClient:
    """Client pour interagir avec l'API Grafana"""
    
    def __init__(self, base_url: str, api_key: str):
        """
        Initialise le client Grafana
        
        Args:
            base_url: URL de base de Grafana (ex: https://grafana.company.com)
            api_key: Token API Grafana (créé dans Settings > API Keys)
        """
        self.base_url = base_url.rstrip('/')
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        
    def get_dashboard(self, dashboard_uid: str) -> Dict[str, Any]:
        """
        Récupère les informations complètes d'un dashboard
        
        Args:
            dashboard_uid: UID unique du dashboard
            
        Returns:
            Dictionnaire contenant la configuration complète du dashboard
        """
        url = f"{self.base_url}/api/dashboards/uid/{dashboard_uid}"
        
        try:
            response = self.session.get(url)
            response.raise_for_status()
            logger.info(f"Dashboard {dashboard_uid} récupéré avec succès")
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de la récupération du dashboard: {e}")
            raise
    
    def get_dashboard_variables(self, dashboard_data: Dict) -> Dict[str, Any]:
        """
        Extrait les variables/filtres du dashboard
        
        Args:
            dashboard_data: Données du dashboard récupérées via get_dashboard()
            
        Returns:
            Dictionnaire des variables et leurs valeurs par défaut
        """
        variables = {}
        templating = dashboard_data.get('dashboard', {}).get('templating', {})
        
        for var in templating.get('list', []):
            var_name = var.get('name')
            var_type = var.get('type')
            
            # Récupère la valeur courante ou par défaut
            if var_type == 'query':
                # Pour les variables de type query, on prend la première valeur
                current = var.get('current', {})
                variables[var_name] = current.get('value', '')
            elif var_type == 'custom':
                # Pour les variables custom, on prend la valeur sélectionnée
                current = var.get('current', {})
                variables[var_name] = current.get('value', '')
            elif var_type == 'interval':
                # Pour les intervalles
                variables[var_name] = var.get('current', {}).get('value', '5m')
                
        logger.info(f"Variables extraites: {variables}")
        return variables
    
    def query_datasource(self, 
                        panel_id: int, 
                        dashboard_data: Dict,
                        time_from: str = 'now-24h',
                        time_to: str = 'now',
                        variables: Optional[Dict] = None) -> List[Dict]:
        """
        Exécute les requêtes d'un panel spécifique du dashboard
        
        Args:
            panel_id: ID du panel à interroger
            dashboard_data: Données du dashboard
            time_from: Début de la période (format Grafana: now-24h, now-7d, etc.)
            time_to: Fin de la période
            variables: Dictionnaire des variables à appliquer
            
        Returns:
            Liste des résultats de requête
        """
        # Trouve le panel dans le dashboard
        panel = self._find_panel_by_id(dashboard_data, panel_id)
        if not panel:
            logger.warning(f"Panel {panel_id} non trouvé")
            return []
        
        # Récupère la datasource du panel
        datasource_uid = panel.get('datasource', {}).get('uid', '')
        
        # Prépare les targets (requêtes) du panel
        targets = panel.get('targets', [])
        
        results = []
        for target in targets:
            # Remplace les variables dans la requête
            query = self._replace_variables(target.get('expr', ''), variables or {})
            
            # Effectue la requête via l'API datasource proxy
            result = self._execute_query(datasource_uid, query, time_from, time_to)
            if result:
                results.append({
                    'panel_title': panel.get('title', f'Panel {panel_id}'),
                    'query': query,
                    'data': result
                })
        
        return results
    
    def get_all_panels_data(self, 
                           dashboard_uid: str,
                           time_from: str = 'now-24h',
                           time_to: str = 'now') -> List[Dict]:
        """
        Récupère les données de tous les panels du dashboard
        
        Args:
            dashboard_uid: UID du dashboard
            time_from: Début de la période
            time_to: Fin de la période
            
        Returns:
            Liste de dictionnaires contenant les données de chaque panel
        """
        # Récupère le dashboard complet
        dashboard_data = self.get_dashboard(dashboard_uid)
        
        # Extrait les variables
        variables = self.get_dashboard_variables(dashboard_data)
        
        # Récupère tous les panels
        panels = self._get_all_panels(dashboard_data)
        
        all_data = []
        for panel in panels:
            panel_id = panel.get('id')
            panel_type = panel.get('type', '')
            
            # Ignore les panels non-data (text, row, etc.)
            if panel_type in ['text', 'row']:
                continue
            
            try:
                panel_data = self.query_datasource(
                    panel_id, 
                    dashboard_data, 
                    time_from, 
                    time_to, 
                    variables
                )
                all_data.extend(panel_data)
            except Exception as e:
                logger.error(f"Erreur lors de la récupération du panel {panel_id}: {e}")
                continue
        
        logger.info(f"Données récupérées pour {len(all_data)} panels")
        return all_data
    
    def _find_panel_by_id(self, dashboard_data: Dict, panel_id: int) -> Optional[Dict]:
        """Trouve un panel par son ID dans le dashboard"""
        panels = self._get_all_panels(dashboard_data)
        for panel in panels:
            if panel.get('id') == panel_id:
                return panel
        return None
    
    def _get_all_panels(self, dashboard_data: Dict) -> List[Dict]:
        """Récupère tous les panels du dashboard (incluant ceux dans les rows)"""
        dashboard = dashboard_data.get('dashboard', {})
        panels = dashboard.get('panels', [])
        
        all_panels = []
        for panel in panels:
            # Si c'est une row, récupère les panels à l'intérieur
            if panel.get('type') == 'row':
                all_panels.extend(panel.get('panels', []))
            else:
                all_panels.append(panel)
        
        return all_panels
    
    def _replace_variables(self, query: str, variables: Dict[str, Any]) -> str:
        """Remplace les variables Grafana ($var) dans la requête"""
        for var_name, var_value in variables.items():
            # Remplace $var_name et ${var_name}
            query = query.replace(f'${var_name}', str(var_value))
            query = query.replace(f'${{{var_name}}}', str(var_value))
        
        return query
    
    def _execute_query(self, 
                      datasource_uid: str, 
                      query: str,
                      time_from: str,
                      time_to: str) -> Optional[Dict]:
        """
        Exécute une requête via l'API datasource de Grafana
        
        Note: Cette méthode est adaptée pour Prometheus. 
        Pour d'autres datasources (InfluxDB, etc.), adapter le format de requête.
        """
        url = f"{self.base_url}/api/ds/query"
        
        # Convertit les timestamps relatifs en timestamps absolus
        now = datetime.now()
        from_ts, to_ts = self._parse_time_range(time_from, time_to, now)
        
        payload = {
            "queries": [{
                "datasource": {"uid": datasource_uid},
                "expr": query,
                "refId": "A",
                "instant": False,
                "range": True,
                "format": "time_series"
            }],
            "from": str(int(from_ts.timestamp() * 1000)),
            "to": str(int(to_ts.timestamp() * 1000))
        }
        
        try:
            response = self.session.post(url, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Erreur lors de l'exécution de la requête: {e}")
            return None
    
    def _parse_time_range(self, 
                         time_from: str, 
                         time_to: str, 
                         reference: datetime) -> tuple:
        """Parse les timestamps relatifs Grafana (now-24h, etc.)"""
        def parse_relative_time(time_str: str, ref: datetime) -> datetime:
            if time_str == 'now':
                return ref
            
            # Parse format "now-24h", "now-7d", etc.
            if time_str.startswith('now-'):
                duration_str = time_str[4:]  # Enlève "now-"
                
                # Extrait le nombre et l'unité
                import re
                match = re.match(r'(\d+)([smhdwMy])', duration_str)
                if match:
                    value = int(match.group(1))
                    unit = match.group(2)
                    
                    if unit == 's':
                        return ref - timedelta(seconds=value)
                    elif unit == 'm':
                        return ref - timedelta(minutes=value)
                    elif unit == 'h':
                        return ref - timedelta(hours=value)
                    elif unit == 'd':
                        return ref - timedelta(days=value)
                    elif unit == 'w':
                        return ref - timedelta(weeks=value)
                    elif unit == 'M':
                        return ref - timedelta(days=value * 30)
                    elif unit == 'y':
                        return ref - timedelta(days=value * 365)
            
            return ref
        
        from_dt = parse_relative_time(time_from, reference)
        to_dt = parse_relative_time(time_to, reference)
        
        return from_dt, to_dt