#!/usr/bin/env python3
"""
Client API Grafana pour récupérer les données d'un dashboard PostgreSQL
"""

import requests
import json
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
import urllib3

# Désactiver les warnings SSL (à cause du -k dans curl)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class GrafanaClient:
    """Client pour interroger l'API Grafana"""
    
    def __init__(self, base_url: str, api_key: str):
        """
        Initialise le client Grafana
        
        Args:
            base_url: URL de base Grafana (ex: https://orchestrator-dashboard.group.echonet)
            api_key: Token d'API Grafana
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }
    
    def query_datasource(
        self, 
        datasource_uid: str,
        datasource_type: str,
        raw_sql: str,
        time_from: str = "now-1h",
        time_to: str = "now",
        ref_id: str = "A"
    ) -> Dict[str, Any]:
        """
        Exécute une requête SQL via l'API Grafana
        
        Args:
            datasource_uid: UID de la datasource (ex: 'cebe2fax1rieob')
            datasource_type: Type de datasource (ex: 'grafana-postgresql-datasource')
            raw_sql: Requête SQL à exécuter
            time_from: Début de la période (ex: 'now-1h', '2025-11-09T19:00:00Z')
            time_to: Fin de la période (ex: 'now', '2025-11-09T20:00:00Z')
            ref_id: Identifiant de référence de la requête
        
        Returns:
            Réponse JSON complète de l'API
        """
        url = f"{self.base_url}/api/ds/query"
        
        payload = {
            "queries": [{
                "refId": ref_id,
                "datasource": {
                    "type": datasource_type,
                    "uid": datasource_uid
                },
                "rawSql": raw_sql,
                "format": "table"
            }],
            "from": time_from,
            "to": time_to
        }
        
        try:
            response = requests.post(
                url,
                headers=self.headers,
                json=payload,
                verify=False  # Équivalent du -k dans curl
            )
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur lors de la requête: {e}")
            if hasattr(e.response, 'text'):
                print(f"Détails: {e.response.text}")
            raise
    
    def extract_value(
        self, 
        response: Dict[str, Any], 
        ref_id: str = "A",
        row: int = 0,
        column: int = 0
    ) -> Optional[Any]:
        """
        Extrait une valeur spécifique de la réponse Grafana
        
        Args:
            response: Réponse JSON de l'API Grafana
            ref_id: Identifiant de référence de la requête (défaut: "A")
            row: Index de la ligne à extraire (défaut: 0 = première ligne)
            column: Index de la colonne à extraire (défaut: 0 = première colonne)
        
        Returns:
            La valeur extraite ou None si non trouvée
        """
        try:
            # Navigation dans la structure JSON de Grafana
            results = response.get('results', {})
            ref_data = results.get(ref_id, {})
            
            # Vérifier s'il y a une erreur
            if 'error' in ref_data:
                print(f"❌ Erreur dans la réponse: {ref_data['error']}")
                return None
            
            frames = ref_data.get('frames', [])
            if not frames:
                print("⚠️  Aucune frame trouvée dans la réponse")
                return None
            
            # Extraire les données
            data = frames[0].get('data', {})
            values = data.get('values', [])
            
            if not values or len(values) <= column:
                print(f"⚠️  Aucune valeur trouvée à la colonne {column}")
                return None
            
            column_values = values[column]
            if not column_values or len(column_values) <= row:
                print(f"⚠️  Aucune valeur trouvée à la ligne {row}")
                return None
            
            return column_values[row]
        
        except (KeyError, IndexError, TypeError) as e:
            print(f"❌ Erreur lors de l'extraction: {e}")
            return None
    
    def extract_all_values(
        self, 
        response: Dict[str, Any], 
        ref_id: str = "A"
    ) -> Optional[List[List[Any]]]:
        """
        Extrait toutes les valeurs sous forme de tableau
        
        Args:
            response: Réponse JSON de l'API Grafana
            ref_id: Identifiant de référence de la requête
        
        Returns:
            Liste de listes (lignes x colonnes) ou None si erreur
        """
        try:
            results = response.get('results', {})
            ref_data = results.get(ref_id, {})
            
            if 'error' in ref_data:
                print(f"❌ Erreur dans la réponse: {ref_data['error']}")
                return None
            
            frames = ref_data.get('frames', [])
            if not frames:
                return None
            
            data = frames[0].get('data', {})
            values = data.get('values', [])
            
            if not values:
                return None
            
            # Transposer: de colonnes à lignes
            num_rows = len(values[0]) if values else 0
            rows = []
            for i in range(num_rows):
                row = [col[i] if i < len(col) else None for col in values]
                rows.append(row)
            
            return rows
        
        except Exception as e:
            print(f"❌ Erreur lors de l'extraction: {e}")
            return None
    
    def get_column_names(
        self, 
        response: Dict[str, Any], 
        ref_id: str = "A"
    ) -> Optional[List[str]]:
        """
        Extrait les noms des colonnes
        
        Args:
            response: Réponse JSON de l'API Grafana
            ref_id: Identifiant de référence de la requête
        
        Returns:
            Liste des noms de colonnes ou None si erreur
        """
        try:
            results = response.get('results', {})
            ref_data = results.get(ref_id, {})
            frames = ref_data.get('frames', [])
            
            if not frames:
                return None
            
            schema = frames[0].get('schema', {})
            fields = schema.get('fields', [])
            
            return [field.get('name', f'column_{i}') for i, field in enumerate(fields)]
        
        except Exception as e:
            print(f"❌ Erreur lors de l'extraction des noms: {e}")
            return None


def main():
    """Exemple d'utilisation du client Grafana"""
    
    # Configuration
    GRAFANA_URL = "https://orchestrator-dashboard.group.echonet"
    API_KEY = "xxxx"
    DATASOURCE_UID = "cebe2fax1rieob"
    DATASOURCE_TYPE = "grafana-postgresql-datasource"
    
    # Requête SQL (corrigée avec les guillemets)
    SQL_QUERY = """
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
    AND dem.create_date >= $_timeFrom() 
    AND dem.end_date <= $_timeTo() 
    AND dem.requestor_uid LIKE 'ServiceId%'
    """
    
    # Initialiser le client
    print("🚀 Initialisation du client Grafana...")
    client = GrafanaClient(GRAFANA_URL, API_KEY)
    
    # Exécuter la requête
    print("📊 Exécution de la requête SQL...")
    response = client.query_datasource(
        datasource_uid=DATASOURCE_UID,
        datasource_type=DATASOURCE_TYPE,
        raw_sql=SQL_QUERY,
        time_from="now-1h",
        time_to="now"
    )
    
    # Afficher la réponse brute (optionnel)
    print("\n📋 Réponse complète:")
    print(json.dumps(response, indent=2))
    
    # Extraire la valeur unique (pourcentage de succès)
    print("\n✅ Extraction de la valeur...")
    success_rate = client.extract_value(response)
    
    if success_rate is not None:
        print(f"🎯 Taux de succès: {success_rate:.2f}%")
    else:
        print("❌ Impossible d'extraire la valeur")
    
    # Exemple: extraire toutes les valeurs (si plusieurs lignes)
    print("\n📊 Extraction de toutes les valeurs...")
    all_values = client.extract_all_values(response)
    
    if all_values:
        print(f"Nombre de lignes: {len(all_values)}")
        for i, row in enumerate(all_values):
            print(f"Ligne {i}: {row}")
    
    # Exemple: obtenir les noms de colonnes
    print("\n📝 Noms des colonnes:")
    column_names = client.get_column_names(response)
    if column_names:
        print(column_names)


if __name__ == "__main__":
    main()
    
    
##FINNNN

#!/usr/bin/env python3
"""
Script simple pour tester la requête Grafana
"""

import requests
import json
import urllib3

# Désactiver les warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def query_grafana():
    """Exécute la requête Grafana et affiche le résultat"""
    
    # Configuration
    url = "https://orchestrator-dashboard.group.echonet/api/ds/query"
    api_key = "xxx"
    
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    # Requête SQL corrigée
    sql_query = """
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
    AND dem.create_date >= $_timeFrom() 
    AND dem.end_date <= $_timeTo() 
    AND dem.requestor_uid LIKE 'ServiceId%'
    """
    
    # Payload
    payload = {
        "queries": [{
            "refId": "A",
            "datasource": {
                "type": "grafana-postgresql-datasource",
                "uid": "cebe2fax1rieob"
            },
            "rawSql": sql_query,
            "format": "table"
        }],
        "from": "now-1h",
        "to": "now"
    }
    
    # Exécuter la requête
    print("🚀 Exécution de la requête...")
    try:
        response = requests.post(url, headers=headers, json=payload, verify=False)
        response.raise_for_status()
        
        data = response.json()
        
        # Afficher la réponse complète
        print("\n📋 Réponse complète:")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        # Extraire la valeur
        try:
            value = data['results']['A']['frames'][0]['data']['values'][0][0]
            print(f"\n✅ Taux de succès: {value:.2f}%")
        except (KeyError, IndexError, TypeError) as e:
            print(f"\n⚠️ Impossible d'extraire la valeur: {e}")
            
            # Vérifier s'il y a une erreur SQL
            if 'error' in data.get('results', {}).get('A', {}):
                print(f"❌ Erreur SQL: {data['results']['A']['error']}")
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur de requête: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Détails: {e.response.text}")


# Lancer le test
if __name__ == "__main__":
    query_grafana()