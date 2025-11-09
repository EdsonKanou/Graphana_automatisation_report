#!/usr/bin/env python3
"""
Script pour récupérer les données réelles d'un panel Grafana via l'API /api/ds/query
Utilise un certificat .pem pour vérifier le serveur SSL
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple


def get_dashboard_config(
    grafana_url: str,
    dashboard_uid: str,
    api_token: str,
    cert_path: str
) -> Dict[str, Any]:
    """
    Récupère la configuration complète d'un dashboard Grafana
    
    Args:
        grafana_url: URL de base de Grafana (ex: https://grafana.example.com)
        dashboard_uid: UID du dashboard
        api_token: Token API Grafana
        cert_path: Chemin vers le certificat .pem pour vérification SSL
        
    Returns:
        Dictionnaire contenant la configuration du dashboard
    """
    url = f"{grafana_url}/api/dashboards/uid/{dashboard_uid}"
    
    headers = {
        'Authorization': f'Bearer {api_token}'
    }
    
    try:
        response = requests.get(url, headers=headers, verify=cert_path)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erreur lors de la récupération du dashboard: {e}")


def get_panel_from_dashboard(
    grafana_url: str,
    dashboard_uid: str,
    api_token: str,
    cert_path: str,
    panel_id: Optional[int] = None,
    panel_title: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """
    Récupère un panel spécifique d'un dashboard par son ID ou son titre
    
    Args:
        grafana_url: URL de base de Grafana
        dashboard_uid: UID du dashboard
        api_token: Token API Grafana
        cert_path: Chemin vers le certificat .pem
        panel_id: ID du panel recherché (optionnel)
        panel_title: Titre du panel recherché (optionnel)
        
    Returns:
        Configuration du panel ou None si non trouvé
    """
    if panel_id is None and panel_title is None:
        raise ValueError("Vous devez fournir soit panel_id soit panel_title")
    
    dashboard_json = get_dashboard_config(grafana_url, dashboard_uid, api_token, cert_path)
    panels = dashboard_json['dashboard']['panels']
    
    for panel in panels:
        if (panel_id is not None and panel['id'] == panel_id) or \
           (panel_title is not None and panel['title'] == panel_title):
            return panel
        
        # Vérifier les sous-panels (pour les rows/groupes)
        if 'panels' in panel:
            for sub_panel in panel['panels']:
                if (panel_id is not None and sub_panel['id'] == panel_id) or \
                   (panel_title is not None and sub_panel['title'] == panel_title):
                    return sub_panel
    
    return None


def get_time_range(hours_back: int = 6) -> Tuple[str, str]:
    """
    Génère un range de temps au format Grafana
    
    Args:
        hours_back: Nombre d'heures en arrière (par défaut 6h)
        
    Returns:
        Tuple (from_time, to_time) au format ISO 8601
    """
    to_time = datetime.utcnow()
    from_time = to_time - timedelta(hours=hours_back)
    
    return (
        from_time.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        to_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    )


def build_query_payload(
    panel_config: Dict[str, Any],
    from_time: str,
    to_time: str,
    max_data_points: int = 1000
) -> Dict[str, Any]:
    """
    Construit le payload pour l'API /api/ds/query à partir de la config du panel
    
    Args:
        panel_config: Configuration du panel
        from_time: Timestamp de début (ISO 8601)
        to_time: Timestamp de fin (ISO 8601)
        max_data_points: Nombre maximum de points de données
        
    Returns:
        Payload formaté pour l'API
    """
    # Extraire les targets (requêtes) du panel
    targets = panel_config.get("targets", [])
    
    if not targets:
        raise Exception("Aucune requête (target) trouvée dans le panel")
    
    # Extraire la datasource UID
    datasource = panel_config.get("datasource", {})
    if isinstance(datasource, dict):
        datasource_uid = datasource.get("uid")
    else:
        datasource_uid = datasource
    
    if not datasource_uid:
        raise Exception("Datasource UID non trouvée dans le panel")
    
    # Construire les queries pour l'API
    queries = []
    for target in targets:
        if target.get("hide"):  # Ignorer les requêtes masquées
            continue
            
        query = {
            "refId": target.get("refId", "A"),
            "datasource": {
                "type": target.get("datasource", {}).get("type", "prometheus"),
                "uid": datasource_uid
            },
            "expr": target.get("expr", ""),  # Pour Prometheus
            "query": target.get("query", ""),  # Pour d'autres datasources
            "legendFormat": target.get("legendFormat", ""),
            "interval": target.get("interval", ""),
            "intervalMs": target.get("intervalMs", 15000),
            "maxDataPoints": max_data_points
        }
        
        # Copier tous les autres champs spécifiques
        for key, value in target.items():
            if key not in query:
                query[key] = value
        
        queries.append(query)
    
    # Construire le payload final
    payload = {
        "queries": queries,
        "from": from_time,
        "to": to_time
    }
    
    return payload


def execute_panel_query(
    grafana_url: str,
    payload: Dict[str, Any],
    api_token: str,
    cert_path: str
) -> Dict[str, Any]:
    """
    Exécute la requête via l'API /api/ds/query
    
    Args:
        grafana_url: URL de base de Grafana
        payload: Payload construit avec build_query_payload
        api_token: Token API Grafana
        cert_path: Chemin vers le certificat .pem
        
    Returns:
        Résultats de la requête
    """
    url = f"{grafana_url}/api/ds/query"
    
    headers = {
        'Authorization': f'Bearer {api_token}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, verify=cert_path)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erreur lors de l'exécution de la requête: {e}")


def parse_query_results(query_results: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse les résultats de l'API /api/ds/query pour extraire les données
    
    Args:
        query_results: Résultats bruts de l'API
        
    Returns:
        Liste de dictionnaires contenant les séries et leurs valeurs
    """
    results = []
    
    # Les résultats sont dans results.{refId}
    for ref_id, data in query_results.get("results", {}).items():
        frames = data.get("frames", [])
        
        for frame in frames:
            # Décoder le frame si nécessaire (base64)
            if isinstance(frame, str):
                import base64
                frame_data = json.loads(base64.b64decode(frame))
            else:
                frame_data = frame
            
            # Extraire le nom de la série
            series_name = frame_data.get("schema", {}).get("name", "Unknown")
            
            # Extraire les champs (colonnes)
            fields = frame_data.get("schema", {}).get("fields", [])
            data_values = frame_data.get("data", {}).get("values", [])
            
            # Construire les séries
            series_data = {
                "refId": ref_id,
                "name": series_name,
                "fields": []
            }
            
            for idx, field in enumerate(fields):
                field_name = field.get("name", f"field_{idx}")
                field_type = field.get("type", "unknown")
                values = data_values[idx] if idx < len(data_values) else []
                
                series_data["fields"].append({
                    "name": field_name,
                    "type": field_type,
                    "values": values
                })
            
            results.append(series_data)
    
    return results


def format_results_readable(parsed_results: List[Dict[str, Any]]) -> str:
    """
    Formate les résultats de manière lisible
    
    Args:
        parsed_results: Résultats parsés
        
    Returns:
        String formaté pour affichage
    """
    output = []
    
    for series in parsed_results:
        output.append(f"\n=== Série: {series['name']} (RefID: {series['refId']}) ===")
        
        fields = series.get("fields", [])
        if not fields:
            output.append("  Aucune donnée")
            continue
        
        # Afficher les noms des champs
        field_names = [f["name"] for f in fields]
        output.append(f"  Champs: {', '.join(field_names)}")
        
        # Afficher quelques valeurs
        if fields and fields[0]["values"]:
            num_values = len(fields[0]["values"])
            output.append(f"  Nombre de points: {num_values}")
            
            # Afficher les 5 premières valeurs
            max_display = min(5, num_values)
            output.append(f"  Premières valeurs:")
            
            for i in range(max_display):
                row_values = []
                for field in fields:
                    value = field["values"][i] if i < len(field["values"]) else "N/A"
                    
                    # Formater les timestamps
                    if field["type"] == "time" and isinstance(value, (int, float)):
                        value = datetime.fromtimestamp(value / 1000).strftime("%Y-%m-%d %H:%M:%S")
                    
                    row_values.append(f"{field['name']}={value}")
                
                output.append(f"    [{i}] {', '.join(row_values)}")
            
            if num_values > max_display:
                output.append(f"    ... et {num_values - max_display} autres points")
    
    return "\n".join(output)


def get_panel_data(
    grafana_url: str,
    dashboard_uid: str,
    api_token: str,
    cert_path: str,
    panel_id: Optional[int] = None,
    panel_title: Optional[str] = None,
    hours_back: int = 6,
    max_data_points: int = 1000
) -> List[Dict[str, Any]]:
    """
    Fonction principale pour récupérer les données d'un panel
    
    Args:
        grafana_url: URL de base de Grafana
        dashboard_uid: UID du dashboard
        api_token: Token API Grafana
        cert_path: Chemin vers le certificat .pem
        panel_id: ID du panel (optionnel)
        panel_title: Titre du panel (optionnel)
        hours_back: Nombre d'heures de données à récupérer
        max_data_points: Nombre maximum de points
        
    Returns:
        Liste des données parsées
    """
    # Récupérer le panel
    panel = get_panel_from_dashboard(
        grafana_url,
        dashboard_uid,
        api_token,
        cert_path,
        panel_id,
        panel_title
    )
    
    if not panel:
        raise Exception(f"Panel non trouvé (ID: {panel_id}, Title: {panel_title})")
    
    # Générer le range de temps
    from_time, to_time = get_time_range(hours_back)
    
    # Construire le payload
    payload = build_query_payload(panel, from_time, to_time, max_data_points)
    
    # Exécuter la requête
    query_results = execute_panel_query(grafana_url, payload, api_token, cert_path)
    
    # Parser et retourner les résultats
    return parse_query_results(query_results)


def main():
    """
    Fonction principale - Exemple d'utilisation
    """
    # ===== CONFIGURATION =====
    GRAFANA_URL = "https://orchestrator-dashboard.group.echonet"  # Votre URL
    API_TOKEN = "votre_token_api"  # Votre token
    CERT_PATH = "/chemin/vers/certificat.pem"  # Chemin vers le certificat
    DASHBOARD_UID = "debe5b0zgerr4b"  # UID du dashboard
    
    # Choisir l'une des deux options:
    PANEL_ID = 2  # Option 1: Chercher par ID
    # PANEL_TITLE = "Mon Panel"  # Option 2: Chercher par titre
    
    HOURS_BACK = 6  # Récupérer les 6 dernières heures
    
    print("=== Récupération des données Grafana ===\n")
    
    try:
        # Méthode simple avec la fonction tout-en-un
        print(f"Récupération du panel (ID: {PANEL_ID}) du dashboard {DASHBOARD_UID}...")
        
        parsed_results = get_panel_data(
            grafana_url=GRAFANA_URL,
            dashboard_uid=DASHBOARD_UID,
            api_token=API_TOKEN,
            cert_path=CERT_PATH,
            panel_id=PANEL_ID,
            # panel_title=PANEL_TITLE,  # Ou utiliser le titre
            hours_back=HOURS_BACK
        )
        
        print(f"✓ {len(parsed_results)} série(s) de données récupérée(s)\n")
        
        # Afficher les résultats
        print(format_results_readable(parsed_results))
        
        # Sauvegarder les résultats
        with open("grafana_results.json", "w") as f:
            json.dump(parsed_results, f, indent=2)
        print("\n✓ Résultats sauvegardés dans grafana_results.json")
        
        # Exemple: Accéder aux données
        print("\n=== Exemple d'utilisation des données ===")
        for series in parsed_results:
            print(f"\nSérie: {series['name']}")
            for field in series['fields']:
                print(f"  - {field['name']} ({field['type']}): {len(field['values'])} valeurs")
                if field['values']:
                    print(f"    Première valeur: {field['values'][0]}")
                    print(f"    Dernière valeur: {field['values'][-1]}")
        
    except Exception as e:
        print(f"\n✗ Erreur: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 
    
    
curl --cacert /path/to/server_cert.pem \
  -H "Authorization: Bearer <API_KEY>" \
  -H "Content-Type: application/json" \
  -X POST \
  https://orchestrator-dashboard.group.echonet/api/datasources/proxy/4/query \
  -d '{
        "queries": [
          {
            "refId": "A",
            "format": "table",
            "rawSql": "SELECT count(*) FROM logs WHERE level = '\''error'\''",
            "datasourceId": 4
          }
        ]
      }'

import requests

def query_postgres_via_grafana(grafana_url, api_key, datasource_id, raw_sql, verify_cert_path=None):
    """
    Exécute une requête SQL via Grafana proxy PostgreSQL.
    - verify_cert_path : chemin vers le certificat serveur à utiliser pour vérifier SSL.
    """
    url = f"{grafana_url.rstrip('/')}/api/datasources/proxy/{datasource_id}/query"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    payload = {
        "queries": [
            {
                "refId": "A",
                "format": "table",
                "rawSql": raw_sql,
                "datasourceId": datasource_id
            }
        ]
    }

    r = requests.post(url, headers=headers, json=payload, verify=verify_cert_path)
    r.raise_for_status()
    return r.json()




grafana_url = "https://orchestrator-dashboard.group.echonet"
api_key = "<API_KEY>"
datasource_id = 4
raw_sql = "SELECT count(*) FROM logs WHERE level = 'error'"
verify_cert_path = "/path/to/server_cert.pem"  # chemin vers certificat serveur

data = query_postgres_via_grafana(grafana_url, api_key, datasource_id, raw_sql, verify_cert_path)
print(data)

# Pour extraire la valeur réelle
frames = data["results"]["A"]["frames"]
if frames:
    values = frames[0]["data"]["values"]
    print("Résultat =", values[0][0])

