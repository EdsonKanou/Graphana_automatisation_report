#!/usr/bin/env python3
"""
Script pour récupérer les données réelles d'un panel Grafana via l'API /api/ds/query
Utilise un certificat .pem pour l'authentification TLS
"""

import requests
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple


def load_certificate(cert_path: str) -> str:
    """
    Charge le certificat .pem depuis un fichier
    
    Args:
        cert_path: Chemin vers le fichier .pem
        
    Returns:
        Chemin du certificat (pour requests)
    """
    try:
        with open(cert_path, 'r') as f:
            f.read()  # Vérifier que le fichier est lisible
        return cert_path
    except Exception as e:
        raise Exception(f"Erreur lors du chargement du certificat: {e}")


def get_dashboard_config(
    grafana_url: str,
    dashboard_uid: str,
    api_token: str,
    cert_path: str,
    verify_ssl: bool = True
) -> Dict[str, Any]:
    """
    Récupère la configuration complète d'un dashboard Grafana
    
    Args:
        grafana_url: URL de base de Grafana (ex: https://grafana.example.com)
        dashboard_uid: UID du dashboard
        api_token: Token API Grafana
        cert_path: Chemin vers le certificat .pem
        verify_ssl: Vérifier le certificat SSL (True par défaut)
        
    Returns:
        Dictionnaire contenant la configuration du dashboard
    """
    url = f"{grafana_url}/api/dashboards/uid/{dashboard_uid}"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.get(
            url,
            headers=headers,
            cert=cert_path,
            verify=verify_ssl
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erreur lors de la récupération du dashboard: {e}")


def find_panel_by_id(dashboard_config: Dict[str, Any], panel_id: int) -> Optional[Dict[str, Any]]:
    """
    Trouve un panel spécifique dans la configuration du dashboard par son ID
    
    Args:
        dashboard_config: Configuration du dashboard
        panel_id: ID du panel recherché
        
    Returns:
        Configuration du panel ou None si non trouvé
    """
    panels = dashboard_config.get("dashboard", {}).get("panels", [])
    
    for panel in panels:
        if panel.get("id") == panel_id:
            return panel
        
        # Vérifier les sous-panels (pour les rows/groupes)
        if "panels" in panel:
            for sub_panel in panel["panels"]:
                if sub_panel.get("id") == panel_id:
                    return sub_panel
    
    return None


def find_panel_by_title(dashboard_config: Dict[str, Any], panel_title: str) -> Optional[Dict[str, Any]]:
    """
    Trouve un panel spécifique dans la configuration du dashboard par son titre
    
    Args:
        dashboard_config: Configuration du dashboard
        panel_title: Titre du panel recherché
        
    Returns:
        Configuration du panel ou None si non trouvé
    """
    panels = dashboard_config.get("dashboard", {}).get("panels", [])
    
    for panel in panels:
        if panel.get("title") == panel_title:
            return panel
        
        # Vérifier les sous-panels
        if "panels" in panel:
            for sub_panel in panel["panels"]:
                if sub_panel.get("title") == panel_title:
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
    cert_path: str,
    verify_ssl: bool = True
) -> Dict[str, Any]:
    """
    Exécute la requête via l'API /api/ds/query
    
    Args:
        grafana_url: URL de base de Grafana
        payload: Payload construit avec build_query_payload
        api_token: Token API Grafana
        cert_path: Chemin vers le certificat .pem
        verify_ssl: Vérifier le certificat SSL
        
    Returns:
        Résultats de la requête
    """
    url = f"{grafana_url}/api/ds/query"
    
    headers = {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json"
    }
    
    try:
        response = requests.post(
            url,
            headers=headers,
            json=payload,
            cert=cert_path,
            verify=verify_ssl
        )
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


def main():
    """
    Fonction principale - Exemple d'utilisation
    """
    # ===== CONFIGURATION =====
    GRAFANA_URL = "https://grafana.example.com"  # Remplacer par votre URL
    API_TOKEN = "votre_token_api"  # Remplacer par votre token
    CERT_PATH = "/chemin/vers/certificat.pem"  # Remplacer par le chemin
    DASHBOARD_UID = "abc123xyz"  # UID du dashboard
    PANEL_ID = 2  # ID du panel (ou utiliser PANEL_TITLE)
    # PANEL_TITLE = "Mon Panel"  # Alternativement, chercher par titre
    HOURS_BACK = 6  # Récupérer les 6 dernières heures
    VERIFY_SSL = True  # Mettre False si certificat auto-signé
    
    print("=== Récupération des données Grafana ===\n")
    
    try:
        # Étape 1 : Charger le certificat
        print("1. Chargement du certificat...")
        cert = load_certificate(CERT_PATH)
        print(f"   ✓ Certificat chargé: {CERT_PATH}")
        
        # Étape 2 : Récupérer la configuration du dashboard
        print("\n2. Récupération de la configuration du dashboard...")
        dashboard_config = get_dashboard_config(
            GRAFANA_URL,
            DASHBOARD_UID,
            API_TOKEN,
            cert,
            VERIFY_SSL
        )
        print(f"   ✓ Dashboard récupéré: {dashboard_config['dashboard']['title']}")
        
        # Étape 3 : Trouver le panel
        print(f"\n3. Recherche du panel (ID: {PANEL_ID})...")
        panel = find_panel_by_id(dashboard_config, PANEL_ID)
        # Ou par titre: panel = find_panel_by_title(dashboard_config, PANEL_TITLE)
        
        if not panel:
            raise Exception(f"Panel avec ID {PANEL_ID} non trouvé")
        
        print(f"   ✓ Panel trouvé: {panel.get('title', 'Sans titre')}")
        print(f"   Datasource: {panel.get('datasource', {})}")
        print(f"   Nombre de targets: {len(panel.get('targets', []))}")
        
        # Étape 4 : Générer le range de temps
        print(f"\n4. Génération du range de temps ({HOURS_BACK}h)...")
        from_time, to_time = get_time_range(HOURS_BACK)
        print(f"   De: {from_time}")
        print(f"   À:  {to_time}")
        
        # Étape 5 : Construire le payload
        print("\n5. Construction du payload de requête...")
        payload = build_query_payload(panel, from_time, to_time)
        print(f"   ✓ Payload construit avec {len(payload['queries'])} requête(s)")
        
        # Afficher le payload pour debug
        print("\n   Payload (aperçu):")
        print(f"   {json.dumps(payload, indent=2)[:500]}...")
        
        # Étape 6 : Exécuter la requête
        print("\n6. Exécution de la requête via /api/ds/query...")
        query_results = execute_panel_query(
            GRAFANA_URL,
            payload,
            API_TOKEN,
            cert,
            VERIFY_SSL
        )
        print("   ✓ Requête exécutée avec succès")
        
        # Étape 7 : Parser les résultats
        print("\n7. Parsing des résultats...")
        parsed_results = parse_query_results(query_results)
        print(f"   ✓ {len(parsed_results)} série(s) de données récupérée(s)")
        
        # Étape 8 : Afficher les résultats
        print("\n8. Résultats:")
        print(format_results_readable(parsed_results))
        
        # Sauvegarder les résultats bruts (optionnel)
        with open("grafana_results.json", "w") as f:
            json.dump({
                "raw_results": query_results,
                "parsed_results": parsed_results
            }, f, indent=2)
        print("\n✓ Résultats sauvegardés dans grafana_results.json")
        
    except Exception as e:
        print(f"\n✗ Erreur: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()