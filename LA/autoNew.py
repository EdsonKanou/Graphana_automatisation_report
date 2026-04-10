#!/usr/bin/env python3
"""
Script simple pour tester la requête Grafana
"""

import requests
import json
import urllib3
from datetime import datetime, timedelta

# Désactiver les warnings SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def query_grafana():
    """Exécute la requête Grafana et affiche le résultat"""
    
    # Configuration
    url = "https://orchestrator-dashboard.group.echonet/api/ds/query"
    api_key = "glsa_EghQphwQdMxZoVQca5aLLRXAQRRNJQ61_9t087d84"
    
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }
    
    # Calculer les timestamps (dernière heure)
    time_to = datetime.utcnow()
    time_from = time_to - timedelta(hours=1)
    
    # Formater en ISO 8601
    time_from_str = time_from.strftime('%Y-%m-%dT%H:%M:%SZ')
    time_to_str = time_to.strftime('%Y-%m-%dT%H:%M:%SZ')
    
    print(f"⏰ Période: {time_from_str} → {time_to_str}")
    
    # Requête SQL corrigée (sans les macros Grafana)
    sql_query = f"""
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
    AND dem.create_date >= '{time_from_str}' 
    AND dem.end_date <= '{time_to_str}' 
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