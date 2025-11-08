import requests
import pandas as pd
import time


# ---------- Fonctions de base ----------
def get_dash(grafana_url: str, api_key: str, uid: str) -> dict:
    """Récupère le JSON complet d’un dashboard par UID."""
    url = f"{grafana_url.rstrip('/')}/api/dashboards/uid/{uid}"
    headers = {"Authorization": f"Bearer {api_key}"}
    r = requests.get(url, headers=headers)
    r.raise_for_status()
    return r.json()


def get_panel_from_dashboard(grafana_url: str, api_key: str, uid: str,
                             panel_id: int = None, panel_title: str = None) -> dict:
    """Retourne un panel spécifique par ID ou titre."""
    dashboard_json = get_dash(grafana_url, api_key, uid)
    panels = dashboard_json['dashboard']['panels']

    for panel in panels:
        if (panel_id is not None and panel['id'] == panel_id) or \
           (panel_title is not None and panel['title'] == panel_title):
            return panel

    raise ValueError("Panel non trouvé dans le dashboard.")


# ---------- Exécution de la requête ----------
def query_datasource(grafana_url: str, api_key: str, queries: list) -> dict:
    """Appelle /api/ds/query avec une liste de queries."""
    url = f"{grafana_url.rstrip('/')}/api/ds/query"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {"queries": queries}

    r = requests.post(url, headers=headers, json=payload)
    r.raise_for_status()
    return r.json()


# ---------- Parsing des résultats ----------
def parse_grafana_results(response_json: dict) -> pd.DataFrame:
    """Convertit la réponse JSON de Grafana en DataFrame pandas."""
    results = response_json.get("results", {})
    if not results:
        return pd.DataFrame()

    dfs = []
    for ref, res in results.items():
        frames = res.get("frames", [])
        for frame in frames:
            fields = frame.get("fields", [])
            frame_data = {}
            for f in fields:
                name = f.get("name", "col")
                vals = f.get("values", {}).get("buffer", f.get("values", []))
                frame_data[name] = vals
            if frame_data:
                dfs.append(pd.DataFrame(frame_data))

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


# ---------- Fonction principale ----------
def get_panel_data(grafana_url: str, api_key: str, uid: str,
                   panel_id: int = None, panel_title: str = None,
                   from_s: int = None, to_s: int = None) -> pd.DataFrame:
    """
    Récupère les données réelles d’un panel via /api/ds/query.
    - grafana_url : URL de Grafana (ex: https://grafana.mondomaine.com)
    - api_key : clé API Grafana
    - uid : UID du dashboard
    - panel_id / panel_title : identifiant du panel à récupérer
    - from_s, to_s : timestamps Unix (secondes) optionnels
    """

    # 1️⃣ Trouver le panel
    panel = get_panel_from_dashboard(grafana_url, api_key, uid, panel_id, panel_title)

    # 2️⃣ Récupérer la datasource
    ds = panel.get('datasource', {})
    if not ds or 'uid' not in ds:
        raise ValueError("Datasource introuvable dans le panel.")
    datasource_uid = ds['uid']
    datasource_type = ds.get('type', 'prometheus')

    # 3️⃣ Définir la période temporelle (défaut = dernière heure)
    now_ms = int(time.time() * 1000)
    from_ms = int((time.time() - 3600) * 1000) if from_s is None else from_s * 1000
    to_ms = now_ms if to_s is None else to_s * 1000

    # 4️⃣ Construire les queries
    queries = []
    for target in panel.get('targets', []):
        q = {
            "refId": target.get('refId', 'A'),
            "datasource": {"uid": datasource_uid, "type": datasource_type},
            "intervalMs": 15000,
            "maxDataPoints": 1000,
            "range": {"from": from_ms, "to": to_ms},
        }
        # type de datasource
        if 'expr' in target:
            q['expr'] = target['expr']  # Prometheus
        elif 'rawSql' in target:
            q['rawSql'] = target['rawSql']  # SQL
        queries.append(q)

    # 5️⃣ Appeler Grafana
    data = query_datasource(grafana_url, api_key, queries)

    # 6️⃣ Parser la réponse
    return parse_grafana_results(data)

grafana_url = "https://grafana.mondomaine.com"
api_key = "eyJrIjo..."  # clé API Grafana
dashboard_uid = "abcd1234"

# Récupérer les données d’un panel
df = get_panel_data(
    grafana_url=grafana_url,
    api_key=api_key,
    uid=dashboard_uid,
    panel_title="CPU Usage"   # ou panel_id=2
)

print(df.head())
