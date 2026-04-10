import requests
import pandas as pd
import time

class GrafanaAPI:
    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

    def get_dash(self, uid: str) -> dict:
        """Récupère le JSON complet d’un dashboard"""
        url = f"{self.base_url}/api/dashboards/uid/{uid}"
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return r.json()

    def get_panel_from_dashboard(self, uid: str, panel_id: int = None, panel_title: str = None) -> dict:
        """Retourne un panel spécifique par ID ou titre"""
        dashboard_json = self.get_dash(uid)
        panels = dashboard_json['dashboard']['panels']
        for panel in panels:
            if (panel_id is not None and panel['id'] == panel_id) or \
               (panel_title is not None and panel['title'] == panel_title):
                return panel
        raise ValueError("Panel non trouvé dans le dashboard")

    def query_panel_data(self, panel: dict, from_s: int = None, to_s: int = None) -> pd.DataFrame:
        """Exécute la requête d’un panel via /api/ds/query et retourne les données réelles"""

        # 1️⃣ Récupération de la datasource
        ds = panel.get('datasource', {})
        if not ds or 'uid' not in ds:
            raise ValueError("Le panel ne contient pas d’information sur la datasource")

        datasource_uid = ds['uid']
        datasource_type = ds.get('type', 'prometheus')  # par défaut

        # 2️⃣ Définir le range temporel
        now_ms = int(time.time() * 1000)
        from_ms = int((time.time() - 3600) * 1000) if from_s is None else from_s * 1000
        to_ms = now_ms if to_s is None else to_s * 1000

        # 3️⃣ Construire les queries
        queries = []
        for target in panel.get('targets', []):
            q = {
                "refId": target.get('refId', 'A'),
                "datasource": {"uid": datasource_uid, "type": datasource_type},
                "intervalMs": 15000,
                "maxDataPoints": 500,
            }

            # Adapter selon le type de datasource
            if 'expr' in target:  # Prometheus
                q["expr"] = target['expr']
            elif 'rawSql' in target:  # SQL datasource
                q["rawSql"] = target['rawSql']

            q["range"] = {"from": from_ms, "to": to_ms}
            queries.append(q)

        # 4️⃣ Appel API /api/ds/query
        url = f"{self.base_url}/api/ds/query"
        r = requests.post(url, headers=self.headers, json={"queries": queries})
        r.raise_for_status()
        data = r.json()

        # 5️⃣ Extraction des résultats (simplifiée)
        results = data.get('results', {})
        if not results:
            raise ValueError("Aucun résultat dans la réponse Grafana")

        dfs = []
        for ref, res in results.items():
            frames = res.get('frames', [])
            for frame in frames:
                fields = frame.get('fields', [])
                frame_data = {}
                for f in fields:
                    name = f.get('name', 'col')
                    vals = f.get('values', {}).get('buffer', f.get('values', []))
                    frame_data[name] = vals
                if frame_data:
                    dfs.append(pd.DataFrame(frame_data))

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

grafana = GrafanaAPI("https://grafana.mondomaine.com", "eyJrIjo...")

# 1️⃣ Récupérer le panel (par UID + titre)
panel = grafana.get_panel_from_dashboard(uid="abcd1234", panel_title="CPU usage")

# 2️⃣ Exécuter la requête du panel
df = grafana.query_panel_data(panel)

# 3️⃣ Visualiser les données réelles du panel
print(df.head())
