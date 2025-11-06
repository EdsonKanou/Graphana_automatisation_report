"""
Module de formatage des données Grafana en tableaux HTML
Transforme les métriques brutes en tableaux structurés et stylés
"""

import pandas as pd
from typing import List, Dict, Any
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataFormatter:
    """Classe pour formater les données Grafana en tableaux"""
    
    def __init__(self):
        """Initialise le formateur"""
        pass
    
    def format_panel_data(self, panel_data: Dict) -> pd.DataFrame:
        """
        Transforme les données d'un panel en DataFrame pandas
        
        Args:
            panel_data: Dictionnaire contenant les données d'un panel
            
        Returns:
            DataFrame pandas avec les données formatées
        """
        try:
            results = panel_data.get('data', {}).get('results', {})
            
            # Extraction des séries temporelles
            all_series = []
            for result_key, result in results.items():
                frames = result.get('frames', [])
                
                for frame in frames:
                    series_data = self._extract_time_series(frame)
                    if series_data:
                        all_series.append(series_data)
            
            # Si aucune donnée, retourne un DataFrame vide
            if not all_series:
                logger.warning(f"Aucune donnée pour le panel {panel_data.get('panel_title')}")
                return pd.DataFrame()
            
            # Combine toutes les séries
            df = pd.concat(all_series, ignore_index=True)
            
            # Ajoute le titre du panel comme colonne
            df['Panel'] = panel_data.get('panel_title', 'Unknown')
            
            return df
            
        except Exception as e:
            logger.error(f"Erreur lors du formatage des données: {e}")
            return pd.DataFrame()
    
    def _extract_time_series(self, frame: Dict) -> pd.DataFrame:
        """
        Extrait une série temporelle d'un frame Grafana
        
        Args:
            frame: Frame de données Grafana
            
        Returns:
            DataFrame avec les colonnes Time, Metric, Value
        """
        schema = frame.get('schema', {})
        data = frame.get('data', {})
        
        # Identifie les colonnes
        time_col = None
        value_col = None
        label_cols = []
        
        fields = schema.get('fields', [])
        for i, field in enumerate(fields):
            field_name = field.get('name', '')
            field_type = field.get('type', '')
            
            if field_name == 'Time' or field_type == 'time':
                time_col = i
            elif field_name == 'Value' or field_type == 'number':
                value_col = i
            else:
                # Autres colonnes (labels, etc.)
                label_cols.append((i, field_name))
        
        if time_col is None or value_col is None:
            return pd.DataFrame()
        
        # Récupère les valeurs
        values = data.get('values', [])
        if not values or len(values) <= max(time_col, value_col):
            return pd.DataFrame()
        
        timestamps = values[time_col]
        metric_values = values[value_col]
        
        # Construit les labels de la métrique
        metric_name_parts = []
        for label_idx, label_name in label_cols:
            if label_idx < len(values):
                label_value = values[label_idx][0] if values[label_idx] else ""
                metric_name_parts.append(f"{label_name}={label_value}")
        
        metric_name = ", ".join(metric_name_parts) if metric_name_parts else "metric"
        
        # Crée le DataFrame
        df = pd.DataFrame({
            'Timestamp': [datetime.fromtimestamp(ts / 1000) for ts in timestamps],
            'Metric': [metric_name] * len(timestamps),
            'Value': metric_values
        })
        
        return df
    
    def aggregate_data(self, df: pd.DataFrame, agg_method: str = 'last') -> pd.DataFrame:
        """
        Agrège les données par métrique
        
        Args:
            df: DataFrame avec les données
            agg_method: Méthode d'agrégation ('last', 'mean', 'max', 'min', 'sum')
            
        Returns:
            DataFrame agrégé
        """
        if df.empty:
            return df
        
        # Groupe par Panel et Metric
        grouped = df.groupby(['Panel', 'Metric'])
        
        # Applique l'agrégation
        if agg_method == 'last':
            result = grouped.last().reset_index()
        elif agg_method == 'mean':
            result = grouped.agg({'Value': 'mean', 'Timestamp': 'max'}).reset_index()
        elif agg_method == 'max':
            result = grouped.agg({'Value': 'max', 'Timestamp': 'max'}).reset_index()
        elif agg_method == 'min':
            result = grouped.agg({'Value': 'min', 'Timestamp': 'max'}).reset_index()
        elif agg_method == 'sum':
            result = grouped.agg({'Value': 'sum', 'Timestamp': 'max'}).reset_index()
        else:
            result = grouped.last().reset_index()
        
        # Arrondit les valeurs
        result['Value'] = result['Value'].round(2)
        
        # Formate le timestamp
        result['Timestamp'] = pd.to_datetime(result['Timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        return result
    
    def create_summary_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Crée un tableau de statistiques récapitulatives
        
        Args:
            df: DataFrame avec les données
            
        Returns:
            DataFrame avec les statistiques (min, max, mean, std)
        """
        if df.empty:
            return pd.DataFrame()
        
        stats = df.groupby(['Panel', 'Metric'])['Value'].agg([
            ('Min', 'min'),
            ('Max', 'max'),
            ('Moyenne', 'mean'),
            ('Écart-type', 'std')
        ]).round(2).reset_index()
        
        return stats
    
    def format_all_panels(self, 
                         all_panel_data: List[Dict],
                         agg_method: str = 'last',
                         include_summary: bool = True) -> Dict[str, pd.DataFrame]:
        """
        Formate les données de tous les panels
        
        Args:
            all_panel_data: Liste de dictionnaires contenant les données des panels
            agg_method: Méthode d'agrégation
            include_summary: Inclure un tableau de statistiques récapitulatives
            
        Returns:
            Dictionnaire avec les DataFrames formatés par panel
        """
        formatted_tables = {}
        all_data = []
        
        # Formate chaque panel
        for panel_data in all_panel_data:
            df = self.format_panel_data(panel_data)
            if not df.empty:
                all_data.append(df)
        
        # Combine toutes les données
        if not all_data:
            logger.warning("Aucune donnée à formater")
            return formatted_tables
        
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Agrège les données
        aggregated_df = self.aggregate_data(combined_df, agg_method)
        
        # Sépare par panel
        for panel_name in aggregated_df['Panel'].unique():
            panel_df = aggregated_df[aggregated_df['Panel'] == panel_name].copy()
            panel_df = panel_df.drop('Panel', axis=1)  # Retire la colonne Panel
            formatted_tables[panel_name] = panel_df
        
        # Ajoute le tableau récapitulatif
        if include_summary:
            summary_df = self.create_summary_stats(combined_df)
            if not summary_df.empty:
                formatted_tables['📊 Résumé Statistique'] = summary_df
        
        logger.info(f"{len(formatted_tables)} tableaux créés")
        return formatted_tables
    
    def dataframe_to_html(self, df: pd.DataFrame, table_title: str = "") -> str:
        """
        Convertit un DataFrame en HTML stylé
        
        Args:
            df: DataFrame à convertir
            table_title: Titre du tableau
            
        Returns:
            String HTML du tableau
        """
        if df.empty:
            return f"<p><em>Aucune donnée disponible pour {table_title}</em></p>"
        
        # Génère le HTML du tableau avec pandas
        html_table = df.to_html(
            index=False,
            classes='data-table',
            border=0,
            justify='left',
            na_rep='N/A'
        )
        
        # Ajoute le titre si spécifié
        if table_title:
            html_content = f"""
            <div class="table-container">
                <h2 class="table-title">{table_title}</h2>
                {html_table}
            </div>
            """
        else:
            html_content = f"""
            <div class="table-container">
                {html_table}
            </div>
            """
        
        return html_content
    
    def create_full_html_report(self, 
                               tables: Dict[str, pd.DataFrame],
                               report_title: str = "Rapport Grafana",
                               dashboard_name: str = "") -> str:
        """
        Crée un rapport HTML complet avec tous les tableaux
        
        Args:
            tables: Dictionnaire de DataFrames {titre: dataframe}
            report_title: Titre principal du rapport
            dashboard_name: Nom du dashboard
            
        Returns:
            String HTML complet du rapport
        """
        # Date de génération
        generation_date = datetime.now().strftime('%d/%m/%Y à %H:%M:%S')
        
        # Crée le contenu HTML pour chaque tableau
        tables_html = ""
        for table_title, df in tables.items():
            tables_html += self.dataframe_to_html(df, table_title)
        
        # Template HTML complet avec CSS
        html_content = f"""
        <!DOCTYPE html>
        <html lang="fr">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{report_title}</title>
            <style>
                body {{
                    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
                    line-height: 1.6;
                    color: #333;
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 20px;
                    background-color: #f5f5f5;
                }}
                
                .header {{
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 30px;
                    border-radius: 10px;
                    margin-bottom: 30px;
                    box-shadow: 0 4px 6px rgba(0,0,0,0.1);
                }}
                
                .header h1 {{
                    margin: 0 0 10px 0;
                    font-size: 2em;
                }}
                
                .header .subtitle {{
                    opacity: 0.9;
                    font-size: 0.9em;
                }}
                
                .table-container {{
                    background: white;
                    padding: 25px;
                    margin-bottom: 25px;
                    border-radius: 8px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                
                .table-title {{
                    color: #667eea;
                    font-size: 1.4em;
                    margin: 0 0 20px 0;
                    padding-bottom: 10px;
                    border-bottom: 2px solid #667eea;
                }}
                
                .data-table {{
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 0.95em;
                }}
                
                .data-table thead {{
                    background-color: #667eea;
                    color: white;
                }}
                
                .data-table th {{
                    padding: 12px;
                    text-align: left;
                    font-weight: 600;
                    text-transform: uppercase;
                    font-size: 0.85em;
                    letter-spacing: 0.5px;
                }}
                
                .data-table td {{
                    padding: 12px;
                    border-bottom: 1px solid #e0e0e0;
                }}
                
                .data-table tbody tr:hover {{
                    background-color: #f8f9fa;
                }}
                
                .data-table tbody tr:last-child td {{
                    border-bottom: none;
                }}
                
                .footer {{
                    text-align: center;
                    color: #666;
                    font-size: 0.85em;
                    margin-top: 40px;
                    padding-top: 20px;
                    border-top: 1px solid #ddd;
                }}
                
                @media (max-width: 768px) {{
                    body {{
                        padding: 10px;
                    }}
                    
                    .data-table {{
                        font-size: 0.85em;
                    }}
                    
                    .data-table th,
                    .data-table td {{
                        padding: 8px;
                    }}
                }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>{report_title}</h1>
                <div class="subtitle">
                    {f'Dashboard: {dashboard_name}<br>' if dashboard_name else ''}
                    Généré le {generation_date}
                </div>
            </div>
            
            {tables_html}
            
            <div class="footer">
                <p>Rapport automatique généré par le système de monitoring</p>
            </div>
        </body>
        </html>
        """
        
        return html_content