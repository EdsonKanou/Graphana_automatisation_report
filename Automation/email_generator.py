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
        if not dashboards_data:
            html += """
                <div style="padding: 30px; text-align: center;">
                    <p class="error-message">❌ Aucune donnée disponible</p>
                </div>
            """
        else:
            for dashboard in dashboards_data:
                html += self._generate_dashboard_section(dashboard)
        
        # Pied de page
        html += f"""
                <div class="footer">
                    <p>Ce rapport a été généré automatiquement par le système de monitoring Grafana.</p>
                    <p>Pour toute question, contactez l'équipe infrastructure.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        logger.info("HTML généré avec succès")
        return html
    
    def _generate_dashboard_section(self, dashboard: Dict[str, Any]) -> str:
        """Génère la section HTML d'un dashboard"""
        html = f"""
                <div class="dashboard">
                    <div class="dashboard-title">🎯 {dashboard['name']}</div>
                    <div class="panels-grid">
        """
        
        for panel in dashboard['panels']:
            html += self._generate_panel_card(panel)
        
        html += """
                    </div>
                </div>
        """
        return html
    
    def _generate_panel_card(self, panel: Dict[str, Any]) -> str:
        """Génère la carte HTML d'un panel"""
        html = f"""
                        <div class="panel-card">
                            <div class="panel-name">{panel['name']}</div>
                            <div class="periods-container">
        """
        
        for period in panel['periods']:
            value = period['value']
            unit = panel['unit']
            thresholds = panel['thresholds']
            
            color = self.get_status_color(value, thresholds, unit)
            formatted_value = self.format_value(value, unit)
            
            html += f"""
                                <div class="period-badge">
                                    <div class="period-label">{period['label']}</div>
                                    <div class="period-value" style="color: {color};">
                                        {formatted_value}
                                    </div>
                                </div>
            """
        
        # Ajouter une tendance si possible
        trend = self.calculate_trend(panel['periods'])
        if trend is not None:
            trend_class = "trend-up" if trend > 0 else "trend-down" if trend < 0 else "trend-neutral"
            trend_symbol = "↗" if trend > 0 else "↘" if trend < 0 else "→"
            html += f"""
                                <div class="trend {trend_class}">
                                    {trend_symbol} {abs(trend):.1f}% vs période la plus ancienne
                                </div>
            """
        
        html += """
                            </div>
                        </div>
        """
        return html