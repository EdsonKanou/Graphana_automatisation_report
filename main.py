"""
Script principal - Orchestration de l'export Grafana vers Email
Coordonne la récupération des données, le formatage et l'envoi
"""

import os
import sys
import logging
from datetime import datetime
from typing import Optional
from dotenv import load_dotenv

# Imports des modules personnalisés
from grafana_client import GrafanaClient
from data_formatter import DataFormatter
from email_sender import EmailSender, EmailTemplates

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('grafana_export.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class GrafanaEmailExporter:
    """Classe principale pour orchestrer l'export Grafana vers Email"""
    
    def __init__(self, config_file: str = '.env'):
        """
        Initialise l'exporteur avec la configuration
        
        Args:
            config_file: Chemin du fichier de configuration (.env)
        """
        # Charge les variables d'environnement
        load_dotenv(config_file)
        
        # Configuration Grafana
        self.grafana_url = os.getenv('GRAFANA_URL')
        self.grafana_api_key = os.getenv('GRAFANA_API_KEY')
        self.dashboard_uid = os.getenv('DASHBOARD_UID')
        
        # Configuration Email
        self.smtp_server = os.getenv('SMTP_SERVER')
        self.smtp_port = int(os.getenv('SMTP_PORT', 587))
        self.smtp_user = os.getenv('SMTP_USER')
        self.smtp_password = os.getenv('SMTP_PASSWORD')
        self.email_from = os.getenv('EMAIL_FROM')
        self.email_to = os.getenv('EMAIL_TO', '').split(',')
        
        # Configuration générale
        self.timezone = os.getenv('TIMEZONE', 'Europe/Paris')
        
        # Validation de la configuration
        self._validate_config()
        
        # Initialisation des clients
        self.grafana_client = GrafanaClient(self.grafana_url, self.grafana_api_key)
        self.data_formatter = DataFormatter()
        self.email_sender = EmailSender(
            smtp_server=self.smtp_server,
            smtp_port=self.smtp_port,
            smtp_user=self.smtp_user,
            smtp_password=self.smtp_password,
            from_email=self.email_from
        )
        
        logger.info("GrafanaEmailExporter initialisé avec succès")
    
    def _validate_config(self):
        """Valide que toutes les configurations nécessaires sont présentes"""
        required_vars = [
            'GRAFANA_URL', 'GRAFANA_API_KEY', 'DASHBOARD_UID',
            'SMTP_SERVER', 'SMTP_USER', 'SMTP_PASSWORD', 
            'EMAIL_FROM', 'EMAIL_TO'
        ]
        
        missing_vars = []
        for var in required_vars:
            if not os.getenv(var):
                missing_vars.append(var)
        
        if missing_vars:
            error_msg = f"Variables de configuration manquantes: {', '.join(missing_vars)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
    
    def export_and_send(self,
                       time_from: str = 'now-24h',
                       time_to: str = 'now',
                       agg_method: str = 'last',
                       include_summary: bool = True,
                       custom_subject: Optional[str] = None,
                       intro_text: Optional[str] = None) -> bool:
        """
        Exporte les données Grafana et envoie le rapport par email
        
        Args:
            time_from: Début de la période (format Grafana: now-24h, now-7d, etc.)
            time_to: Fin de la période
            agg_method: Méthode d'agrégation ('last', 'mean', 'max', 'min', 'sum')
            include_summary: Inclure un tableau de statistiques récapitulatives
            custom_subject: Sujet personnalisé pour l'email (optionnel)
            intro_text: Texte d'introduction personnalisé (optionnel)
            
        Returns:
            True si l'export et l'envoi ont réussi, False sinon
        """
        try:
            logger.info("=== Début de l'export Grafana ===")
            
            # Étape 1: Récupération des données depuis Grafana
            logger.info("Récupération des données depuis Grafana...")
            dashboard_data = self.grafana_client.get_dashboard(self.dashboard_uid)
            dashboard_name = dashboard_data.get('dashboard', {}).get('title', 'Dashboard')
            
            all_panel_data = self.grafana_client.get_all_panels_data(
                self.dashboard_uid,
                time_from,
                time_to
            )
            
            if not all_panel_data:
                logger.warning("Aucune donnée récupérée depuis Grafana")
                return False
            
            logger.info(f"✓ {len(all_panel_data)} panels récupérés")
            
            # Étape 2: Formatage des données
            logger.info("Formatage des données...")
            formatted_tables = self.data_formatter.format_all_panels(
                all_panel_data,
                agg_method=agg_method,
                include_summary=include_summary
            )
            
            if not formatted_tables:
                logger.warning("Aucun tableau généré après formatage")
                return False
            
            logger.info(f"✓ {len(formatted_tables)} tableaux générés")
            
            # Étape 3: Génération du rapport HTML
            logger.info("Génération du rapport HTML...")
            html_report = self.data_formatter.create_full_html_report(
                tables=formatted_tables,
                report_title="Rapport Grafana",
                dashboard_name=dashboard_name
            )
            
            # Ajoute l'introduction si fournie
            if intro_text:
                html_report = EmailTemplates.add_introduction(html_report, intro_text)
            
            logger.info("✓ Rapport HTML généré")
            
            # Étape 4: Envoi de l'email
            logger.info("Envoi de l'email...")
            subject = custom_subject or EmailTemplates.daily_report_subject(dashboard_name)
            
            success = self.email_sender.send_html_email(
                to_emails=self.email_to,
                subject=subject,
                html_content=html_report
            )
            
            if success:
                logger.info("✓ Email envoyé avec succès")
                logger.info("=== Export Grafana terminé avec succès ===")
                return True
            else:
                logger.error("✗ Échec de l'envoi de l'email")
                return False
                
        except Exception as e:
            logger.error(f"Erreur lors de l'export: {e}", exc_info=True)
            return False
    
    def test_configuration(self) -> bool:
        """
        Teste toute la configuration (Grafana + Email)
        
        Returns:
            True si tous les tests passent, False sinon
        """
        logger.info("=== Test de configuration ===")
        
        # Test 1: Connexion Grafana
        logger.info("Test de connexion à Grafana...")
        try:
            dashboard = self.grafana_client.get_dashboard(self.dashboard_uid)
            dashboard_name = dashboard.get('dashboard', {}).get('title', 'Unknown')
            logger.info(f"✓ Connexion Grafana OK - Dashboard: {dashboard_name}")
        except Exception as e:
            logger.error(f"✗ Échec connexion Grafana: {e}")
            return False
        
        # Test 2: Connexion SMTP
        logger.info("Test de connexion SMTP...")
        if self.email_sender.test_connection():
            logger.info("✓ Connexion SMTP OK")
        else:
            logger.error("✗ Échec connexion SMTP")
            return False
        
        logger.info("=== Tous les tests sont passés ===")
        return True
    
    def save_report_to_file(self,
                           time_from: str = 'now-24h',
                           time_to: str = 'now',
                           output_file: str = 'report.html') -> bool:
        """
        Génère et sauvegarde le rapport dans un fichier (sans envoi email)
        
        Args:
            time_from: Début de la période
            time_to: Fin de la période
            output_file: Nom du fichier de sortie
            
        Returns:
            True si la sauvegarde a réussi, False sinon
        """
        try:
            logger.info("Génération du rapport pour sauvegarde...")
            
            # Récupère les données
            dashboard_data = self.grafana_client.get_dashboard(self.dashboard_uid)
            dashboard_name = dashboard_data.get('dashboard', {}).get('title', 'Dashboard')
            
            all_panel_data = self.grafana_client.get_all_panels_data(
                self.dashboard_uid,
                time_from,
                time_to
            )
            
            # Formate les données
            formatted_tables = self.data_formatter.format_all_panels(all_panel_data)
            
            # Génère le HTML
            html_report = self.data_formatter.create_full_html_report(
                tables=formatted_tables,
                report_title="Rapport Grafana",
                dashboard_name=dashboard_name
            )
            
            # Sauvegarde dans le fichier
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_report)
            
            logger.info(f"✓ Rapport sauvegardé dans {output_file}")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de la sauvegarde du rapport: {e}")
            return False


def main():
    """Fonction principale - Point d'entrée du script"""
    
    # Affiche le banner
    print("""
    ╔══════════════════════════════════════════════╗
    ║   GRAFANA EMAIL EXPORTER                     ║
    ║   Export automatique Dashboard → Email       ║
    ╚══════════════════════════════════════════════╝
    """)
    
    try:
        # Initialise l'exporteur
        exporter = GrafanaEmailExporter()
        
        # Vérifie les arguments de ligne de commande
        if len(sys.argv) > 1:
            command = sys.argv[1].lower()
            
            if command == 'test':
                # Mode test: vérifie la configuration
                print("\n🔍 Mode TEST - Vérification de la configuration\n")
                success = exporter.test_configuration()
                sys.exit(0 if success else 1)
            
            elif command == 'save':
                # Mode sauvegarde: génère le rapport dans un fichier
                output_file = sys.argv[2] if len(sys.argv) > 2 else 'report.html'
                print(f"\n💾 Mode SAVE - Génération du rapport dans {output_file}\n")
                success = exporter.save_report_to_file(output_file=output_file)
                sys.exit(0 if success else 1)
            
            elif command == 'help':
                # Affiche l'aide
                print("""
Usage:
    python main.py              - Exécute l'export et envoie l'email
    python main.py test         - Teste la configuration (Grafana + Email)
    python main.py save [file]  - Sauvegarde le rapport dans un fichier
    python main.py help         - Affiche cette aide
                """)
                sys.exit(0)
        
        # Mode normal: export et envoi d'email
        print("\n📧 Mode NORMAL - Export et envoi d'email\n")
        
        success = exporter.export_and_send(
            time_from='now-24h',      # Dernières 24h
            time_to='now',
            agg_method='last',        # Prend la dernière valeur
            include_summary=True,     # Inclut un tableau récapitulatif
            intro_text="Veuillez trouver ci-dessous le rapport quotidien de monitoring."
        )
        
        if success:
            print("\n✅ Export terminé avec succès!\n")
            sys.exit(0)
        else:
            print("\n❌ Échec de l'export\n")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Erreur fatale: {e}", exc_info=True)
        print(f"\n❌ Erreur: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()