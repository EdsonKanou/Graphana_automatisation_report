"""
Planificateur d'exécution automatique
Gère l'exécution quotidienne du script d'export
"""

import scheduler
import time
import logging
import sys
from datetime import datetime
from dotenv import load_dotenv
import os

# Import du module principal
from main import GrafanaEmailExporter

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def job_daily_export():
    """
    Tâche planifiée: Export quotidien Grafana
    Cette fonction sera appelée chaque jour à l'heure définie
    """
    logger.info("╔" + "═" * 60 + "╗")
    logger.info("║" + " " * 10 + "DÉMARRAGE EXPORT QUOTIDIEN" + " " * 23 + "║")
    logger.info("╚" + "═" * 60 + "╝")
    
    try:
        # Initialise l'exporteur
        exporter = GrafanaEmailExporter()
        
        # Exécute l'export
        success = exporter.export_and_send(
            time_from='now-24h',      # Données des dernières 24h
            time_to='now',
            agg_method='last',        # Dernière valeur de chaque métrique
            include_summary=True,     # Inclut statistiques récapitulatives
            intro_text="""
            Bonjour,<br><br>
            Veuillez trouver ci-dessous le rapport quotidien de monitoring pour les dernières 24 heures.
            Les données ont été collectées automatiquement depuis votre dashboard Grafana.
            """
        )
        
        if success:
            logger.info("✅ Export quotidien réussi!")
        else:
            logger.error("❌ Export quotidien échoué!")
            # Optionnel: envoyer une alerte en cas d'échec
            send_failure_alert(exporter)
            
    except Exception as e:
        logger.error(f"❌ Erreur lors de l'export quotidien: {e}", exc_info=True)
        # Optionnel: envoyer une alerte en cas d'erreur
        try:
            exporter = GrafanaEmailExporter()
            send_failure_alert(exporter, error_msg=str(e))
        except:
            pass


def job_weekly_summary():
    """
    Tâche planifiée: Résumé hebdomadaire (optionnel)
    Export des données de la semaine écoulée
    """
    logger.info("╔" + "═" * 60 + "╗")
    logger.info("║" + " " * 10 + "DÉMARRAGE RÉSUMÉ HEBDOMADAIRE" + " " * 19 + "║")
    logger.info("╚" + "═" * 60 + "╝")
    
    try:
        exporter = GrafanaEmailExporter()
        
        success = exporter.export_and_send(
            time_from='now-7d',       # Données des 7 derniers jours
            time_to='now',
            agg_method='mean',        # Moyenne sur la période
            include_summary=True,
            custom_subject="📊 Résumé Hebdomadaire - Grafana",
            intro_text="""
            Bonjour,<br><br>
            Veuillez trouver ci-dessous le résumé hebdomadaire de monitoring pour les 7 derniers jours.
            Les valeurs présentées sont les moyennes calculées sur cette période.
            """
        )
        
        if success:
            logger.info("✅ Résumé hebdomadaire envoyé!")
        else:
            logger.error("❌ Échec du résumé hebdomadaire!")
            
    except Exception as e:
        logger.error(f"❌ Erreur lors du résumé hebdomadaire: {e}", exc_info=True)


def send_failure_alert(exporter: GrafanaEmailExporter, error_msg: str = ""):
    """
    Envoie un email d'alerte en cas d'échec de l'export
    
    Args:
        exporter: Instance de GrafanaEmailExporter
        error_msg: Message d'erreur détaillé (optionnel)
    """
    alert_html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta charset="UTF-8">
        <style>
            body {{
                font-family: Arial, sans-serif;
                padding: 20px;
                background-color: #f5f5f5;
            }}
            .alert {{
                background-color: #fff;
                border-left: 4px solid #dc3545;
                padding: 20px;
                border-radius: 8px;
                box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            }}
            h1 {{
                color: #dc3545;
                margin-top: 0;
            }}
            .error-details {{
                background-color: #f8d7da;
                color: #721c24;
                padding: 15px;
                border-radius: 5px;
                margin-top: 15px;
                font-family: monospace;
            }}
        </style>
    </head>
    <body>
        <div class="alert">
            <h1>⚠️ Alerte - Échec Export Grafana</h1>
            <p>L'export automatique quotidien des données Grafana a échoué.</p>
            <p><strong>Date et heure:</strong> {datetime.now().strftime('%d/%m/%Y à %H:%M:%S')}</p>
            {f'<div class="error-details"><strong>Détails de l\'erreur:</strong><br>{error_msg}</div>' if error_msg else ''}
            <p style="margin-top: 20px;">
                Veuillez vérifier les logs du système pour plus d'informations.
            </p>
        </div>
    </body>
    </html>
    """
    
    try:
        exporter.email_sender.send_html_email(
            to_emails=exporter.email_to,
            subject="⚠️ Alerte - Échec Export Grafana",
            html_content=alert_html
        )
        logger.info("Email d'alerte envoyé")
    except Exception as e:
        logger.error(f"Impossible d'envoyer l'email d'alerte: {e}")


class Scheduler:
    """Gestionnaire de planification des tâches"""
    
    def __init__(self):
        """Initialise le planificateur"""
        load_dotenv()
        self.schedule_time = os.getenv('SCHEDULE_TIME', '08:00')
        self.enable_weekly = os.getenv('ENABLE_WEEKLY_REPORT', 'false').lower() == 'true'
        
    def setup_schedules(self):
        """Configure les tâches planifiées"""
        
        # Tâche quotidienne
        scheduler.every().day.at(self.schedule_time).do(job_daily_export)
        logger.info(f"✓ Export quotidien planifié à {self.schedule_time}")
        
        # Tâche hebdomadaire (optionnel)
        if self.enable_weekly:
            # Tous les lundis à 09:00
            scheduler.every().monday.at("09:00").do(job_weekly_summary)
            logger.info("✓ Résumé hebdomadaire planifié (lundis à 09:00)")
        
        # Affiche toutes les tâches planifiées
        logger.info(f"Nombre de tâches planifiées: {len(scheduler.jobs)}")
        for job in scheduler.jobs:
            logger.info(f"  - {job}")
    
    def run(self):
        """Lance le planificateur en mode continu"""
        logger.info("╔" + "═" * 60 + "╗")
        logger.info("║" + " " * 15 + "SCHEDULER DÉMARRÉ" + " " * 27 + "║")
        logger.info("╚" + "═" * 60 + "╝")
        
        self.setup_schedules()
        
        logger.info("\n🚀 Le planificateur est maintenant actif...")
        logger.info("Appuyez sur Ctrl+C pour arrêter\n")
        
        try:
            while True:
                # Vérifie et exécute les tâches planifiées
                scheduler.run_pending()
                
                # Attend 60 secondes avant la prochaine vérification
                time.sleep(60)
                
        except KeyboardInterrupt:
            logger.info("\n\n🛑 Arrêt du planificateur demandé")
            logger.info("Planificateur arrêté proprement\n")
            sys.exit(0)
    
    def run_now(self):
        """Exécute immédiatement l'export (pour tester)"""
        logger.info("Exécution immédiate de l'export...")
        job_daily_export()


def main():
    """Fonction principale du planificateur"""
    
    print("""
    ╔══════════════════════════════════════════════╗
    ║   GRAFANA SCHEDULER                          ║
    ║   Planification automatique des exports      ║
    ╚══════════════════════════════════════════════╝
    """)
    
    scheduler = Scheduler()
    
    # Vérifie les arguments
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'now':
            # Exécute immédiatement
            print("\n⚡ Exécution immédiate\n")
            scheduler.run_now()
            sys.exit(0)
        
        elif command == 'test':
            # Test de configuration
            print("\n🔍 Test de configuration\n")
            exporter = GrafanaEmailExporter()
            success = exporter.test_configuration()
            sys.exit(0 if success else 1)
        
        elif command == 'help':
            print("""
Usage:
    python scheduler.py          - Démarre le planificateur en mode continu
    python scheduler.py now      - Exécute l'export immédiatement
    python scheduler.py test     - Teste la configuration
    python scheduler.py help     - Affiche cette aide

Configuration:
    Les horaires sont définis dans le fichier .env:
    - SCHEDULE_TIME: Heure de l'export quotidien (format HH:MM)
    - ENABLE_WEEKLY_REPORT: Active le rapport hebdomadaire (true/false)
            """)
            sys.exit(0)
    
    # Mode normal: lance le planificateur
    scheduler.run()


if __name__ == "__main__":
    main()