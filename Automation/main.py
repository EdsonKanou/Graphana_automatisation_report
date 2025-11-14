"""
Script principal pour générer et envoyer le rapport Grafana
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Import des modules du projet
from logger_config import setup_logger, cleanup_old_logs
from grafana_client import GrafanaClient
from email_generator import EmailGenerator
from email_sender import EmailSender
from config import DASHBOARDS, TIME_PERIODS


def load_environment() -> dict:
    """
    Charge les variables d'environnement
    
    Returns:
        Dictionnaire avec les configurations
    """
    # Charger le fichier .env
    load_dotenv()
    
    # Vérifier les variables obligatoires
    required_vars = [
        'GRAFANA_URL',
        'GRAFANA_API_KEY',
        'SMTP_SERVER',
        'SMTP_PORT',
        'SMTP_USERNAME',
        'SMTP_PASSWORD',
        'EMAIL_FROM',
        'EMAIL_TO',
        'EMAIL_SUBJECT'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"Variables d'environnement manquantes: {', '.join(missing_vars)}")
    
    return {
        'grafana_url': os.getenv('GRAFANA_URL'),
        'grafana_api_key': os.getenv('GRAFANA_API_KEY'),
        'smtp_server': os.getenv('SMTP_SERVER'),
        'smtp_port': int(os.getenv('SMTP_PORT', 587)),
        'smtp_username': os.getenv('SMTP_USERNAME'),
        'smtp_password': os.getenv('SMTP_PASSWORD'),
        'smtp_use_tls': os.getenv('SMTP_USE_TLS', 'True').lower() == 'true',
        'email_from': os.getenv('EMAIL_FROM'),
        'email_to': os.getenv('EMAIL_TO').split(','),
        'email_cc': os.getenv('EMAIL_CC', '').split(',') if os.getenv('EMAIL_CC') else [],
        'email_subject': os.getenv('EMAIL_SUBJECT')
    }


def save_html_report(html_content: str, filename: str = "report.html"):
    """
    Sauvegarde le rapport HTML localement
    
    Args:
        html_content: Contenu HTML
        filename: Nom du fichier
    """
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    
    filepath = reports_dir / filename
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    logger.info(f"Rapport HTML sauvegardé: {filepath}")


def main():
    """Fonction principale"""
    
    # 1. Configuration du logger
    global logger
    logger = setup_logger()
    logger.info("=" * 80)
    logger.info("DÉMARRAGE DU GÉNÉRATEUR DE RAPPORT GRAFANA")
    logger.info("=" * 80)
    
    try:
        # 2. Charger l'environnement
        logger.info("Chargement de la configuration...")
        env = load_environment()
        logger.info("✓ Configuration chargée")
        
        # 3. Initialiser le client Grafana
        logger.info("Initialisation du client Grafana...")
        grafana_client = GrafanaClient(
            base_url=env['grafana_url'],
            api_key=env['grafana_api_key']
        )
        
        # 4. Interroger tous les dashboards
        logger.info(f"Interrogation de {len(DASHBOARDS)} dashboard(s)...")
        dashboards_data = grafana_client.query_all_dashboards(
            dashboards=DASHBOARDS,
            time_periods=TIME_PERIODS
        )
        
        if not dashboards_data:
            logger.warning("⚠️  Aucune donnée récupérée des dashboards")
            return
        
        # 5. Générer le HTML
        logger.info("Génération du rapport HTML...")
        email_generator = EmailGenerator()
        html_content = email_generator.generate_html(dashboards_data)
        
        # 6. Sauvegarder localement
        from datetime import datetime
        filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        save_html_report(html_content, filename)
        
        # 7. Envoyer l'email
        logger.info("Préparation de l'envoi de l'email...")
        email_sender = EmailSender(
            smtp_server=env['smtp_server'],
            smtp_port=env['smtp_port'],
            username=env['smtp_username'],
            password=env['smtp_password'],
            use_tls=env['smtp_use_tls']
        )
        
        # Test de connexion (optionnel)
        if not email_sender.test_connection():
            logger.error("Impossible de se connecter au serveur SMTP")
            logger.info("Le rapport a été sauvegardé localement mais pas envoyé")
            return
        
        # Envoi
        success = email_sender.send_email(
            from_addr=env['email_from'],
            to_addrs=env['email_to'],
            subject=env['email_subject'],
            html_content=html_content,
            cc_addrs=env['email_cc'] if env['email_cc'] else None
        )
        
        if success:
            logger.info("=" * 80)
            logger.info("✅ RAPPORT GÉNÉRÉ ET ENVOYÉ AVEC SUCCÈS")
            logger.info("=" * 80)
        else:
            logger.warning("⚠️  Rapport généré mais échec de l'envoi")
        
    except ValueError as e:
        logger.error(f"Erreur de configuration: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Erreur fatale: {e}", exc_info=True)
        sys.exit(1)
    finally:
        # Nettoyage des vieux logs (30 jours)
        cleanup_old_logs(days=30)


if __name__ == "__main__":
    main()