"""
Tests complets pour l'envoi d'emails
Teste toutes les fonctionnalités du module email_sender
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from email_sender import EmailSender, EmailTemplates

# Charge les variables d'environnement
load_dotenv()


def print_section(title):
    """Affiche une section formatée"""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_success(message):
    """Affiche un message de succès"""
    print(f"✅ {message}")


def print_error(message):
    """Affiche un message d'erreur"""
    print(f"❌ {message}")


def print_info(message):
    """Affiche une information"""
    print(f"ℹ️  {message}")


def get_email_config():
    """Récupère la configuration email depuis .env"""
    config = {
        'smtp_server': os.getenv('SMTP_SERVER'),
        'smtp_port': int(os.getenv('SMTP_PORT', 587)),
        'smtp_user': os.getenv('SMTP_USER'),
        'smtp_password': os.getenv('SMTP_PASSWORD'),
        'from_email': os.getenv('EMAIL_FROM'),
        'to_emails': os.getenv('EMAIL_TO', '').split(',')
    }
    
    return config


def test_smtp_connection():
    """Test 1: Vérification de la connexion SMTP"""
    print_section("TEST 1: Connexion SMTP")
    
    try:
        config = get_email_config()
        
        print_info(f"Serveur SMTP: {config['smtp_server']}")
        print_info(f"Port: {config['smtp_port']}")
        print_info(f"Utilisateur: {config['smtp_user']}")
        
        # Initialise le client email
        sender = EmailSender(
            smtp_server=config['smtp_server'],
            smtp_port=config['smtp_port'],
            smtp_user=config['smtp_user'],
            smtp_password=config['smtp_password'],
            from_email=config['from_email']
        )
        
        # Teste la connexion
        if sender.test_connection():
            print_success("Connexion SMTP réussie")
            return True
        else:
            print_error("Échec de la connexion SMTP")
            return False
            
    except Exception as e:
        print_error(f"Erreur: {e}")
        return False


def test_simple_text_email():
    """Test 2: Envoi d'un email texte simple"""
    print_section("TEST 2: Email Texte Simple")
    
    try:
        config = get_email_config()
        
        sender = EmailSender(
            smtp_server=config['smtp_server'],
            smtp_port=config['smtp_port'],
            smtp_user=config['smtp_user'],
            smtp_password=config['smtp_password'],
            from_email=config['from_email']
        )
        
        # Email HTML simple
        html_content = """
        <html>
            <body>
                <h1>Test Email Simple</h1>
                <p>Ceci est un email de test avec du contenu HTML basique.</p>
                <p>Heure d'envoi: {}</p>
            </body>
        </html>
        """.format(datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
        
        print_info("Envoi de l'email de test...")
        
        success = sender.send_html_email(
            to_emails=[config['smtp_user']],  # Envoie à soi-même
            subject="Test 2 - Email Simple",
            html_content=html_content
        )
        
        if success:
            print_success("Email envoyé avec succès")
            print_info(f"Vérifiez votre boîte mail: {config['smtp_user']}")
            return True
        else:
            print_error("Échec de l'envoi")
            return False
            
    except Exception as e:
        print_error(f"Erreur: {e}")
        return False


def test_styled_html_email():
    """Test 3: Envoi d'un email HTML stylé"""
    print_section("TEST 3: Email HTML Stylé")
    
    try:
        config = get_email_config()
        
        sender = EmailSender(
            smtp_server=config['smtp_server'],
            smtp_port=config['smtp_port'],
            smtp_user=config['smtp_user'],
            smtp_password=config['smtp_password'],
            from_email=config['from_email']
        )
        
        # Email HTML avec CSS intégré
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    background-color: #f5f5f5;
                    padding: 20px;
                }
                .container {
                    background-color: white;