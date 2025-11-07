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
                    border-radius: 10px;
                    padding: 30px;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.1);
                }
                .header {
                    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    color: white;
                    padding: 20px;
                    border-radius: 8px;
                    margin-bottom: 20px;
                }
                .content {
                    color: #333;
                    line-height: 1.6;
                }
                .button {
                    display: inline-block;
                    padding: 12px 24px;
                    background-color: #667eea;
                    color: white;
                    text-decoration: none;
                    border-radius: 5px;
                    margin-top: 20px;
                }
                .footer {
                    margin-top: 30px;
                    text-align: center;
                    color: #666;
                    font-size: 0.9em;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>🎨 Test Email HTML Stylé</h1>
                </div>
                <div class="content">
                    <p>Bonjour,</p>
                    <p>Ceci est un email de test avec du style CSS avancé.</p>
                    <p>Ce type d'email peut contenir:</p>
                    <ul>
                        <li>Des couleurs personnalisées</li>
                        <li>Des dégradés</li>
                        <li>Des ombres</li>
                        <li>Des boutons stylés</li>
                    </ul>
                    <a href="#" class="button">Bouton d'exemple</a>
                </div>
                <div class="footer">
                    <p>Email de test envoyé le {}</p>
                </div>
            </div>
        </body>
        </html>
        """.format(datetime.now().strftime('%d/%m/%Y à %H:%M:%S'))
        
        print_info("Envoi de l'email stylé...")
        
        success = sender.send_html_email(
            to_emails=[config['smtp_user']],
            subject="Test 3 - Email HTML Stylé 🎨",
            html_content=html_content
        )
        
        if success:
            print_success("Email stylé envoyé avec succès")
            return True
        else:
            print_error("Échec de l'envoi")
            return False
            
    except Exception as e:
        print_error(f"Erreur: {e}")
        return False


def test_email_with_table():
    """Test 4: Email avec un tableau de données"""
    print_section("TEST 4: Email avec Tableau")
    
    try:
        config = get_email_config()
        
        sender = EmailSender(
            smtp_server=config['smtp_server'],
            smtp_port=config['smtp_port'],
            smtp_user=config['smtp_user'],
            smtp_password=config['smtp_password'],
            from_email=config['from_email']
        )
        
        # Email avec tableau
        html_content = """
        <!DOCTYPE html>
        <html>
        <head>
            <style>
                body {
                    font-family: Arial, sans-serif;
                    padding: 20px;
                    background-color: #f5f5f5;
                }
                .container {
                    background-color: white;
                    padding: 30px;
                    border-radius: 10px;
                }
                table {
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                }
                th {
                    background-color: #667eea;
                    color: white;
                    padding: 12px;
                    text-align: left;
                }
                td {
                    padding: 12px;
                    border-bottom: 1px solid #ddd;
                }
                tr:hover {
                    background-color: #f5f5f5;
                }
            </style>
        </head>
        <body>
            <div class="container">
                <h1>📊 Exemple de Tableau de Données</h1>
                <p>Voici un exemple de tableau qui pourrait contenir vos métriques Grafana:</p>
                
                <table>
                    <thead>
                        <tr>
                            <th>Métrique</th>
                            <th>Valeur</th>
                            <th>Unité</th>
                            <th>Statut</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr>
                            <td>CPU Usage</td>
                            <td>45.2</td>
                            <td>%</td>
                            <td>✅ Normal</td>
                        </tr>
                        <tr>
                            <td>Memory Usage</td>
                            <td>67.8</td>
                            <td>%</td>
                            <td>⚠️ Attention</td>
                        </tr>
                        <tr>
                            <td>Disk Usage</td>
                            <td>89.3</td>
                            <td>%</td>
                            <td>❌ Critique</td>
                        </tr>
                        <tr>
                            <td>Network Traffic</td>
                            <td>1.2</td>
                            <td>GB/s</td>
                            <td>✅ Normal</td>
                        </tr>
                    </tbody>
                </table>
                
                <p style="color: #666; font-size: 0.9em;">
                    Généré le {}
                </p>
            </div>
        </body>
        </html>
        """.format(datetime.now().strftime('%d/%m/%Y à %H:%M:%S'))
        
        print_info("Envoi de l'email avec tableau...")
        
        success = sender.send_html_email(
            to_emails=[config['smtp_user']],
            subject="Test 4 - Email avec Tableau 📊",
            html_content=html_content
        )
        
        if success:
            print_success("Email avec tableau envoyé avec succès")
            return True
        else:
            print_error("Échec de l'envoi")
            return False
            
    except Exception as e:
        print_error(f"Erreur: {e}")
        return False


def test_multiple_recipients():
    """Test 5: Email à plusieurs destinataires"""
    print_section("TEST 5: Email Multiple Destinataires")
    
    try:
        config = get_email_config()
        
        sender = EmailSender(
            smtp_server=config['smtp_server'],
            smtp_port=config['smtp_port'],
            smtp_user=config['smtp_user'],
            smtp_password=config['smtp_password'],
            from_email=config['from_email']
        )
        
        # Liste des destinataires depuis .env
        recipients = config['to_emails']
        print_info(f"Destinataires configurés: {len(recipients)}")
        for i, email in enumerate(recipients, 1):
            print(f"  {i}. {email.strip()}")
        
        html_content = """
        <html>
            <body style="font-family: Arial, sans-serif; padding: 20px;">
                <h1>Test - Envoi Multiple</h1>
                <p>Cet email a été envoyé à plusieurs destinataires simultanément.</p>
                <p>Date: {}</p>
            </body>
        </html>
        """.format(datetime.now().strftime('%d/%m/%Y %H:%M:%S'))
        
        print_info("Envoi aux destinataires...")
        
        success = sender.send_html_email(
            to_emails=[email.strip() for email in recipients],
            subject="Test 5 - Email Multiple Destinataires",
            html_content=html_content
        )
        
        if success:
            print_success(f"Email envoyé à {len(recipients)} destinataire(s)")
            return True
        else:
            print_error("Échec de l'envoi")
            return False
            
    except Exception as e:
        print_error(f"Erreur: {e}")
        return False


def test_email_with_cc_bcc():
    """Test 6: Email avec CC et BCC"""
    print_section("TEST 6: Email avec CC et BCC")
    
    try:
        config = get_email_config()
        
        sender = EmailSender(
            smtp_server=config['smtp_server'],
            smtp_port=config['smtp_port'],
            smtp_user=config['smtp_user'],
            smtp_password=config['smtp_password'],
            from_email=config['from_email']
        )
        
        html_content = """
        <html>
            <body style="font-family: Arial; padding: 20px;">
                <h1>Test CC et BCC</h1>
                <p>Cet email teste l'envoi avec destinataires en copie (CC) et copie cachée (BCC).</p>
            </body>
        </html>
        """
        
        print_info("Envoi avec CC et BCC...")
        
        # Pour le test, on utilise le même email pour TO, CC et BCC
        success = sender.send_html_email(
            to_emails=[config['smtp_user']],
            subject="Test 6 - Email avec CC/BCC",
            html_content=html_content,
            cc_emails=[config['smtp_user']],  # Même email en CC pour test
            bcc_emails=None  # BCC ne sera pas visible
        )
        
        if success:
            print_success("Email envoyé avec CC")
            print_info("Vérifiez que vous apparaissez en CC dans l'email reçu")
            return True
        else:
            print_error("Échec de l'envoi")
            return False
            
    except Exception as e:
        print_error(f"Erreur: {e}")
        return False


def test_email_templates():
    """Test 7: Test des templates d'email"""
    print_section("TEST 7: Templates d'Email")
    
    try:
        config = get_email_config()
        
        sender = EmailSender(
            smtp_server=config['smtp_server'],
            smtp_port=config['smtp_port'],
            smtp_user=config['smtp_user'],
            smtp_password=config['smtp_password'],
            from_email=config['from_email']
        )
        
        # Génère le sujet avec le template
        subject = EmailTemplates.daily_report_subject("Dashboard Test")
        print_info(f"Sujet généré: {subject}")
        
        # Crée un rapport HTML basique
        base_html = """
        <html>
            <body style="padding: 20px;">
                <h1>Rapport de Test</h1>
                <p>Contenu du rapport...</p>
            </body>
        </html>
        """
        
        # Ajoute l'introduction
        html_with_intro = EmailTemplates.add_introduction(
            base_html,
            "Ceci est une introduction personnalisée pour tester le template."
        )
        
        # Ajoute une note
        html_final = EmailTemplates.add_footer_note(
            html_with_intro,
            "Ceci est un email de test - vous pouvez l'ignorer."
        )
        
        print_info("Envoi de l'email avec template...")
        
        success = sender.send_html_email(
            to_emails=[config['smtp_user']],
            subject=subject,
            html_content=html_final
        )
        
        if success:
            print_success("Email avec template envoyé")
            return True
        else:
            print_error("Échec de l'envoi")
            return False
            
    except Exception as e:
        print_error(f"Erreur: {e}")
        return False


def test_different_smtp_ports():
    """Test 8: Test des différents ports SMTP"""
    print_section("TEST 8: Test des Ports SMTP")
    
    config = get_email_config()
    
    print_info("Test des ports SMTP disponibles:")
    print("  • Port 587 (TLS) - Recommandé")
    print("  • Port 465 (SSL)")
    print("  • Port 25 (Non sécurisé - généralement bloqué)")
    
    current_port = config['smtp_port']
    print_info(f"\nPort actuellement configuré: {current_port}")
    
    if current_port == 587:
        print_success("Port 587 (TLS) - Configuration recommandée ✓")
    elif current_port == 465:
        print_info("Port 465 (SSL) - Configuration alternative valide")
    else:
        print_error(f"Port {current_port} - Non standard, vérifiez votre configuration")
    
    return True


def test_email_validation():
    """Test 9: Validation des adresses email"""
    print_section("TEST 9: Validation des Emails")
    
    config = get_email_config()
    
    print_info("Vérification des adresses email configurées:")
    
    all_valid = True
    
    # Validation simple de l'email
    import re
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    
    # Vérifie l'email d'envoi
    if re.match(email_pattern, config['from_email']):
        print_success(f"Email FROM valide: {config['from_email']}")
    else:
        print_error(f"Email FROM invalide: {config['from_email']}")
        all_valid = False
    
    # Vérifie les destinataires
    for email in config['to_emails']:
        email = email.strip()
        if re.match(email_pattern, email):
            print_success(f"Email TO valide: {email}")
        else:
            print_error(f"Email TO invalide: {email}")
            all_valid = False
    
    return all_valid


def interactive_test():
    """Test 10: Mode interactif pour tests personnalisés"""
    print_section("TEST 10: Mode Interactif")
    
    print("\n🔧 Mode Interactif - Personnalisation de l'email de test\n")
    
    try:
        # Demande les paramètres
        to_email = input("Email destinataire (Entrée pour utiliser config): ").strip()
        subject = input("Sujet de l'email (Entrée pour défaut): ").strip()
        message = input("Message à envoyer (Entrée pour défaut): ").strip()
        
        config = get_email_config()
        
        if not to_email:
            to_email = config['smtp_user']
        
        if not subject:
            subject = "Test Interactif - Email Personnalisé"
        
        if not message:
            message = "Ceci est un message de test envoyé en mode interactif."
        
        sender = EmailSender(
            smtp_server=config['smtp_server'],
            smtp_port=config['smtp_port'],
            smtp_user=config['smtp_user'],
            smtp_password=config['smtp_password'],
            from_email=config['from_email']
        )
        
        html_content = f"""
        <html>
            <body style="font-family: Arial; padding: 20px;">
                <h1>Email Personnalisé</h1>
                <p>{message}</p>
                <hr>
                <p style="color: #666; font-size: 0.9em;">
                    Envoyé le {datetime.now().strftime('%d/%m/%Y à %H:%M:%S')}
                </p>
            </body>
        </html>
        """
        
        print_info("\nEnvoi de l'email personnalisé...")
        
        success = sender.send_html_email(
            to_emails=[to_email],
            subject=subject,
            html_content=html_content
        )
        
        if success:
            print_success("Email personnalisé envoyé")
            return True
        else:
            print_error("Échec de l'envoi")
            return False
            
    except Exception as e:
        print_error(f"Erreur: {e}")
        return False


def main():
    """Fonction principale - Lance tous les tests"""
    
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║         TESTS EMAIL - Suite Complète                    ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Vérifie que les variables d'environnement sont définies
    required_vars = ['SMTP_SERVER', 'SMTP_PORT', 'SMTP_USER', 'SMTP_PASSWORD', 'EMAIL_FROM']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print_error(f"Variables manquantes dans .env: {', '.join(missing_vars)}")
        print_info("Veuillez configurer le fichier .env avant de lancer les tests")
        sys.exit(1)
    
    # Menu de sélection
    if len(sys.argv) > 1:
        test_num = sys.argv[1]
        
        if test_num == '1':
            return test_smtp_connection()
        elif test_num == '2':
            return test_simple_text_email()
        elif test_num == '3':
            return test_styled_html_email()
        elif test_num == '4':
            return test_email_with_table()
        elif test_num == '5':
            return test_multiple_recipients()
        elif test_num == '6':
            return test_email_with_cc_bcc()
        elif test_num == '7':
            return test_email_templates()
        elif test_num == '8':
            return test_different_smtp_ports()
        elif test_num == '9':
            return test_email_validation()
        elif test_num == '10' or test_num == 'interactive':
            return interactive_test()
    
    # Lance tous les tests
    results = {}
    
    results['connection'] = test_smtp_connection()
    if not results['connection']:
        print_error("Impossible de continuer sans connexion SMTP valide")
        sys.exit(1)
    
    results['simple_email'] = test_simple_text_email()
    results['styled_email'] = test_styled_html_email()
    results['table_email'] = test_email_with_table()
    results['multiple_recipients'] = test_multiple_recipients()
    results['cc_bcc'] = test_email_with_cc_bcc()
    results['templates'] = test_email_templates()
    results['ports'] = test_different_smtp_ports()
    results['validation'] = test_email_validation()
    
    # Résumé
    print_section("RÉSUMÉ DES TESTS")
    
    total = len(results)
    passed = sum(1 for v in results.values() if v)
    
    print(f"\n  Tests réussis: {passed}/{total}")
    
    print("\n  Détails:")
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"    {status} - {test_name}")
    
    print("\n" + "=" * 80 + "\n")
    
    # Propose le mode interactif
    print("💡 Pour lancer un test personnalisé, utilisez: python test_email.py interactive")
    
    return passed == total


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)