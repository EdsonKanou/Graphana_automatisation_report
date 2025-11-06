"""
Module d'envoi d'emails avec support HTML
Gère l'envoi des rapports par SMTP
"""

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Optional
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class EmailSender:
    """Classe pour l'envoi d'emails"""
    
    def __init__(self, 
                 smtp_server: str,
                 smtp_port: int,
                 smtp_user: str,
                 smtp_password: str,
                 from_email: str):
        """
        Initialise le client email
        
        Args:
            smtp_server: Adresse du serveur SMTP (ex: smtp.gmail.com)
            smtp_port: Port SMTP (587 pour TLS, 465 pour SSL)
            smtp_user: Nom d'utilisateur SMTP
            smtp_password: Mot de passe ou App Password
            from_email: Adresse email d'envoi
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.from_email = from_email
        
    def send_html_email(self,
                       to_emails: List[str],
                       subject: str,
                       html_content: str,
                       cc_emails: Optional[List[str]] = None,
                       bcc_emails: Optional[List[str]] = None,
                       attachments: Optional[List[str]] = None) -> bool:
        """
        Envoie un email HTML
        
        Args:
            to_emails: Liste des destinataires principaux
            subject: Sujet de l'email
            html_content: Contenu HTML de l'email
            cc_emails: Liste des destinataires en copie (optionnel)
            bcc_emails: Liste des destinataires en copie cachée (optionnel)
            attachments: Liste des chemins de fichiers à attacher (optionnel)
            
        Returns:
            True si l'envoi a réussi, False sinon
        """
        try:
            # Crée le message
            message = MIMEMultipart('alternative')
            message['From'] = self.from_email
            message['To'] = ', '.join(to_emails)
            message['Subject'] = subject
            
            # Ajoute les CC
            if cc_emails:
                message['Cc'] = ', '.join(cc_emails)
            
            # Crée une version texte simple du contenu
            text_content = self._html_to_text(html_content)
            
            # Attache les versions texte et HTML
            part_text = MIMEText(text_content, 'plain', 'utf-8')
            part_html = MIMEText(html_content, 'html', 'utf-8')
            
            message.attach(part_text)
            message.attach(part_html)
            
            # Ajoute les pièces jointes si présentes
            if attachments:
                for file_path in attachments:
                    self._attach_file(message, file_path)
            
            # Prépare la liste complète des destinataires
            all_recipients = to_emails.copy()
            if cc_emails:
                all_recipients.extend(cc_emails)
            if bcc_emails:
                all_recipients.extend(bcc_emails)
            
            # Envoi de l'email
            self._send_message(message, all_recipients)
            
            logger.info(f"Email envoyé avec succès à {len(all_recipients)} destinataire(s)")
            return True
            
        except Exception as e:
            logger.error(f"Erreur lors de l'envoi de l'email: {e}")
            return False
    
    def _send_message(self, message: MIMEMultipart, recipients: List[str]):
        """
        Envoie le message via SMTP
        
        Args:
            message: Message MIME à envoyer
            recipients: Liste de tous les destinataires
        """
        # Détermine le type de connexion selon le port
        if self.smtp_port == 465:
            # SSL
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(message, to_addrs=recipients)
        else:
            # TLS (port 587 généralement)
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()  # Active TLS
                server.login(self.smtp_user, self.smtp_password)
                server.send_message(message, to_addrs=recipients)
    
    def _attach_file(self, message: MIMEMultipart, file_path: str):
        """
        Attache un fichier au message
        
        Args:
            message: Message MIME
            file_path: Chemin du fichier à attacher
        """
        try:
            path = Path(file_path)
            if not path.exists():
                logger.warning(f"Fichier non trouvé: {file_path}")
                return
            
            # Lit le fichier en mode binaire
            with open(file_path, 'rb') as file:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(file.read())
            
            # Encode le fichier en base64
            encoders.encode_base64(part)
            
            # Ajoute l'en-tête
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {path.name}'
            )
            
            # Attache le fichier au message
            message.attach(part)
            logger.info(f"Fichier attaché: {path.name}")
            
        except Exception as e:
            logger.error(f"Erreur lors de l'ajout de la pièce jointe {file_path}: {e}")
    
    def _html_to_text(self, html_content: str) -> str:
        """
        Convertit le HTML en texte simple (fallback pour clients email sans HTML)
        
        Args:
            html_content: Contenu HTML
            
        Returns:
            Version texte du contenu
        """
        import re
        
        # Retire les balises HTML
        text = re.sub('<[^<]+?>', '', html_content)
        
        # Décode les entités HTML communes
        text = text.replace('&nbsp;', ' ')
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = text.replace('&quot;', '"')
        
        # Nettoie les espaces multiples
        text = re.sub(r'\s+', ' ', text)
        
        return text.strip()
    
    def test_connection(self) -> bool:
        """
        Teste la connexion SMTP
        
        Returns:
            True si la connexion réussit, False sinon
        """
        try:
            if self.smtp_port == 465:
                with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                    server.login(self.smtp_user, self.smtp_password)
            else:
                with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                    server.starttls()
                    server.login(self.smtp_user, self.smtp_password)
            
            logger.info("Connexion SMTP réussie")
            return True
            
        except Exception as e:
            logger.error(f"Erreur de connexion SMTP: {e}")
            return False


class EmailTemplates:
    """Templates d'emails prédéfinis"""
    
    @staticmethod
    def daily_report_subject(dashboard_name: str = "") -> str:
        """Génère le sujet pour un rapport quotidien"""
        from datetime import datetime
        date_str = datetime.now().strftime('%d/%m/%Y')
        
        if dashboard_name:
            return f"📊 Rapport Grafana - {dashboard_name} - {date_str}"
        else:
            return f"📊 Rapport Grafana quotidien - {date_str}"
    
    @staticmethod
    def add_introduction(html_content: str, intro_text: str = "") -> str:
        """
        Ajoute une introduction au début du rapport HTML
        
        Args:
            html_content: Contenu HTML du rapport
            intro_text: Texte d'introduction personnalisé
            
        Returns:
            HTML avec introduction
        """
        if not intro_text:
            intro_text = """
            Bonjour,<br><br>
            Vous trouverez ci-dessous le rapport quotidien des métriques de votre dashboard Grafana.
            Les données ont été collectées et agrégées automatiquement.
            """
        
        intro_html = f"""
        <div style="background-color: #f8f9fa; padding: 20px; border-radius: 8px; 
                    margin-bottom: 25px; border-left: 4px solid #667eea;">
            <p style="margin: 0; line-height: 1.6;">{intro_text}</p>
        </div>
        """
        
        # Insère l'introduction après le header
        return html_content.replace(
            '</div>\n            \n            ',
            f'</div>\n            \n            {intro_html}\n            ',
            1
        )
    
    @staticmethod
    def add_footer_note(html_content: str, note_text: str = "") -> str:
        """
        Ajoute une note en pied de page
        
        Args:
            html_content: Contenu HTML du rapport
            note_text: Texte de la note
            
        Returns:
            HTML avec note ajoutée
        """
        if not note_text:
            return html_content
        
        note_html = f"""
            <div style="background-color: #fff3cd; padding: 15px; border-radius: 8px; 
                        margin-top: 25px; border-left: 4px solid #ffc107;">
                <p style="margin: 0; color: #856404;"><strong>📝 Note :</strong> {note_text}</p>
            </div>
        """
        
        # Insère avant le footer
        return html_content.replace(
            '<div class="footer">',
            f'{note_html}\n            \n            <div class="footer">',
            1
        )


def send_test_email(config: dict) -> bool:
    """
    Fonction utilitaire pour tester l'envoi d'email
    
    Args:
        config: Dictionnaire de configuration contenant les paramètres SMTP
        
    Returns:
        True si le test réussit, False sinon
    """
    sender = EmailSender(
        smtp_server=config['smtp_server'],
        smtp_port=config['smtp_port'],
        smtp_user=config['smtp_user'],
        smtp_password=config['smtp_password'],
        from_email=config['from_email']
    )
    
    # Teste la connexion
    if not sender.test_connection():
        return False
    
    # Envoie un email de test
    test_html = """
    <html>
        <body>
            <h1 style="color: #667eea;">Test de connexion</h1>
            <p>Ceci est un email de test pour vérifier la configuration SMTP.</p>
            <p>Si vous recevez cet email, la configuration est correcte ! ✅</p>
        </body>
    </html>
    """
    
    return sender.send_html_email(
        to_emails=[config['smtp_user']],  # Envoie à soi-même
        subject="Test - Configuration Email Grafana",
        html_content=test_html
    )