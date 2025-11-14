"""
Gestion de l'envoi d'emails via SMTP
"""
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List
import logging

logger = logging.getLogger("grafana_reporter")


class EmailSender:
    """Gestionnaire d'envoi d'emails SMTP"""
    
    def __init__(
        self,
        smtp_server: str,
        smtp_port: int,
        username: str,
        password: str,
        use_tls: bool = True
    ):
        """
        Initialise le gestionnaire d'emails
        
        Args:
            smtp_server: Serveur SMTP
            smtp_port: Port SMTP
            username: Nom d'utilisateur
            password: Mot de passe
            use_tls: Utiliser TLS
        """
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        
        logger.info(f"EmailSender configuré pour {smtp_server}:{smtp_port}")
    
    def send_email(
        self,
        from_addr: str,
        to_addrs: List[str],
        subject: str,
        html_content: str,
        cc_addrs: List[str] = None,
        bcc_addrs: List[str] = None
    ) -> bool:
        """
        Envoie un email HTML
        
        Args:
            from_addr: Adresse expéditeur
            to_addrs: Liste des destinataires
            subject: Sujet de l'email
            html_content: Contenu HTML
            cc_addrs: Liste des destinataires en copie
            bcc_addrs: Liste des destinataires en copie cachée
            
        Returns:
            True si envoi réussi, False sinon
        """
        logger.info(f"Préparation de l'email vers {len(to_addrs)} destinataire(s)")
        
        try:
            # Créer le message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = from_addr
            msg['To'] = ', '.join(to_addrs)
            
            if cc_addrs:
                msg['Cc'] = ', '.join(cc_addrs)
            
            # Version texte simple en fallback
            text_content = self._html_to_text(html_content)
            part1 = MIMEText(text_content, 'plain', 'utf-8')
            part2 = MIMEText(html_content, 'html', 'utf-8')
            
            msg.attach(part1)
            msg.attach(part2)
            
            # Tous les destinataires
            all_recipients = to_addrs.copy()
            if cc_addrs:
                all_recipients.extend(cc_addrs)
            if bcc_addrs:
                all_recipients.extend(bcc_addrs)
            
            # Connexion et envoi
            logger.debug(f"Connexion à {self.smtp_server}:{self.smtp_port}")
            
            with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=30) as server:
                if self.use_tls:
                    server.starttls()
                    logger.debug("TLS activé")
                
                server.login(self.username, self.password)
                logger.debug("Authentification réussie")
                
                server.send_message(msg)
                logger.info(f"✅ Email envoyé avec succès à {len(all_recipients)} destinataire(s)")
                
                return True
                
        except smtplib.SMTPAuthenticationError as e:
            logger.error(f"❌ Erreur d'authentification SMTP: {e}")
            return False
        except smtplib.SMTPException as e:
            logger.error(f"❌ Erreur SMTP: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Erreur lors de l'envoi de l'email: {e}")
            return False
    
    @staticmethod
    def _html_to_text(html: str) -> str:
        """
        Convertit le HTML en texte simple (basique)
        
        Args:
            html: Contenu HTML
            
        Returns:
            Texte simple
        """
        # Conversion très basique - pour un vrai projet, utiliser html2text ou BeautifulSoup
        import re
        
        # Supprimer les scripts et styles
        text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        
        # Remplacer les balises HTML communes
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<div[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
        
        # Supprimer toutes les autres balises
        text = re.sub(r'<[^>]+>', '', text)
        
        # Nettoyer les espaces multiples
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r'[ \t]+', ' ', text)
        
        return text.strip()
    
    def test_connection(self) -> bool:
        """
        Teste la connexion SMTP
        
        Returns:
            True si connexion réussie, False sinon
        """
        logger.info("Test de connexion SMTP...")
        
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_port, timeout=10) as server:
                if self.use_tls:
                    server.starttls()
                server.login(self.username, self.password)
                logger.info("✅ Connexion SMTP réussie")
                return True
        except Exception as e:
            logger.error(f"❌ Échec de la connexion SMTP: {e}")
            return False