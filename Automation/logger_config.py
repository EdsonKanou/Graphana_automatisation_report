"""
Configuration du système de logging
"""
import logging
import os
from datetime import datetime
from pathlib import Path


def setup_logger(name: str = "grafana_reporter") -> logging.Logger:
    """
    Configure et retourne un logger avec rotation de fichiers
    
    Args:
        name: Nom du logger
        
    Returns:
        Logger configuré
    """
    # Créer le dossier logs s'il n'existe pas
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Nom du fichier avec timestamp
    log_filename = log_dir / f"grafana_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Configuration du logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Éviter les duplications si le logger existe déjà
    if logger.handlers:
        return logger
    
    # Format détaillé pour les logs
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Handler pour fichier (tout)
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    
    # Handler pour console (INFO et plus)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter(
        '%(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    
    # Ajouter les handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logger initialisé - fichier: {log_filename}")
    
    return logger


def cleanup_old_logs(days: int = 30):
    """
    Supprime les logs plus vieux que X jours
    
    Args:
        days: Nombre de jours de rétention
    """
    log_dir = Path("logs")
    if not log_dir.exists():
        return
    
    cutoff_time = datetime.now().timestamp() - (days * 86400)
    deleted_count = 0
    
    for log_file in log_dir.glob("grafana_report_*.log"):
        if log_file.stat().st_mtime < cutoff_time:
            log_file.unlink()
            deleted_count += 1
    
    if deleted_count > 0:
        logger = logging.getLogger("grafana_reporter")
        logger.info(f"Nettoyage: {deleted_count} fichier(s) log supprimé(s)")