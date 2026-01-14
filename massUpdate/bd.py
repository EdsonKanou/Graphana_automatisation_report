-- ==========================================
-- MIGRATION SQL - Ajouter les colonnes manquantes
-- Table: account_extract_iam (ou le nom de ta table)
-- ==========================================

-- 1. Colonnes pour les configurations updatables
ALTER TABLE account_extract_iam 
ADD COLUMN IF NOT EXISTS subnet_count INTEGER DEFAULT 3;

ALTER TABLE account_extract_iam 
ADD COLUMN IF NOT EXISTS enable_monitoring BOOLEAN DEFAULT false;

ALTER TABLE account_extract_iam 
ADD COLUMN IF NOT EXISTS tags TEXT[] DEFAULT '{}';
-- Si TEXT[] ne fonctionne pas (selon ta version PostgreSQL), utilise :
-- ADD COLUMN IF NOT EXISTS tags TEXT DEFAULT '';

-- 2. Colonnes pour le tracking de déploiement
ALTER TABLE account_extract_iam 
ADD COLUMN IF NOT EXISTS deployment_status VARCHAR(50) DEFAULT 'not_started';

ALTER TABLE account_extract_iam 
ADD COLUMN IF NOT EXISTS last_update_requested TIMESTAMP;

ALTER TABLE account_extract_iam 
ADD COLUMN IF NOT EXISTS last_deployment_date TIMESTAMP;

ALTER TABLE account_extract_iam 
ADD COLUMN IF NOT EXISTS deployment_error TEXT;

ALTER TABLE account_extract_iam 
ADD COLUMN IF NOT EXISTS schematics_activity_id VARCHAR(100);

-- 3. Ajouter des commentaires pour documentation
COMMENT ON COLUMN account_extract_iam.subnet_count IS 'Number of subnets to create for this account';
COMMENT ON COLUMN account_extract_iam.enable_monitoring IS 'Enable monitoring flag for the account';
COMMENT ON COLUMN account_extract_iam.tags IS 'Tags associated with the account';
COMMENT ON COLUMN account_extract_iam.deployment_status IS 'Current deployment status: not_started, pending, planning, applying, deployed, plan_completed, deployment_failed, etc.';
COMMENT ON COLUMN account_extract_iam.last_update_requested IS 'Timestamp when the last update was requested via DAG';
COMMENT ON COLUMN account_extract_iam.last_deployment_date IS 'Timestamp when the last deployment completed';
COMMENT ON COLUMN account_extract_iam.deployment_error IS 'Error message if deployment failed';
COMMENT ON COLUMN account_extract_iam.schematics_activity_id IS 'IBM Schematics activity/job ID for tracking';

-- ==========================================
-- VÉRIFICATION - Voir les colonnes créées
-- ==========================================

SELECT 
    column_name, 
    data_type, 
    is_nullable, 
    column_default
FROM information_schema.columns
WHERE table_name = 'account_extract_iam'
ORDER BY ordinal_position;

-- ==========================================
-- INDEX RECOMMANDÉS (pour performance)
-- ==========================================

-- Index sur deployment_status pour filtrer rapidement
CREATE INDEX IF NOT EXISTS idx_account_extract_iam_deployment_status 
ON account_extract_iam(deployment_status);

-- Index sur account_name (si pas déjà existant)
CREATE INDEX IF NOT EXISTS idx_account_extract_iam_account_name 
ON account_extract_iam(account_name);

-- Index composite pour recherches fréquentes
CREATE INDEX IF NOT EXISTS idx_account_extract_iam_status_date 
ON account_extract_iam(deployment_status, last_deployment_date);

-- ==========================================
-- DONNÉES DE TEST (optionnel)
-- ==========================================

-- Exemple d'update pour tester
UPDATE account_extract_iam
SET 
    subnet_count = 5,
    enable_monitoring = true,
    tags = ARRAY['production', 'critical']::TEXT[],  -- Pour PostgreSQL avec TEXT[]
    -- tags = 'production,critical',  -- Alternative si TEXT simple
    deployment_status = 'not_started'
WHERE account_name = 'TON_COMPTE_TEST';

-- Vérifier les données
SELECT 
    account_name,
    iam_version,
    subnet_count,
    enable_monitoring,
    tags,
    deployment_status,
    last_update_requested
FROM account_extract_iam
WHERE account_name = 'TON_COMPTE_TEST';




"""
Modèle SQLAlchemy pour la table account_extract_iam
Ajoute les colonnes pour le tracking de déploiement et les configurations
"""

from sqlalchemy import Column, String, Integer, Boolean, Text, TIMESTAMP, ARRAY
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from typing import Optional, List

# Si tu as déjà une base, utilise-la
# Base = declarative_base()

class AccountExtractIAM(Base):
    """
    Modèle pour les comptes avec extraction IAM
    
    Colonnes existantes (à garder):
        - account_name
        - account_number
        - account_type
        - environment_type
        - code_bu
        - iam_version (déjà existante)
    
    Nouvelles colonnes ajoutées:
        - subnet_count: Nombre de subnets
        - enable_monitoring: Flag monitoring
        - tags: Tags du compte
        - deployment_status: Statut du déploiement
        - last_update_requested: Date de la dernière demande de MAJ
        - last_deployment_date: Date du dernier déploiement
        - deployment_error: Message d'erreur si échec
        - schematics_activity_id: ID de l'activité Schematics
    """
    
    __tablename__ = 'account_extract_iam'
    
    # ==========================================
    # COLONNES EXISTANTES (à garder telles quelles)
    # ==========================================
    
    # Remplace par tes colonnes existantes réelles
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_name = Column(String(255), nullable=False, index=True)
    account_number = Column(String(100), nullable=False)
    account_type = Column(String(50), nullable=False)
    environment_type = Column(String(50), nullable=False)
    code_bu = Column(String(50), nullable=False)
    
    # Colonne qui existait déjà
    iam_version = Column(String(50), nullable=True)
    
    # ==========================================
    # NOUVELLES COLONNES - Configuration
    # ==========================================
    
    subnet_count = Column(
        Integer, 
        nullable=True, 
        default=3,
        comment="Number of subnets to create for this account"
    )
    
    enable_monitoring = Column(
        Boolean, 
        nullable=True, 
        default=False,
        comment="Enable monitoring flag for the account"
    )
    
    # Option 1: Si PostgreSQL supporte ARRAY
    tags = Column(
        ARRAY(String), 
        nullable=True,
        default=[],
        comment="Tags associated with the account"
    )
    
    # Option 2: Si ARRAY ne fonctionne pas, utilise Text avec séparateur
    # tags = Column(
    #     Text,
    #     nullable=True,
    #     default='',
    #     comment="Tags associated with the account (comma-separated)"
    # )
    
    # ==========================================
    # NOUVELLES COLONNES - Tracking déploiement
    # ==========================================
    
    deployment_status = Column(
        String(50), 
        nullable=True, 
        default='not_started',
        index=True,
        comment="Current deployment status"
    )
    # Valeurs possibles:
    # - not_started: Jamais déployé
    # - pending: MAJ demandée, pas encore traitée
    # - planning: Terraform plan en cours
    # - applying: Terraform apply en cours
    # - deployed: Déployé avec succès
    # - plan_completed: Plan OK (mode plan_only)
    # - update_failed: Échec de MAJ en BD
    # - workspace_not_found: Workspace Schematics introuvable
    # - deployment_failed: Échec du déploiement
    
    last_update_requested = Column(
        TIMESTAMP,
        nullable=True,
        comment="Timestamp when the last update was requested via DAG"
    )
    
    last_deployment_date = Column(
        TIMESTAMP,
        nullable=True,
        comment="Timestamp when the last deployment completed"
    )
    
    deployment_error = Column(
        Text,
        nullable=True,
        comment="Error message if deployment failed"
    )
    
    schematics_activity_id = Column(
        String(100),
        nullable=True,
        comment="IBM Schematics activity/job ID for tracking"
    )
    
    # ==========================================
    # MÉTHODES UTILITAIRES
    # ==========================================
    
    def __repr__(self):
        return (
            f"<AccountExtractIAM("
            f"account_name='{self.account_name}', "
            f"iam_version='{self.iam_version}', "
            f"deployment_status='{self.deployment_status}'"
            f")>"
        )
    
    def to_dict(self) -> dict:
        """Convertir l'objet en dictionnaire"""
        return {
            'id': self.id,
            'account_name': self.account_name,
            'account_number': self.account_number,
            'account_type': self.account_type,
            'environment_type': self.environment_type,
            'code_bu': self.code_bu,
            'iam_version': self.iam_version,
            'subnet_count': self.subnet_count,
            'enable_monitoring': self.enable_monitoring,
            'tags': self.tags,
            'deployment_status': self.deployment_status,
            'last_update_requested': self.last_update_requested,
            'last_deployment_date': self.last_deployment_date,
            'deployment_error': self.deployment_error,
            'schematics_activity_id': self.schematics_activity_id,
        }
    
    @property
    def is_deployable(self) -> bool:
        """Vérifie si le compte peut être déployé"""
        return self.deployment_status not in [
            'planning', 
            'applying'
        ]
    
    @property
    def has_pending_changes(self) -> bool:
        """Vérifie si le compte a des changements en attente"""
        return self.deployment_status == 'pending'
    
    @property
    def deployment_failed(self) -> bool:
        """Vérifie si le dernier déploiement a échoué"""
        return self.deployment_status in [
            'update_failed',
            'workspace_not_found', 
            'deployment_failed'
        ]


# ==========================================
# HELPER POUR GÉRER LES TAGS (si utilisation de Text)
# ==========================================

class AccountExtractIAMWithTextTags(AccountExtractIAM):
    """
    Version alternative si tu utilises Text pour les tags
    au lieu de ARRAY
    """
    
    # Override la colonne tags
    tags = Column(
        Text,
        nullable=True,
        default='',
        comment="Tags associated with the account (comma-separated)"
    )
    
    @property
    def tags_list(self) -> List[str]:
        """Retourne les tags sous forme de liste"""
        if not self.tags:
            return []
        return [tag.strip() for tag in self.tags.split(',') if tag.strip()]
    
    @tags_list.setter
    def tags_list(self, value: List[str]):
        """Définit les tags à partir d'une liste"""
        self.tags = ','.join(value) if value else ''
    
    def to_dict(self) -> dict:
        """Override pour retourner tags comme liste"""
        data = super().to_dict()
        data['tags'] = self.tags_list
        return data


# ==========================================
# EXEMPLE D'UTILISATION
# ==========================================

if __name__ == "__main__":
    # Exemple de création d'un compte
    account = AccountExtractIAM(
        account_name="ACC-TEST-001",
        account_number="1234567890",
        account_type="WKLAPP",
        environment_type="PRD",
        code_bu="BU01",
        iam_version="2.0",
        subnet_count=5,
        enable_monitoring=True,
        tags=["production", "critical"],
        deployment_status="not_started"
    )
    
    print(account)
    print(account.to_dict())
    
    # Exemple de mise à jour
    account.deployment_status = "pending"
    account.last_update_requested = datetime.utcnow()
    
    # Vérifications
    print(f"Deployable: {account.is_deployable}")
    print(f"Has pending changes: {account.has_pending_changes}")
    print(f"Deployment failed: {account.deployment_failed}")