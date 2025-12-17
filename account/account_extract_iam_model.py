from sqlalchemy import Column, Text, Index
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class AccountExtractIAM(Base):
    """
    Modèle de la table des comptes AWS pour l'extraction IAM.
    
    Attributes:
        account_number (str): Numéro unique du compte AWS (clé primaire)
        account_id (str): ID du compte AWS
        account_name (str): Nom lisible du compte
        account_type (str): Type d'orchestrateur (PAASV4 ou DMZRC)
        account_env (str): Environnement du compte (NPR ou PRD)
    """
    
    __tablename__ = "account_extract_iam"
    
    # Colonnes
    account_number = Column(Text, nullable=False, primary_key=True)
    account_id = Column(Text, nullable=False)
    account_name = Column(Text, nullable=False)
    account_type = Column(Text, nullable=False)  # PAASV4 | DMZRC
    account_env = Column(Text, nullable=False)   # NPR | PRD
    
    # Index pour optimiser les queries fréquentes
    __table_args__ = (
        Index('idx_account_type_env', 'account_type', 'account_env'),
        Index('idx_account_type', 'account_type'),
        Index('idx_account_env', 'account_env'),
    )
    
    def __repr__(self) -> str:
        return (
            f"<AccountExtractIAM("
            f"number={self.account_number}, "
            f"name={self.account_name}, "
            f"type={self.account_type}, "
            f"env={self.account_env})>"
        )