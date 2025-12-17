from typing import Literal
from bpzi_airflow_library.schemas import ProductActionPayload
from pydantic import Field, field_validator, model_validator

# Types stricts pour la validation
AccountsOrchestrator = Literal["PAASV4", "DMZRC", "ALL"]
AccountsEnvironment = Literal["NPR", "PRD", "ALL"]


class AccountsExtractIamPayload(ProductActionPayload):
    """
    Payload pour l'extraction IAM des comptes AWS.
    
    Gestion du 'ALL':
    - accounts_orchestrator = "ALL" : sélectionne PAASV4 + DMZRC
    - accounts_environment = "ALL" : sélectionne NPR + PRD
    - Les deux à "ALL" : tous les comptes sans filtrage
    
    Exemples de payloads valides:
        {"accounts_orchestrator": "PAASV4", "accounts_environment": "PRD"}
        {"accounts_orchestrator": "ALL", "accounts_environment": "NPR"}
        {"accounts_orchestrator": "ALL", "accounts_environment": "ALL"}
    """
    
    accounts_orchestrator: AccountsOrchestrator = Field(
        ...,
        description="Orchestrateur des comptes AWS (PAASV4, DMZRC ou ALL pour tous)",
        examples=["PAASV4", "DMZRC", "ALL"]
    )
    
    accounts_environment: AccountsEnvironment = Field(
        ...,
        description="Environnement des comptes (NPR=Non-Prod, PRD=Production, ALL=tous)",
        examples=["NPR", "PRD", "ALL"]
    )

    @field_validator("accounts_orchestrator")
    @classmethod
    def validate_accounts_orchestrator(cls, v: str) -> str:
        """Valide que l'orchestrateur est dans les valeurs autorisées."""
        valid_values = ["PAASV4", "DMZRC", "ALL"]
        if v not in valid_values:
            raise ValueError(
                f"Invalid accounts_orchestrator: '{v}'. "
                f"Allowed values: {', '.join(valid_values)}"
            )
        return v.upper()  # Normalisation en majuscules

    @field_validator("accounts_environment")
    @classmethod
    def validate_accounts_environment(cls, v: str) -> str:
        """Valide que l'environnement est dans les valeurs autorisées."""
        valid_values = ["NPR", "PRD", "ALL"]
        if v not in valid_values:
            raise ValueError(
                f"Invalid accounts_environment: '{v}'. "
                f"Allowed values: {', '.join(valid_values)}"
            )
        return v.upper()  # Normalisation en majuscules

    @model_validator(mode="after")
    def validate_and_log_payload(self):
        """Validation post-traitement et logging des valeurs."""
        # Log pour debugging (visible dans les logs Airflow)
        print(f"✓ Payload validated - Orchestrator: {self.accounts_orchestrator}, "
              f"Environment: {self.accounts_environment}")
        
        # Affichage du scope de l'extraction
        scope = self._get_extraction_scope()
        print(f"✓ Extraction scope: {scope}")
        
        return self

    def get_orchestrator_filter_values(self) -> list[str]:
        """
        Retourne la liste des valeurs d'orchestrateurs à filtrer en base.
        
        Returns:
            list[str]: ["PAASV4", "DMZRC"] si ALL, sinon [valeur unique]
        
        Example:
            >>> payload = AccountsExtractIamPayload(
            ...     accounts_orchestrator="ALL",
            ...     accounts_environment="PRD"
            ... )
            >>> payload.get_orchestrator_filter_values()
            ["PAASV4", "DMZRC"]
        """
        if self.accounts_orchestrator == "ALL":
            return ["PAASV4", "DMZRC"]
        return [self.accounts_orchestrator]

    def get_environment_filter_values(self) -> list[str]:
        """
        Retourne la liste des valeurs d'environnements à filtrer en base.
        
        Returns:
            list[str]: ["NPR", "PRD"] si ALL, sinon [valeur unique]
        
        Example:
            >>> payload = AccountsExtractIamPayload(
            ...     accounts_orchestrator="PAASV4",
            ...     accounts_environment="ALL"
            ... )
            >>> payload.get_environment_filter_values()
            ["NPR", "PRD"]
        """
        if self.accounts_environment == "ALL":
            return ["NPR", "PRD"]
        return [self.accounts_environment]

    def is_full_extraction(self) -> bool:
        """
        Vérifie si c'est une extraction complète (ALL + ALL).
        
        Returns:
            bool: True si les deux sont à "ALL"
        """
        return (
            self.accounts_orchestrator == "ALL" 
            and self.accounts_environment == "ALL"
        )

    def _get_extraction_scope(self) -> str:
        """
        Génère un message décrivant le scope de l'extraction.
        
        Returns:
            str: Description lisible du scope
        """
        if self.is_full_extraction():
            return "ALL accounts (all orchestrators, all environments)"
        
        orch = "all orchestrators" if self.accounts_orchestrator == "ALL" else self.accounts_orchestrator
        env = "all environments" if self.accounts_environment == "ALL" else self.accounts_environment
        
        return f"{orch} in {env}"

    def get_filter_summary(self) -> dict[str, list[str]]:
        """
        Retourne un résumé des filtres à appliquer.
        
        Returns:
            dict: Mapping des filtres SQL à appliquer
        
        Example:
            >>> payload.get_filter_summary()
            {
                "account_type": ["PAASV4"],
                "account_env": ["NPR", "PRD"]
            }
        """
        return {
            "account_type": self.get_orchestrator_filter_values(),
            "account_env": self.get_environment_filter_values()
        }