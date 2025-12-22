from typing import Literal, Optional, List
from bpzi_airflow_library.schemas import ProductActionPayload
from pydantic import Field, field_validator, model_validator
from enum import Enum

# Types stricts pour la validation
AccountsOrchestrator = Literal["PAASV4", "DMZRC", "ALL"]
AccountsEnvironment = Literal["NPR", "PRD", "ALL"]


class ExtractionMode(str, Enum):
    """Mode d'extraction IAM."""
    BY_NAMES = "by_names"  # Extraction par liste de noms de comptes
    BY_FILTERS = "by_filters"  # Extraction par filtres orchestrator/environment


class UnifiedExtractIamPayload(ProductActionPayload):
    """
    Payload unifié pour l'extraction IAM des comptes AWS.
    
    Supporte 2 modes d'extraction avec PRIORITÉ:
    
    1. Mode BY_NAMES (PRIORITAIRE):
       - Si account_names est fourni → utilise ce mode
       - Ignore accounts_orchestrator et accounts_environment
       - Exemple: {"account_names": ["ac0021000259", "ac0021000260"]}
    
    2. Mode BY_FILTERS (par défaut):
       - Si account_names est absent/vide → utilise ce mode
       - Filtre par accounts_orchestrator et/ou accounts_environment
       - Exemple: {"accounts_orchestrator": "PAASV4"}
       - Exemple: {"accounts_environment": "PRD"}
       - Exemple: {} → ALL + ALL
    
    RÈGLE DE PRIORITÉ:
    - Si les 3 paramètres sont présents → account_names est PRIORITAIRE
    - accounts_orchestrator et accounts_environment sont IGNORÉS dans ce cas
    
    Exemples de payloads valides:
        # Mode BY_NAMES
        {"account_names": ["ac0021000259"]}
        {"account_names": ["ac001", "ac002"], "accounts_orchestrator": "PAASV4"}  # orchestrator ignoré
        
        # Mode BY_FILTERS
        {}  # ALL + ALL
        {"accounts_orchestrator": "PAASV4"}  # PAASV4 + ALL
        {"accounts_environment": "PRD"}  # ALL + PRD
        {"accounts_orchestrator": "DMZRC", "accounts_environment": "NPR"}
    """
    
    # Mode BY_NAMES
    account_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "Liste des noms de comptes à extraire (mode prioritaire). "
            "Si fourni, les filtres orchestrator/environment sont ignorés."
        ),
        examples=[["ac0021000259"], ["ac001", "ac002", "ac003"]],
        min_length=1
    )
    
    # Mode BY_FILTERS
    accounts_orchestrator: AccountsOrchestrator = Field(
        default="ALL",
        description=(
            "Orchestrateur des comptes AWS (PAASV4, DMZRC ou ALL). "
            "Ignoré si account_names est fourni. Défaut: ALL"
        ),
        examples=["PAASV4", "DMZRC", "ALL"]
    )
    
    accounts_environment: AccountsEnvironment = Field(
        default="ALL",
        description=(
            "Environnement des comptes (NPR, PRD ou ALL). "
            "Ignoré si account_names est fourni. Défaut: ALL"
        ),
        examples=["NPR", "PRD", "ALL"]
    )

    @field_validator("account_names", mode="before")
    @classmethod
    def validate_account_names(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """
        Valide la liste des noms de comptes.
        - Accepte None (mode BY_FILTERS)
        - Si fourni, doit contenir au moins 1 nom valide
        """
        # None ou liste vide → mode BY_FILTERS
        if v is None or (isinstance(v, list) and len(v) == 0):
            return None
        
        # Validation des noms
        if not isinstance(v, list):
            raise ValueError(f"account_names must be a list, got {type(v)}")
        
        validated_names = []
        for idx, name in enumerate(v):
            if not isinstance(name, str):
                raise ValueError(
                    f"account_names[{idx}] must be a string, got {type(name)}"
                )
            
            cleaned_name = name.strip()
            if not cleaned_name:
                raise ValueError(
                    f"account_names[{idx}] cannot be empty or contain only whitespace"
                )
            
            validated_names.append(cleaned_name)
        
        # Suppression des doublons (optionnel)
        unique_names = list(dict.fromkeys(validated_names))
        if len(unique_names) < len(validated_names):
            print(f"⚠ Warning: Removed {len(validated_names) - len(unique_names)} duplicate account names")
        
        return unique_names

    @field_validator("accounts_orchestrator", mode="before")
    @classmethod
    def validate_accounts_orchestrator(cls, v: Optional[str]) -> str:
        """Valide et normalise l'orchestrateur."""
        if v is None or (isinstance(v, str) and not v.strip()):
            return "ALL"
        
        v = v.strip().upper()
        valid_values = ["PAASV4", "DMZRC", "ALL"]
        if v not in valid_values:
            raise ValueError(
                f"Invalid accounts_orchestrator: '{v}'. "
                f"Allowed values: {', '.join(valid_values)}"
            )
        return v

    @field_validator("accounts_environment", mode="before")
    @classmethod
    def validate_accounts_environment(cls, v: Optional[str]) -> str:
        """Valide et normalise l'environnement."""
        if v is None or (isinstance(v, str) and not v.strip()):
            return "ALL"
        
        v = v.strip().upper()
        valid_values = ["NPR", "PRD", "ALL"]
        if v not in valid_values:
            raise ValueError(
                f"Invalid accounts_environment: '{v}'. "
                f"Allowed values: {', '.join(valid_values)}"
            )
        return v

    @model_validator(mode="after")
    def validate_and_log_payload(self):
        """Validation finale et logging du mode d'extraction."""
        mode = self.get_extraction_mode()
        
        print("=" * 80)
        print("✓ Payload validated successfully")
        print(f"  - Extraction mode: {mode.value.upper()}")
        
        if mode == ExtractionMode.BY_NAMES:
            print(f"  - account_names: {self.account_names} ({len(self.account_names)} account(s))")
            
            # Avertissement si les filtres sont aussi fournis
            if self.accounts_orchestrator != "ALL" or self.accounts_environment != "ALL":
                print(f"⚠ WARNING: accounts_orchestrator and accounts_environment are IGNORED")
                print(f"  (account_names takes priority)")
        
        else:  # BY_FILTERS
            print(f"  - accounts_orchestrator: {self.accounts_orchestrator}")
            print(f"  - accounts_environment: {self.accounts_environment}")
            print(f"  - Extraction scope: {self._get_extraction_scope()}")
            
            if self.is_full_extraction():
                print(f"⚠ WARNING: Full extraction (ALL orchestrators + ALL environments)")
        
        print("=" * 80)
        
        return self

    def get_extraction_mode(self) -> ExtractionMode:
        """
        Détermine le mode d'extraction selon la présence de account_names.
        
        Returns:
            ExtractionMode.BY_NAMES si account_names fourni, sinon BY_FILTERS
        """
        if self.account_names is not None and len(self.account_names) > 0:
            return ExtractionMode.BY_NAMES
        return ExtractionMode.BY_FILTERS

    def is_by_names_mode(self) -> bool:
        """Vérifie si le mode BY_NAMES est actif."""
        return self.get_extraction_mode() == ExtractionMode.BY_NAMES

    def is_by_filters_mode(self) -> bool:
        """Vérifie si le mode BY_FILTERS est actif."""
        return self.get_extraction_mode() == ExtractionMode.BY_FILTERS

    def get_account_names(self) -> List[str]:
        """
        Retourne la liste des noms de comptes (mode BY_NAMES).
        
        Returns:
            List[str]: Liste des noms de comptes
        
        Raises:
            ValueError: Si appelé en mode BY_FILTERS
        """
        if not self.is_by_names_mode():
            raise ValueError(
                "get_account_names() can only be called in BY_NAMES mode"
            )
        return self.account_names

    def get_orchestrator_filter_values(self) -> List[str]:
        """
        Retourne la liste des orchestrateurs (mode BY_FILTERS).
        
        Returns:
            List[str]: ["PAASV4", "DMZRC"] si ALL, sinon [valeur unique]
        
        Raises:
            ValueError: Si appelé en mode BY_NAMES
        """
        if self.is_by_names_mode():
            raise ValueError(
                "get_orchestrator_filter_values() cannot be called in BY_NAMES mode. "
                "Use get_account_names() instead."
            )
        
        if self.accounts_orchestrator == "ALL":
            return ["PAASV4", "DMZRC"]
        return [self.accounts_orchestrator]

    def get_environment_filter_values(self) -> List[str]:
        """
        Retourne la liste des environnements (mode BY_FILTERS).
        
        Returns:
            List[str]: ["NPR", "PRD"] si ALL, sinon [valeur unique]
        
        Raises:
            ValueError: Si appelé en mode BY_NAMES
        """
        if self.is_by_names_mode():
            raise ValueError(
                "get_environment_filter_values() cannot be called in BY_NAMES mode. "
                "Use get_account_names() instead."
            )
        
        if self.accounts_environment == "ALL":
            return ["NPR", "PRD"]
        return [self.accounts_environment]

    def is_full_extraction(self) -> bool:
        """
        Vérifie si c'est une extraction complète en mode BY_FILTERS.
        
        Returns:
            bool: True si orchestrator=ALL et environment=ALL
        """
        return (
            self.is_by_filters_mode()
            and self.accounts_orchestrator == "ALL" 
            and self.accounts_environment == "ALL"
        )

    def _get_extraction_scope(self) -> str:
        """Génère un message décrivant le scope (mode BY_FILTERS uniquement)."""
        if self.is_full_extraction():
            return "ALL accounts (all orchestrators, all environments)"
        
        orch = "all orchestrators" if self.accounts_orchestrator == "ALL" else self.accounts_orchestrator
        env = "all environments" if self.accounts_environment == "ALL" else self.accounts_environment
        
        return f"{orch} in {env}"

    def get_filter_summary(self) -> dict:
        """
        Retourne un résumé des filtres selon le mode.
        
        Returns:
            dict: Structure différente selon le mode
        """
        mode = self.get_extraction_mode()
        
        if mode == ExtractionMode.BY_NAMES:
            return {
                "mode": "by_names",
                "account_names": self.account_names,
                "count": len(self.account_names)
            }
        else:
            return {
                "mode": "by_filters",
                "account_type": self.get_orchestrator_filter_values(),
                "account_env": self.get_environment_filter_values()
            }