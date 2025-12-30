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
    
    RÈGLES DE VALIDATION (EXCLUSION MUTUELLE):
    
    1. Mode BY_NAMES:
       - Fournir UNIQUEMENT account_names
       - NE PAS fournir accounts_orchestrator ni accounts_environment
       - Exemple: {"account_names": ["ac0021000259", "ac0021000260"]}
    
    2. Mode BY_FILTERS:
       - Fournir accounts_orchestrator et/ou accounts_environment
       - NE PAS fournir account_names
       - Si un seul paramètre fourni, l'autre passe à "ALL"
       - Exemples:
         * {"accounts_orchestrator": "PAASV4"} → PAASV4 + ALL
         * {"accounts_environment": "PRD"} → ALL + PRD
         * {"accounts_orchestrator": "DMZRC", "accounts_environment": "NPR"} → DMZRC + NPR
    
    ERREURS:
    - ❌ Payload vide: {}
    - ❌ account_names + accounts_orchestrator
    - ❌ account_names + accounts_environment
    - ❌ account_names + accounts_orchestrator + accounts_environment
    
    Exemples de payloads valides:
        # Mode BY_NAMES
        {"account_names": ["ac0021000259"]}
        {"account_names": ["ac001", "ac002"]}
        
        # Mode BY_FILTERS
        {"accounts_orchestrator": "PAASV4"}
        {"accounts_environment": "PRD"}
        {"accounts_orchestrator": "DMZRC", "accounts_environment": "NPR"}
    """
    
    # Mode BY_NAMES (exclusif)
    account_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "Liste des noms de comptes à extraire (mode BY_NAMES exclusif). "
            "INCOMPATIBLE avec accounts_orchestrator et accounts_environment."
        ),
        examples=[["ac0021000259"], ["ac001", "ac002", "ac003"]],
        min_length=1
    )
    
    # Mode BY_FILTERS (exclusif)
    accounts_orchestrator: Optional[AccountsOrchestrator] = Field(
        default=None,
        description=(
            "Orchestrateur des comptes AWS (PAASV4, DMZRC ou ALL). "
            "INCOMPATIBLE avec account_names. "
            "Si absent et accounts_environment fourni, passe automatiquement à ALL."
        ),
        examples=["PAASV4", "DMZRC", "ALL"]
    )
    
    accounts_environment: Optional[AccountsEnvironment] = Field(
        default=None,
        description=(
            "Environnement des comptes (NPR, PRD ou ALL). "
            "INCOMPATIBLE avec account_names. "
            "Si absent et accounts_orchestrator fourni, passe automatiquement à ALL."
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
    def validate_accounts_orchestrator(cls, v: Optional[str]) -> Optional[str]:
        """
        Valide et normalise l'orchestrateur.
        Retourne None si absent (sera géré par model_validator).
        """
        # Si None ou vide, on garde None (pas de défaut automatique ici)
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        
        # Normalisation en majuscules
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
    def validate_accounts_environment(cls, v: Optional[str]) -> Optional[str]:
        """
        Valide et normalise l'environnement.
        Retourne None si absent (sera géré par model_validator).
        """
        # Si None ou vide, on garde None (pas de défaut automatique ici)
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        
        # Normalisation en majuscules
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
        
        # accounts_orchestrator ne peut pas être None ici (garanti par model_validator)
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
        
        # accounts_environment ne peut pas être None ici (garanti par model_validator)
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
                "account_env": self.get_environment_filter_values(),
                "orchestrator": self.accounts_orchestrator,
                "environment": self.accounts_environment
            }
            
            


from typing import Literal, Optional, List
from bpzi_airflow_library.schemas import ProductActionPayload
from pydantic import Field, field_validator, model_validator
from enum import Enum

# Strict types for validation
AccountsOrchestrator = Literal["PAASV4", "DMZRC", "ALL"]
AccountsEnvironment = Literal["NPR", "PRD", "ALL"]


class ExtractionMode(str, Enum):
    """IAM extraction mode."""
    BY_NAMES = "by_names"  # Extraction by list of account names
    BY_FILTERS = "by_filters"  # Extraction by orchestrator/environment filters


class UnifiedExtractIamPayload(ProductActionPayload):
    """
    Unified payload for AWS account IAM extraction.
    
    VALIDATION RULES (MUTUAL EXCLUSION):
    
    1. BY_NAMES Mode:
       - Provide ONLY account_names
       - DO NOT provide accounts_orchestrator or accounts_environment
       - Example: {"account_names": ["ac0021000259", "ac0021000260"]}
    
    2. BY_FILTERS Mode:
       - Provide accounts_orchestrator and/or accounts_environment
       - DO NOT provide account_names
       - If only one parameter is provided, the other defaults to "ALL"
       - Examples:
         * {"accounts_orchestrator": "PAASV4"} → PAASV4 + ALL
         * {"accounts_environment": "PRD"} → ALL + PRD
         * {"accounts_orchestrator": "DMZRC", "accounts_environment": "NPR"} → DMZRC + NPR
    
    ERRORS:
    - ❌ Empty payload: {}
    - ❌ account_names + accounts_orchestrator
    - ❌ account_names + accounts_environment
    - ❌ account_names + accounts_orchestrator + accounts_environment
    
    Valid payload examples:
        # BY_NAMES Mode
        {"account_names": ["ac0021000259"]}
        {"account_names": ["ac001", "ac002"]}
        
        # BY_FILTERS Mode
        {"accounts_orchestrator": "PAASV4"}
        {"accounts_environment": "PRD"}
        {"accounts_orchestrator": "DMZRC", "accounts_environment": "NPR"}
    """
    
    # BY_NAMES Mode (exclusive)
    account_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of account names to extract (exclusive BY_NAMES mode). "
            "INCOMPATIBLE with accounts_orchestrator and accounts_environment."
        ),
        examples=[["ac0021000259"], ["ac001", "ac002", "ac003"]],
        min_length=1
    )
    
    # BY_FILTERS Mode (exclusive)
    accounts_orchestrator: Optional[AccountsOrchestrator] = Field(
        default=None,
        description=(
            "AWS account orchestrator (PAASV4, DMZRC or ALL). "
            "INCOMPATIBLE with account_names. "
            "If absent and accounts_environment is provided, automatically defaults to ALL."
        ),
        examples=["PAASV4", "DMZRC", "ALL"]
    )
    
    accounts_environment: Optional[AccountsEnvironment] = Field(
        default=None,
        description=(
            "Account environment (NPR, PRD or ALL). "
            "INCOMPATIBLE with account_names. "
            "If absent and accounts_orchestrator is provided, automatically defaults to ALL."
        ),
        examples=["NPR", "PRD", "ALL"]
    )

    @field_validator("account_names", mode="before")
    @classmethod
    def validate_account_names(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """
        Validates the list of account names.
        - Accepts None (BY_FILTERS mode)
        - If provided, must contain at least 1 valid name
        """
        # None or empty list → BY_FILTERS mode
        if v is None or (isinstance(v, list) and len(v) == 0):
            return None
        
        # Names validation
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
        
        # Remove duplicates (optional)
        unique_names = list(dict.fromkeys(validated_names))
        if len(unique_names) < len(validated_names):
            print(f"⚠ Warning: Removed {len(validated_names) - len(unique_names)} duplicate account names")
        
        return unique_names

    @field_validator("accounts_orchestrator", mode="before")
    @classmethod
    def validate_accounts_orchestrator(cls, v: Optional[str]) -> Optional[str]:
        """
        Validates and normalizes the orchestrator.
        Returns None if absent (will be handled by model_validator).
        """
        # If None or empty, keep None (no automatic default here)
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        
        # Normalize to uppercase
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
    def validate_accounts_environment(cls, v: Optional[str]) -> Optional[str]:
        """
        Validates and normalizes the environment.
        Returns None if absent (will be handled by model_validator).
        """
        # If None or empty, keep None (no automatic default here)
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        
        # Normalize to uppercase
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
        """Final validation and logging of extraction mode."""
        mode = self.get_extraction_mode()
        
        print("=" * 80)
        print("✓ Payload validated successfully")
        print(f"  - Extraction mode: {mode.value.upper()}")
        
        if mode == ExtractionMode.BY_NAMES:
            print(f"  - account_names: {self.account_names} ({len(self.account_names)} account(s))")
            
            # Warning if filters are also provided
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
        Determines the extraction mode based on the presence of account_names.
        
        Returns:
            ExtractionMode.BY_NAMES if account_names is provided, otherwise BY_FILTERS
        """
        if self.account_names is not None and len(self.account_names) > 0:
            return ExtractionMode.BY_NAMES
        return ExtractionMode.BY_FILTERS

    def is_by_names_mode(self) -> bool:
        """Checks if BY_NAMES mode is active."""
        return self.get_extraction_mode() == ExtractionMode.BY_NAMES

    def is_by_filters_mode(self) -> bool:
        """Checks if BY_FILTERS mode is active."""
        return self.get_extraction_mode() == ExtractionMode.BY_FILTERS

    def get_account_names(self) -> List[str]:
        """
        Returns the list of account names (BY_NAMES mode).
        
        Returns:
            List[str]: List of account names
        
        Raises:
            ValueError: If called in BY_FILTERS mode
        """
        if not self.is_by_names_mode():
            raise ValueError(
                "get_account_names() can only be called in BY_NAMES mode"
            )
        return self.account_names

    def get_orchestrator_filter_values(self) -> List[str]:
        """
        Returns the list of orchestrators (BY_FILTERS mode).
        
        Returns:
            List[str]: ["PAASV4", "DMZRC"] if ALL, otherwise [single value]
        
        Raises:
            ValueError: If called in BY_NAMES mode
        """
        if self.is_by_names_mode():
            raise ValueError(
                "get_orchestrator_filter_values() cannot be called in BY_NAMES mode. "
                "Use get_account_names() instead."
            )
        
        # accounts_orchestrator cannot be None here (guaranteed by model_validator)
        if self.accounts_orchestrator == "ALL":
            return ["PAASV4", "DMZRC"]
        return [self.accounts_orchestrator]

    def get_environment_filter_values(self) -> List[str]:
        """
        Returns the list of environments (BY_FILTERS mode).
        
        Returns:
            List[str]: ["NPR", "PRD"] if ALL, otherwise [single value]
        
        Raises:
            ValueError: If called in BY_NAMES mode
        """
        if self.is_by_names_mode():
            raise ValueError(
                "get_environment_filter_values() cannot be called in BY_NAMES mode. "
                "Use get_account_names() instead."
            )
        
        # accounts_environment cannot be None here (guaranteed by model_validator)
        if self.accounts_environment == "ALL":
            return ["NPR", "PRD"]
        return [self.accounts_environment]

    def is_full_extraction(self) -> bool:
        """
        Checks if it's a full extraction in BY_FILTERS mode.
        
        Returns:
            bool: True if orchestrator=ALL and environment=ALL
        """
        return (
            self.is_by_filters_mode()
            and self.accounts_orchestrator == "ALL" 
            and self.accounts_environment == "ALL"
        )

    def _get_extraction_scope(self) -> str:
        """Generates a message describing the scope (BY_FILTERS mode only)."""
        if self.is_full_extraction():
            return "ALL accounts (all orchestrators, all environments)"
        
        orch = "all orchestrators" if self.accounts_orchestrator == "ALL" else self.accounts_orchestrator
        env = "all environments" if self.accounts_environment == "ALL" else self.accounts_environment
        
        return f"{orch} in {env}"

    def get_filter_summary(self) -> dict:
        """
        Returns a summary of filters based on the mode.
        
        Returns:
            dict: Different structure depending on the mode
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
                "account_env": self.get_environment_filter_values(),
                "orchestrator": self.accounts_orchestrator,
                "environment": self.accounts_environment
            }