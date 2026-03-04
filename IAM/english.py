from typing import Literal, Optional, List
from bpzi_airflow_library.schemas import ProductActionPayload
from pydantic import Field, field_validator, model_validator
from enum import Enum

# Strict types for validation
AccountsOrchestrator = Literal["PAASV4", "DMZRC", "ALL"]
AccountsEnvironment = Literal["NPR", "PRD", "ALL"]


class ExtractionMode(str, Enum):
    """IAM extraction mode."""
    BY_NAMES = "by_names"  # Extraction using a list of account names (no filters)
    BY_FILTERS = "by_filters"  # Extraction using orchestrator/environment filters
    BY_NAMES_WITH_FILTERS = "by_names_with_filters"  # Extraction by names + filter validation


class UnifiedExtractIamPayload(ProductActionPayload):
    """
    Unified payload for AWS IAM account extraction.
    
    VALIDATION RULES:
    
    1. BY_NAMES mode:
       - Provide ONLY account_names
       - Example: {"account_names": ["ac0021000259", "ac0021000260"]}
    
    2. BY_FILTERS mode:
       - Provide accounts_orchestrator and/or accounts_environment
       - If only one parameter is provided, the other defaults to "ALL"
       - Examples:
         * {"accounts_orchestrator": "PAASV4"} → PAASV4 + ALL
         * {"accounts_environment": "PRD"} → ALL + PRD
         * {"accounts_orchestrator": "DMZRC", "accounts_environment": "NPR"} → DMZRC + NPR
    
    3. BY_NAMES_WITH_FILTERS mode (NEW):
       - Provide account_names + filters (orchestrator and/or environment)
       - Filters act as additional validation constraints
       - Only accounts matching the filters are kept
       - Example: 
         * Input: {"account_names": ["ac001", "ac002", "ac003"], "accounts_orchestrator": "PAASV4"}
         * If ac003 is not PAASV4 → excluded with warning
         * Output: Only ac001 and ac002
    
    ERRORS:
    - ❌ Empty payload: {}
    
    Valid payload examples:
        # BY_NAMES mode
        {"account_names": ["ac0021000259"]}
        
        # BY_FILTERS mode
        {"accounts_orchestrator": "PAASV4"}
        {"accounts_environment": "PRD"}
        {"accounts_orchestrator": "DMZRC", "accounts_environment": "NPR"}
        
        # BY_NAMES_WITH_FILTERS mode (NEW)
        {"account_names": ["ac001", "ac002"], "accounts_orchestrator": "PAASV4"}
        {"account_names": ["ac001"], "accounts_environment": "PRD"}
        {"account_names": ["ac001", "ac002"], "accounts_orchestrator": "PAASV4", "accounts_environment": "PRD"}
    """
    
    # BY_NAMES mode (can be combined with filters)
    account_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of account names to extract. "
            "Can be combined with accounts_orchestrator and/or accounts_environment "
            "to apply additional validation constraints."
        ),
        examples=[["ac0021000259"], ["ac001", "ac002", "ac003"]],
        min_length=1
    )
    
    # BY_FILTERS mode (optional if account_names is provided)
    accounts_orchestrator: Optional[AccountsOrchestrator] = Field(
        default=None,
        description=(
            "AWS account orchestrator (PAASV4, DMZRC or ALL). "
            "If provided with account_names, acts as a validation filter. "
            "If omitted while accounts_environment is provided, defaults to ALL."
        ),
        examples=["PAASV4", "DMZRC", "ALL"]
    )
    
    accounts_environment: Optional[AccountsEnvironment] = Field(
        default=None,
        description=(
            "Account environment (NPR, PRD or ALL). "
            "If provided with account_names, acts as a validation filter. "
            "If omitted while accounts_orchestrator is provided, defaults to ALL."
        ),
        examples=["NPR", "PRD", "ALL"]
    )

    @field_validator("account_names", mode="before")
    @classmethod
    def validate_account_names(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """
        Validate the account names list.
        - Accepts None (BY_FILTERS mode)
        - If provided, must contain at least one valid name
        """
        # None or empty list → BY_FILTERS mode
        if v is None or (isinstance(v, list) and len(v) == 0):
            return None
        
        # Validate type
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
        Validate and normalize orchestrator.
        Returns None if not provided (handled later by model_validator).
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
        Validate and normalize environment.
        Returns None if not provided (handled later by model_validator).
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
        """Final validation and extraction mode logging."""
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
        
        else:  # BY_FILTERS or BY_NAMES_WITH_FILTERS
            print(f"  - accounts_orchestrator: {self.accounts_orchestrator}")
            print(f"  - accounts_environment: {self.accounts_environment}")
            print(f"  - Extraction scope: {self._get_extraction_scope()}")
            
            if self.is_full_extraction():
                print(f"⚠ WARNING: Full extraction (ALL orchestrators + ALL environments)")
        
        print("=" * 80)
        
        return self
    
    

from typing import Literal, Optional, List
from bpzi_airflow_library.schemas import ProductActionPayload
from pydantic import Field, field_validator, model_validator
from enum import Enum

# Strict validation types
AccountsOrchestrator = Literal["PAASV4", "DMZRC", "ALL"]
AccountsEnvironment = Literal["NPR", "PRD", "ALL"]


class ExtractionMode(str, Enum):
    """IAM extraction mode."""
    BY_NAMES = "by_names"  # Extract specific accounts
    BY_FILTERS = "by_filters"  # Extract using filters
    BY_NAMES_WITH_FILTERS = "by_names_with_filters"  # Extract names with filter validation


class UnifiedExtractIamPayload(ProductActionPayload):
    """
    Unified payload for AWS IAM account extraction.

    Modes:
    - BY_NAMES → account_names only
    - BY_FILTERS → orchestrator and/or environment
    - BY_NAMES_WITH_FILTERS → account_names + filters

    At least one of account_names or filters must be provided.
    """

    # Account list (optional if filters are used)
    account_names: Optional[List[str]] = Field(
        default=None,
        description=(
            "List of account names to extract. "
            "Can be combined with filters for validation."
        ),
        examples=[["ac0021000259"], ["ac001", "ac002"]],
        min_length=1
    )

    # Orchestrator filter
    accounts_orchestrator: Optional[AccountsOrchestrator] = Field(
        default=None,
        description="Account orchestrator (PAASV4, DMZRC or ALL).",
        examples=["PAASV4", "DMZRC", "ALL"]
    )

    # Environment filter
    accounts_environment: Optional[AccountsEnvironment] = Field(
        default=None,
        description="Account environment (NPR, PRD or ALL).",
        examples=["NPR", "PRD", "ALL"]
    )

    @field_validator("account_names", mode="before")
    @classmethod
    def validate_account_names(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """
        Validate account names list.
        - Accepts None (filter mode)
        - Removes duplicates
        """
        if v is None or (isinstance(v, list) and len(v) == 0):
            return None

        if not isinstance(v, list):
            raise ValueError(f"account_names must be a list, got {type(v)}")

        cleaned = []
        for idx, name in enumerate(v):
            if not isinstance(name, str):
                raise ValueError(f"account_names[{idx}] must be a string")
            name = name.strip()
            if not name:
                raise ValueError(f"account_names[{idx}] cannot be empty")
            cleaned.append(name)

        # Remove duplicates while preserving order
        return list(dict.fromkeys(cleaned))

    @field_validator("accounts_orchestrator", mode="before")
    @classmethod
    def validate_accounts_orchestrator(cls, v: Optional[str]) -> Optional[str]:
        """Validate and normalize orchestrator."""
        if v is None or (isinstance(v, str) and not v.strip()):
            return None

        v = v.strip().upper()
        if v not in ["PAASV4", "DMZRC", "ALL"]:
            raise ValueError("Invalid accounts_orchestrator")
        return v

    @field_validator("accounts_environment", mode="before")
    @classmethod
    def validate_accounts_environment(cls, v: Optional[str]) -> Optional[str]:
        """Validate and normalize environment."""
        if v is None or (isinstance(v, str) and not v.strip()):
            return None

        v = v.strip().upper()
        if v not in ["NPR", "PRD", "ALL"]:
            raise ValueError("Invalid accounts_environment")
        return v

    @model_validator(mode="after")
    def validate_and_log_payload(self):
        """Final validation and mode logging."""
        mode = self.get_extraction_mode()

        print("=" * 80)
        print("✓ Payload validated")
        print(f"Mode: {mode.value.upper()}")

        if mode == ExtractionMode.BY_NAMES:
            print(f"Accounts: {self.account_names} ({len(self.account_names)} total)")
        else:
            print(f"Orchestrator: {self.accounts_orchestrator}")
            print(f"Environment: {self.accounts_environment}")

        print("=" * 80)
        return self