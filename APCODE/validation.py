"""
Payload Definition for APCode Product
======================================

Flexible payload with priority-based filtering:
1. target_apcodes (highest priority)
2. target_realms (fallback)
3. All apcodes (default)

With optional filtering by code_bu and env_type.
"""

from typing import List, Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from enum import Enum


# ==========================================
# ENUMS & CONSTANTS
# ==========================================

class EnvironmentType(str, Enum):
    """Environment types"""
    DEV = "dev"
    INT = "int"
    UAT = "uat"
    PROD = "prod"
    PREPROD = "preprod"


# ==========================================
# PAYLOAD DEFINITION
# ==========================================

class APCodeActionPayload(BaseModel):
    """
    Payload for APCode operations with flexible targeting.
    
    ## Priority Rules:
    1. If `target_apcodes` is provided → Use only these APCodes
       - Validates that each APCode belongs to code_bu and env_type (if provided)
    
    2. If `target_apcodes` is absent → Use `target_realms`
       - Fetches all APCodes from specified realms
       - Filters by code_bu and env_type (if provided)
    
    3. If both are absent → Use ALL APCodes
       - Filters by code_bu and env_type (if provided)
    
    ## Examples:
    
    ### Example 1: Target specific APCodes
    ```json
    {
      "target_apcodes": ["APC001", "APC002"],
      "code_bu": "BU123",
      "env_type": "prod"
    }
    ```
    → Only APC001 and APC002 (validated against BU123/prod)
    
    ### Example 2: Target realms
    ```json
    {
      "target_realms": ["REALM_A", "REALM_B"],
      "env_type": "prod"
    }
    ```
    → All APCodes in REALM_A and REALM_B (filtered by prod)
    
    ### Example 3: All APCodes with filter
    ```json
    {
      "code_bu": "BU123",
      "env_type": "dev"
    }
    ```
    → All APCodes in BU123/dev
    
    ### Example 4: All APCodes (no filter)
    ```json
    {}
    ```
    → All APCodes in the system
    """
    
    target_apcodes: Optional[List[str]] = Field(
        None,
        min_length=1,
        max_length=100,
        description="List of specific APCode IDs to target (highest priority)",
        examples=[["APC001", "APC002", "APC003"]]
    )
    
    target_realms: Optional[List[str]] = Field(
        None,
        min_length=1,
        max_length=50,
        description="List of realms to target (used if target_apcodes is absent)",
        examples=[["REALM_A", "REALM_B"]]
    )
    
    code_bu: Optional[str] = Field(
        None,
        min_length=1,
        max_length=50,
        description="Business Unit code for filtering",
        examples=["BU123", "BU456"]
    )
    
    env_type: Optional[EnvironmentType] = Field(
        None,
        description="Environment type for filtering",
        examples=["prod", "dev", "uat"]
    )
    
    # Optional: Action-specific parameters
    execution_mode: Literal["dry_run", "execute"] = Field(
        default="dry_run",
        description="Execution mode: dry_run (preview only) or execute (apply changes)"
    )
    
    max_parallel_executions: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Maximum number of parallel executions"
    )
    
    notify_on_completion: bool = Field(
        default=True,
        description="Send notification when operation completes"
    )
    
    
    # ==========================================
    # VALIDATORS
    # ==========================================
    
    @field_validator("target_apcodes")
    @classmethod
    def validate_target_apcodes(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate APCode format and uniqueness"""
        if v is None:
            return v
        
        # Remove duplicates while preserving order
        seen = set()
        unique_apcodes = []
        for apcode in v:
            if not apcode or not apcode.strip():
                raise ValueError("APCode cannot be empty or whitespace")
            
            apcode = apcode.strip().upper()
            
            if apcode not in seen:
                seen.add(apcode)
                unique_apcodes.append(apcode)
        
        if not unique_apcodes:
            raise ValueError("target_apcodes cannot be empty after validation")
        
        return unique_apcodes
    
    
    @field_validator("target_realms")
    @classmethod
    def validate_target_realms(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        """Validate realm format and uniqueness"""
        if v is None:
            return v
        
        # Remove duplicates
        seen = set()
        unique_realms = []
        for realm in v:
            if not realm or not realm.strip():
                raise ValueError("Realm cannot be empty or whitespace")
            
            realm = realm.strip().upper()
            
            if realm not in seen:
                seen.add(realm)
                unique_realms.append(realm)
        
        if not unique_realms:
            raise ValueError("target_realms cannot be empty after validation")
        
        return unique_realms
    
    
    @field_validator("code_bu")
    @classmethod
    def validate_code_bu(cls, v: Optional[str]) -> Optional[str]:
        """Validate code_bu format"""
        if v is None:
            return v
        
        v = v.strip().upper()
        
        if not v:
            raise ValueError("code_bu cannot be empty or whitespace")
        
        # Optional: Add pattern validation
        # if not re.match(r'^BU\d+$', v):
        #     raise ValueError("code_bu must follow pattern BU{digits}")
        
        return v
    
    
    @model_validator(mode='after')
    def validate_payload_not_empty(self):
        """
        Ensure payload is not completely empty.
        
        At least one of these must be provided:
        - target_apcodes
        - target_realms
        - code_bu
        - env_type
        
        OR the user explicitly wants all APCodes (empty payload is allowed)
        """
        # If everything is None, that's actually valid (means "all APCodes")
        # But we might want to require explicit confirmation for safety
        
        # Uncomment this block if you want to prevent completely empty payloads:
        # if not any([
        #     self.target_apcodes,
        #     self.target_realms,
        #     self.code_bu,
        #     self.env_type
        # ]):
        #     raise ValueError(
        #         "Payload cannot be completely empty. "
        #         "Provide at least one of: target_apcodes, target_realms, code_bu, or env_type"
        #     )
        
        return self
    
    
    @model_validator(mode='after')
    def validate_targeting_logic(self):
        """
        Validate the targeting logic and provide warnings.
        
        This is informational - doesn't fail validation but logs warnings.
        """
        import logging
        logger = logging.getLogger(__name__)
        
        if self.target_apcodes:
            logger.info(
                f"🎯 Targeting mode: SPECIFIC APCodes ({len(self.target_apcodes)} APCodes)"
            )
            if self.target_realms:
                logger.warning(
                    "⚠️ target_realms will be IGNORED because target_apcodes is provided"
                )
        
        elif self.target_realms:
            logger.info(
                f"🎯 Targeting mode: REALMS ({len(self.target_realms)} realms)"
            )
            if self.code_bu or self.env_type:
                filters = []
                if self.code_bu:
                    filters.append(f"code_bu={self.code_bu}")
                if self.env_type:
                    filters.append(f"env_type={self.env_type}")
                logger.info(f"   Filters: {', '.join(filters)}")
        
        else:
            logger.info("🎯 Targeting mode: ALL APCodes")
            if self.code_bu or self.env_type:
                filters = []
                if self.code_bu:
                    filters.append(f"code_bu={self.code_bu}")
                if self.env_type:
                    filters.append(f"env_type={self.env_type}")
                logger.info(f"   Filters: {', '.join(filters)}")
            else:
                logger.warning(
                    "⚠️ No filters provided - will target ALL APCodes in the system!"
                )
        
        return self
    
    
    # ==========================================
    # HELPER METHODS
    # ==========================================
    
    def get_targeting_strategy(self) -> str:
        """
        Get the targeting strategy based on provided fields.
        
        Returns:
            str: "specific_apcodes", "realms", or "all_apcodes"
        """
        if self.target_apcodes:
            return "specific_apcodes"
        elif self.target_realms:
            return "realms"
        else:
            return "all_apcodes"
    
    
    def has_filters(self) -> bool:
        """Check if any filters are applied"""
        return bool(self.code_bu or self.env_type)
    
    
    def get_filters_dict(self) -> dict:
        """Get active filters as a dictionary"""
        filters = {}
        if self.code_bu:
            filters["code_bu"] = self.code_bu
        if self.env_type:
            filters["env_type"] = self.env_type
        return filters
    
    
    def get_summary(self) -> str:
        """Get a human-readable summary of the targeting"""
        strategy = self.get_targeting_strategy()
        
        if strategy == "specific_apcodes":
            summary = f"Targeting {len(self.target_apcodes)} specific APCodes"
            if self.has_filters():
                filters_str = ", ".join(
                    f"{k}={v}" for k, v in self.get_filters_dict().items()
                )
                summary += f" (filtered by {filters_str})"
        
        elif strategy == "realms":
            summary = f"Targeting {len(self.target_realms)} realms"
            if self.has_filters():
                filters_str = ", ".join(
                    f"{k}={v}" for k, v in self.get_filters_dict().items()
                )
                summary += f" (filtered by {filters_str})"
        
        else:
            if self.has_filters():
                filters_str = ", ".join(
                    f"{k}={v}" for k, v in self.get_filters_dict().items()
                )
                summary = f"Targeting all APCodes (filtered by {filters_str})"
            else:
                summary = "Targeting ALL APCodes (no filters)"
        
        return summary


# ==========================================
# EXAMPLES & TESTS
# ==========================================

if __name__ == "__main__":
    import json
    
    print("=" * 70)
    print("APCode Payload Examples")
    print("=" * 70)
    print()
    
    # Example 1: Specific APCodes
    print("Example 1: Specific APCodes")
    print("-" * 70)
    payload1 = APCodeActionPayload(
        target_apcodes=["APC001", "APC002", "apc003"],  # Will be uppercased
        code_bu="BU123",
        env_type="prod"
    )
    print(json.dumps(payload1.model_dump(), indent=2))
    print(f"Strategy: {payload1.get_targeting_strategy()}")
    print(f"Summary: {payload1.get_summary()}")
    print()
    
    # Example 2: Target realms
    print("Example 2: Target Realms")
    print("-" * 70)
    payload2 = APCodeActionPayload(
        target_realms=["REALM_A", "realm_b"],  # Will be uppercased
        env_type="dev"
    )
    print(json.dumps(payload2.model_dump(), indent=2))
    print(f"Strategy: {payload2.get_targeting_strategy()}")
    print(f"Summary: {payload2.get_summary()}")
    print()
    
    # Example 3: All APCodes with filters
    print("Example 3: All APCodes with Filters")
    print("-" * 70)
    payload3 = APCodeActionPayload(
        code_bu="BU456",
        env_type="uat"
    )
    print(json.dumps(payload3.model_dump(), indent=2))
    print(f"Strategy: {payload3.get_targeting_strategy()}")
    print(f"Summary: {payload3.get_summary()}")
    print()
    
    # Example 4: All APCodes (no filters)
    print("Example 4: All APCodes (No Filters)")
    print("-" * 70)
    payload4 = APCodeActionPayload()
    print(json.dumps(payload4.model_dump(), indent=2))
    print(f"Strategy: {payload4.get_targeting_strategy()}")
    print(f"Summary: {payload4.get_summary()}")
    print()
    
    # Example 5: Validation error - empty APCode
    print("Example 5: Validation Error")
    print("-" * 70)
    try:
        payload5 = APCodeActionPayload(
            target_apcodes=["APC001", "", "APC003"]  # Empty string
        )
    except ValueError as e:
        print(f"❌ Validation Error: {e}")
    print()
    
    # Example 6: Duplicate removal
    print("Example 6: Duplicate Removal")
    print("-" * 70)
    payload6 = APCodeActionPayload(
        target_apcodes=["APC001", "apc001", "APC002", "APC001"]  # Duplicates
    )
    print(json.dumps(payload6.model_dump(), indent=2))
    print(f"Original: ['APC001', 'apc001', 'APC002', 'APC001']")
    print(f"After validation: {payload6.target_apcodes}")
    print()
    
    print("=" * 70)