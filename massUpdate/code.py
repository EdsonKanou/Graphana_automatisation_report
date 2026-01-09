from typing import List, Optional, Literal
from pydantic import Field, field_validator, model_validator
from sqlalchemy.orm import Session as SASession
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from datetime import datetime

# ==========================================
# 1. MODELS & PAYLOAD
# ==========================================

class AccountUpdates(BaseModel):
    """Configuration changes to apply"""
    iam_version: Optional[str] = None
    subnet_count: Optional[int] = None
    enable_monitoring: Optional[bool] = None
    tags: Optional[List[str]] = None
    
    @model_validator(mode='after')
    def check_at_least_one_update(self):
        if not any([self.iam_version, self.subnet_count, 
                    self.enable_monitoring, self.tags]):
            raise ValueError("At least one update field must be provided")
        return self


class AccountUpdatePayload(ProductActionPayload):
    """Payload for account update workflow"""
    account_names: List[str] = Field(
        ...,
        min_items=1,
        max_items=10,  # Limite raisonnable
        description="List of account names to update"
    )
    updates: AccountUpdates = Field(
        ...,
        description="Configuration changes to apply"
    )
    execution_mode: Literal["plan_only", "plan_and_apply"] = Field(
        default="plan_only",
        description="Execute only plan or plan + apply"
    )
    max_parallel_executions: int = Field(
        default=2,
        ge=1,
        le=5,
        description="Maximum parallel executions"
    )
    
    @field_validator("account_names")
    @classmethod
    def validate_account_names(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("account_names cannot be empty")
        for name in v:
            if not name or not name.strip():
                raise ValueError("account_names cannot contain empty values")
        return v


# ==========================================
# 2. DATABASE OPERATIONS
# ==========================================

def get_account_by_name(
    name: str, 
    session: SASession
) -> db_models.AccountExtractIAM | None:
    """Retrieve account from database by name"""
    account = (
        session.query(db_models.AccountExtractIAM)
        .filter(db_models.AccountExtractIAM.account_name.contains(name))
        .first()
    )
    return account


def update_account_config(
    account_name: str,
    updates: AccountUpdates,
    session: SASession
) -> db_models.AccountExtractIAM:
    """Update account configuration in database"""
    account = get_account_by_name(account_name, session)
    
    if account is None:
        raise ValueError(f"Account {account_name} not found")
    
    # Mise à jour des champs
    if updates.iam_version:
        account.iam_version = updates.iam_version
    if updates.subnet_count is not None:
        account.subnet_count = updates.subnet_count
    if updates.enable_monitoring is not None:
        account.enable_monitoring = updates.enable_monitoring
    if updates.tags:
        account.tags = updates.tags
    
    # Métadonnées de tracking
    account.last_update_requested = datetime.utcnow()
    account.deployment_status = "pending"
    
    session.commit()
    session.refresh(account)
    
    return account


def update_deployment_status(
    account_name: str,
    status: str,
    session: SASession,
    error_message: Optional[str] = None,
    schematics_activity_id: Optional[str] = None
):
    """Update deployment status in database"""
    account = get_account_by_name(account_name, session)
    
    if account:
        account.deployment_status = status
        account.last_deployment_date = datetime.utcnow()
        
        if error_message:
            account.deployment_error = error_message
        if schematics_activity_id:
            account.schematics_activity_id = schematics_activity_id
        
        session.commit()


# ==========================================
# 3. IBM SCHEMATICS INTEGRATION
# ==========================================

class SchematicsClient:
    """Client for IBM Schematics API"""
    
    def __init__(self, api_key: str, region: str = "us-south"):
        self.base_url = f"https://{region}.schematics.cloud.ibm.com/v1"
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def get_workspace_id(self, account_name: str) -> str:
        """Get or create workspace ID for account"""
        # Logique pour récupérer le workspace_id
        # Soit depuis la BD, soit via API Schematics
        pass
    
    def update_workspace_variables(
        self, 
        workspace_id: str, 
        variables: dict
    ) -> dict:
        """Update workspace variables (from Vault/DB)"""
        url = f"{self.base_url}/workspaces/{workspace_id}"
        
        payload = {
            "template_data": [{
                "variablestore": [
                    {
                        "name": key,
                        "value": str(value),
                        "type": "string"
                    }
                    for key, value in variables.items()
                ]
            }]
        }
        
        response = requests.patch(url, json=payload, headers=self.headers)
        response.raise_for_status()
        return response.json()
    
    def create_plan(self, workspace_id: str) -> str:
        """Trigger terraform plan"""
        url = f"{self.base_url}/workspaces/{workspace_id}/plan"
        
        response = requests.post(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json()["activityid"]
    
    def apply_plan(self, workspace_id: str) -> str:
        """Trigger terraform apply"""
        url = f"{self.base_url}/workspaces/{workspace_id}/apply"
        
        response = requests.post(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json()["activityid"]
    
    def get_activity_status(self, workspace_id: str, activity_id: str) -> dict:
        """Get status of plan/apply activity"""
        url = f"{self.base_url}/workspaces/{workspace_id}/actions/{activity_id}"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json()
    
    def wait_for_activity(
        self, 
        workspace_id: str, 
        activity_id: str,
        timeout: int = 1800  # 30 minutes
    ) -> dict:
        """Wait for activity to complete"""
        import time
        start_time = time.time()
        
        while True:
            if time.time() - start_time > timeout:
                raise TimeoutError(f"Activity {activity_id} timed out")
            
            status = self.get_activity_status(workspace_id, activity_id)
            
            if status["status"] in ["COMPLETED", "FAILED", "STOPPED"]:
                return status
            
            time.sleep(10)  # Poll every 10 seconds


# ==========================================
# 4. WORKFLOW STEPS
# ==========================================

@step
def check_account_names(
    payload: AccountUpdatePayload = depends(payload_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
) -> bool:
    """Validate that all accounts exist in database"""
    missing_accounts: list[str] = []
    
    for account_name in payload.account_names:
        account = get_account_by_name(account_name, db_session)
        if account is None:
            missing_accounts.append(account_name)
    
    if missing_accounts:
        raise DeclineDemandException(
            f"Accounts not found in database: {', '.join(missing_accounts)}"
        )
    
    return True


@step
def update_accounts_database(
    payload: AccountUpdatePayload = depends(payload_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
) -> List[str]:
    """Update all accounts configuration in database"""
    updated_accounts = []
    
    for account_name in payload.account_names:
        try:
            account = update_account_config(
                account_name=account_name,
                updates=payload.updates,
                session=db_session
            )
            updated_accounts.append(account_name)
            
        except Exception as e:
            # Log error but continue with other accounts
            print(f"Error updating {account_name}: {str(e)}")
            update_deployment_status(
                account_name=account_name,
                status="update_failed",
                session=db_session,
                error_message=str(e)
            )
    
    if not updated_accounts:
        raise Exception("No accounts were successfully updated")
    
    return updated_accounts


def process_single_account_deployment(
    account_name: str,
    payload: AccountUpdatePayload,
    db_session: SASession,
    schematics_client: SchematicsClient,
    vault: Vault
) -> dict:
    """Process deployment for a single account"""
    try:
        # 1. Get account from DB
        account = get_account_by_name(account_name, db_session)
        
        # 2. Get workspace ID
        workspace_id = schematics_client.get_workspace_id(account_name)
        
        # 3. Prepare variables from DB + Vault
        variables = {
            "iam_version": account.iam_version,
            "subnet_count": str(account.subnet_count),
            # Ajoute d'autres variables depuis Vault si nécessaire
        }
        
        # 4. Update workspace variables
        schematics_client.update_workspace_variables(workspace_id, variables)
        
        # 5. Create and wait for plan
        plan_activity_id = schematics_client.create_plan(workspace_id)
        
        update_deployment_status(
            account_name=account_name,
            status="planning",
            session=db_session,
            schematics_activity_id=plan_activity_id
        )
        
        plan_result = schematics_client.wait_for_activity(
            workspace_id, 
            plan_activity_id
        )
        
        if plan_result["status"] != "COMPLETED":
            raise Exception(f"Plan failed: {plan_result.get('message', 'Unknown error')}")
        
        # 6. Apply if requested
        if payload.execution_mode == "plan_and_apply":
            apply_activity_id = schematics_client.apply_plan(workspace_id)
            
            update_deployment_status(
                account_name=account_name,
                status="applying",
                session=db_session,
                schematics_activity_id=apply_activity_id
            )
            
            apply_result = schematics_client.wait_for_activity(
                workspace_id,
                apply_activity_id
            )
            
            if apply_result["status"] != "COMPLETED":
                raise Exception(f"Apply failed: {apply_result.get('message', 'Unknown error')}")
            
            final_status = "deployed"
        else:
            final_status = "plan_completed"
        
        # 7. Update final status
        update_deployment_status(
            account_name=account_name,
            status=final_status,
            session=db_session
        )
        
        return {
            "account_name": account_name,
            "status": "success",
            "final_status": final_status
        }
        
    except Exception as e:
        update_deployment_status(
            account_name=account_name,
            status="deployment_failed",
            session=db_session,
            error_message=str(e)
        )
        
        return {
            "account_name": account_name,
            "status": "failed",
            "error": str(e)
        }


@step
def deploy_accounts_parallel(
    updated_accounts: List[str] = depends(update_accounts_database),
    payload: AccountUpdatePayload = depends(payload_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
    vault: Vault = depends(vault_dependency),
) -> dict:
    """Deploy updates to IBM Schematics in parallel"""
    
    # Initialize Schematics client
    api_key = vault.get_secret("ibm_cloud_api_key")
    schematics_client = SchematicsClient(api_key=api_key)
    
    results = {
        "successful": [],
        "failed": [],
        "total": len(updated_accounts)
    }
    
    # Execute in parallel with max workers
    with ThreadPoolExecutor(max_workers=payload.max_parallel_executions) as executor:
        futures = {
            executor.submit(
                process_single_account_deployment,
                account_name,
                payload,
                db_session,
                schematics_client,
                vault
            ): account_name
            for account_name in updated_accounts
        }
        
        for future in as_completed(futures):
            account_name = futures[future]
            try:
                result = future.result()
                
                if result["status"] == "success":
                    results["successful"].append(result)
                else:
                    results["failed"].append(result)
                    
            except Exception as e:
                results["failed"].append({
                    "account_name": account_name,
                    "status": "failed",
                    "error": str(e)
                })
    
    # Fail workflow if all deployments failed
    if len(results["failed"]) == results["total"]:
        raise Exception("All deployments failed")
    
    return results


# ==========================================
# 5. DAG DEFINITION
# ==========================================

@workflow
def account_update_workflow():
    """
    Complete workflow for updating account configurations
    
    Steps:
    1. Validate account names exist
    2. Update configurations in PostgreSQL
    3. Deploy changes via IBM Schematics (parallel)
    """
    
    # Step 1: Validate accounts
    check_account_names()
    
    # Step 2: Update database
    updated_accounts = update_accounts_database()
    
    # Step 3: Deploy to IBM Cloud (parallel)
    results = deploy_accounts_parallel()
    
    return results






#Proposition d'architecture complete

# Structure du DAG
class AccountUpdatePayload(ProductActionPayload):
    account_names: List[str] = Field(
        ...,
        min_items=1,
        description="Specify the list of account names"
    )
    updates: AccountUpdates = Field(
        ...,
        description="Configuration changes to apply"
    )
    execution_mode: Literal["plan_only", "plan_and_apply"] = Field(
        default="plan_only",
        description="Execution mode"
    )

class AccountUpdates(BaseModel):
    iam_version: Optional[str] = None
    subnet_count: Optional[int] = None
    # Ajoute d'autres champs selon tes besoins
    
    @model_validator(mode='after')
    def check_at_least_one_update(self):
        if not any([self.iam_version, self.subnet_count]):
            raise ValueError("At least one update field must be provided")
        return self

#BD a ajuster 

-- Ajoute ces colonnes à ta table AccountExtractIAM
ALTER TABLE account_extract_iam ADD COLUMN IF NOT EXISTS
    deployment_status VARCHAR(50),
    last_update_requested TIMESTAMP,
    last_deployment_date TIMESTAMP,
    deployment_error TEXT,
    schematics_activity_id VARCHAR(100),
    schematics_workspace_id VARCHAR(100);