from typing import List, Optional, Literal
from pydantic import Field, field_validator, model_validator
from sqlalchemy.orm import Session as SASession
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# ==========================================
# 1. MODELS & PAYLOAD
# ==========================================

class AccountUpdates(BaseModel):
    """Configuration changes to apply"""
    iam_version: Optional[str] = Field(None, description="IAM version to update")
    subnet_count: Optional[int] = Field(None, description="Number of subnets")
    enable_monitoring: Optional[bool] = Field(None, description="Enable monitoring")
    tags: Optional[List[str]] = Field(None, description="Tags to apply")
    
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
        max_items=10,
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
# 2. DATABASE HELPERS
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
    activity_id: Optional[str] = None
):
    """Update deployment status in database"""
    account = get_account_by_name(account_name, session)
    
    if account:
        account.deployment_status = status
        account.last_deployment_date = datetime.utcnow()
        
        if error_message:
            account.deployment_error = error_message
        if activity_id:
            account.schematics_activity_id = activity_id
        
        session.commit()


# ==========================================
# 3. WORKFLOW STEPS (Phase 1 & 2)
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
    
    logger.info(f"✅ All {len(payload.account_names)} accounts found in database")
    return True


@step
def update_accounts_database(
    payload: AccountUpdatePayload = depends(payload_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
) -> List[dict]:
    """Update all accounts configuration in database"""
    updated_accounts = []
    
    for account_name in payload.account_names:
        try:
            account = update_account_config(
                account_name=account_name,
                updates=payload.updates,
                session=db_session
            )
            
            updated_accounts.append({
                "account_name": account.account_name,
                "account_number": account.account_number,
                "account_type": account.account_type,
                "environment_type": account.environment_type,
                "code_bu": account.code_bu,
            })
            
            logger.info(f"✅ Updated {account_name} in database")
            
        except Exception as e:
            logger.error(f"❌ Error updating {account_name}: {str(e)}")
            update_deployment_status(
                account_name=account_name,
                status="update_failed",
                session=db_session,
                error_message=str(e)
            )
    
    if not updated_accounts:
        raise Exception("No accounts were successfully updated in database")
    
    logger.info(f"✅ Updated {len(updated_accounts)} accounts in database")
    return updated_accounts


# ==========================================
# 4. VAULT & SECRETS (Phase 3)
# ==========================================

@step(hide_output=True)
def get_vault_secrets(
    payload: AccountUpdatePayload = depends(payload_dependency),
    vault: Vault = depends(vault_dependency),
) -> dict:
    """Get all necessary secrets from Vault"""
    env_type = "prod" if payload.environment_type == "PRD" else "nprod"
    
    # Get orchestrator secrets
    gitlab_secret = vault.get_secret(
        f"orchestrator/{ENVIRONMENT}/products/account/gitlab"
    )
    db_secret = vault.get_secret(
        f"orchestrator/{ENVIRONMENT}/products/account/db"
    )
    tfe_secrets = vault.get_secret(
        f"orchestrator/{ENVIRONMENT}/tfe/{env_type}"
    )
    
    secrets = {
        "gitlab_token": gitlab_secret["token"],
        "vault_token": vault.vault.token,
        "db_secret": db_secret,
        "tfe_secrets": tfe_secrets,
    }
    
    logger.info("✅ Retrieved vault secrets")
    return secrets


@step(hide_output=True)
def check_accounts_in_vault(
    updated_accounts: List[dict] = depends(update_accounts_database),
    vault: Vault = depends(vault_dependency),
) -> bool:
    """Verify all accounts have API keys in Vault"""
    missing_api_keys = []
    
    for account_info in updated_accounts:
        account_number = account_info["account_number"]
        
        try:
            api_key = vault.get_secret(
                f"{account_number}/account-owner",
                mount_point="ibmsid"
            )["api_key"]
            
            if api_key is None:
                missing_api_keys.append(account_number)
                
        except Exception as e:
            logger.error(f"Error getting API key for {account_number}: {str(e)}")
            missing_api_keys.append(account_number)
    
    if missing_api_keys:
        raise DeclineDemandException(
            f"Account numbers not present in vault: {', '.join(missing_api_keys)}"
        )
    
    logger.info(f"✅ All {len(updated_accounts)} accounts have API keys in Vault")
    return True


# ==========================================
# 5. SCHEMATICS WORKSPACE OPERATIONS
# ==========================================

def get_workspace_for_account(
    account_info: dict,
    tf: SchematicsBackend,
    db_session: SASession
) -> db_models.Workspace | None:
    """Get or verify Schematics workspace for an account"""
    
    # Build workspace name using your naming convention
    workspace_name = (
        f"WS-ACCOUNT-DMZRC-"
        f"{account_info['environment_type'].upper()}-"
        f"{account_info['account_type'].upper()}-"
        f"{account_info['code_bu'].upper()}-"
        f"{account_info['account_name'].upper()}"
    )
    
    logger.info(f"Looking for workspace: {workspace_name}")
    
    # Check if workspace exists in DB
    workspace_in_db: db_models.Workspace = (
        db_session.query(db_models.Workspace)
        .filter(
            db_models.Workspace.name == workspace_name,
            db_models.Workspace.type == "schematics"
        )
        .one_or_none()
    )
    
    if not workspace_in_db:
        raise ValueError(f"Workspace {workspace_name} not found in database")
    
    # Verify workspace is ACTIVE in Schematics
    workspace = tf.workspaces.get_by_id(workspace_in_db.id)
    status = SchematicsWorkspaceStatus(workspace.status)
    
    logger.info(f"Workspace {workspace_name} status: {status.value}")
    
    if status.value not in ["ACTIVE", "INACTIVE"]:
        raise ValueError(
            f"Workspace {workspace_name} is in invalid state: {status.value}"
        )
    
    return workspace_in_db


@step
def get_workspaces_for_accounts(
    updated_accounts: List[dict] = depends(update_accounts_database),
    accounts_in_vault: bool = depends(check_accounts_in_vault),
    tf: SchematicsBackend = depends(schematics_backend_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
) -> List[dict]:
    """Get Schematics workspaces for all accounts"""
    
    account_workspaces = []
    
    for account_info in updated_accounts:
        try:
            workspace = get_workspace_for_account(
                account_info=account_info,
                tf=tf,
                db_session=db_session
            )
            
            account_workspaces.append({
                **account_info,
                "workspace_id": workspace.id,
                "workspace_name": workspace.name
            })
            
            logger.info(f"✅ Found workspace for {account_info['account_name']}")
            
        except Exception as e:
            logger.error(
                f"❌ Error finding workspace for {account_info['account_name']}: {str(e)}"
            )
            update_deployment_status(
                account_name=account_info['account_name'],
                status="workspace_not_found",
                session=db_session,
                error_message=str(e)
            )
    
    if not account_workspaces:
        raise Exception("No workspaces found for any accounts")
    
    logger.info(f"✅ Found {len(account_workspaces)} workspaces")
    return account_workspaces


# ==========================================
# 6. DEPLOYMENT EXECUTION (Parallel)
# ==========================================

def deploy_single_account(
    account_workspace: dict,
    payload: AccountUpdatePayload,
    tf: SchematicsBackend,
    db_session: SASession,
    vault: Vault
) -> dict:
    """Deploy updates for a single account via Schematics"""
    
    account_name = account_workspace["account_name"]
    workspace_id = account_workspace["workspace_id"]
    
    try:
        logger.info(f"🚀 Starting deployment for {account_name}")
        
        # 1. Get account from DB to retrieve updated config
        account = get_account_by_name(account_name, db_session)
        
        # 2. Update workspace variables with new config from DB
        variables = {
            "iam_version": account.iam_version,
            "subnet_count": str(account.subnet_count),
            # Add other variables as needed
        }
        
        # Update workspace template data
        tf.workspaces.update_variables(workspace_id, variables)
        logger.info(f"✅ Updated variables for {account_name}")
        
        # 3. Create plan
        logger.info(f"📝 Creating plan for {account_name}")
        plan_job = tf.jobs.plan(workspace_id)
        plan_activity_id = plan_job.id
        
        update_deployment_status(
            account_name=account_name,
            status="planning",
            session=db_session,
            activity_id=plan_activity_id
        )
        
        # Wait for plan to complete
        plan_result = tf.jobs.wait_for_completion(plan_activity_id, timeout=1800)
        
        if plan_result.status != "job_finished":
            raise Exception(f"Plan failed with status: {plan_result.status}")
        
        logger.info(f"✅ Plan completed for {account_name}")
        
        # 4. Apply if requested
        if payload.execution_mode == "plan_and_apply":
            logger.info(f"🔧 Applying changes for {account_name}")
            
            apply_job = tf.jobs.apply(workspace_id)
            apply_activity_id = apply_job.id
            
            update_deployment_status(
                account_name=account_name,
                status="applying",
                session=db_session,
                activity_id=apply_activity_id
            )
            
            # Wait for apply to complete
            apply_result = tf.jobs.wait_for_completion(apply_activity_id, timeout=1800)
            
            if apply_result.status != "job_finished":
                raise Exception(f"Apply failed with status: {apply_result.status}")
            
            final_status = "deployed"
            logger.info(f"✅ Apply completed for {account_name}")
        else:
            final_status = "plan_completed"
        
        # 5. Update final status
        update_deployment_status(
            account_name=account_name,
            status=final_status,
            session=db_session
        )
        
        return {
            "account_name": account_name,
            "status": "success",
            "final_status": final_status,
            "workspace_id": workspace_id
        }
        
    except Exception as e:
        logger.error(f"❌ Deployment failed for {account_name}: {str(e)}")
        
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
    account_workspaces: List[dict] = depends(get_workspaces_for_accounts),
    payload: AccountUpdatePayload = depends(payload_dependency),
    tf: SchematicsBackend = depends(schematics_backend_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
    vault: Vault = depends(vault_dependency),
) -> dict:
    """Deploy updates to all accounts in parallel via Schematics"""
    
    results = {
        "successful": [],
        "failed": [],
        "total": len(account_workspaces)
    }
    
    logger.info(
        f"🚀 Starting parallel deployment for {results['total']} accounts "
        f"(max {payload.max_parallel_executions} parallel)"
    )
    
    # Execute deployments in parallel
    with ThreadPoolExecutor(max_workers=payload.max_parallel_executions) as executor:
        futures = {
            executor.submit(
                deploy_single_account,
                account_workspace,
                payload,
                tf,
                db_session,
                vault
            ): account_workspace["account_name"]
            for account_workspace in account_workspaces
        }
        
        for future in as_completed(futures):
            account_name = futures[future]
            try:
                result = future.result()
                
                if result["status"] == "success":
                    results["successful"].append(result)
                    logger.info(f"✅ {account_name}: {result['final_status']}")
                else:
                    results["failed"].append(result)
                    logger.error(f"❌ {account_name}: {result['error']}")
                    
            except Exception as e:
                results["failed"].append({
                    "account_name": account_name,
                    "status": "failed",
                    "error": str(e)
                })
                logger.error(f"❌ {account_name}: Unexpected error: {str(e)}")
    
    # Summary
    logger.info(f"📊 Deployment summary:")
    logger.info(f"   ✅ Successful: {len(results['successful'])}")
    logger.info(f"   ❌ Failed: {len(results['failed'])}")
    
    # Fail workflow if all deployments failed
    if len(results["failed"]) == results["total"]:
        raise Exception("All deployments failed")
    
    return results


# ==========================================
# 7. WORKFLOW DEFINITION
# ==========================================

@workflow
def account_update_workflow():
    """
    Complete workflow for updating account configurations
    
    Phases:
    1. Validation: Check accounts exist in DB
    2. Database Update: Update configurations in PostgreSQL
    3. Vault Check: Verify API keys exist
    4. Workspace Lookup: Get Schematics workspaces
    5. Parallel Deployment: Execute Terraform plan/apply via Schematics
    
    Returns:
        dict: Summary of successful and failed deployments
    """
    
    # Phase 1: Validate accounts exist
    accounts_valid = check_account_names()
    
    # Phase 2: Update database with new configurations
    updated_accounts = update_accounts_database()
    
    # Phase 3: Verify accounts have API keys in Vault
    accounts_in_vault = check_accounts_in_vault(updated_accounts)
    
    # Phase 4: Get Schematics workspaces for accounts
    account_workspaces = get_workspaces_for_accounts(
        updated_accounts, accounts_in_vault
    )
    
    # Phase 5: Deploy changes in parallel
    results = deploy_accounts_parallel(account_workspaces)
    
    return results

{
  "account_names": ["ACC-001", "ACC-002"],
  "updates": {
    "iam_version": "2.0",
    "subnet_count": 5
  },
  "execution_mode": "plan_only",
  "max_parallel_executions": 2
}