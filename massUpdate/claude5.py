from typing import List, Optional, Literal
from pydantic import Field, field_validator, model_validator
from sqlalchemy.orm import Session as SASession
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# ==========================================
# PAYLOAD DEFINITION
# ==========================================

class AccountUpdates(BaseModel):
    """Configuration changes to apply to accounts"""
    iam_version: Optional[str] = Field(None, description="IAM version to update")
    subnet_count: Optional[int] = Field(None, ge=1, le=20, description="Number of subnets")
    enable_monitoring: Optional[bool] = Field(None, description="Enable monitoring flag")
    tags: Optional[List[str]] = Field(None, description="Tags to apply")
    
    @model_validator(mode='after')
    def check_at_least_one_update(self):
        """Ensure at least one field is being updated"""
        if not any([
            self.iam_version, 
            self.subnet_count is not None, 
            self.enable_monitoring is not None, 
            self.tags
        ]):
            raise ValueError("At least one update field must be provided")
        return self


class AccountUpdatePayload(ProductActionPayload):
    """Main payload for account update workflow"""
    account_names: List[str] = Field(
        ...,
        min_items=1,
        max_items=10,
        description="List of account names to update (1-10 accounts)"
    )
    updates: AccountUpdates = Field(
        ...,
        description="Configuration changes to apply"
    )
    execution_mode: Literal["plan_only", "plan_and_apply"] = Field(
        default="plan_only",
        description="Execution mode: plan_only (safe) or plan_and_apply (auto-deploy)"
    )
    max_parallel_executions: int = Field(
        default=2,
        ge=1,
        le=5,
        description="Maximum number of parallel executions (1-5)"
    )
    
    @field_validator("account_names")
    @classmethod
    def validate_account_names(cls, v: List[str]) -> List[str]:
        """Validate account names are not empty"""
        if not v:
            raise ValueError("account_names cannot be empty")
        
        for name in v:
            if not name or not name.strip():
                raise ValueError("account_names cannot contain empty or whitespace-only values")
        
        # Remove duplicates while preserving order
        seen = set()
        unique_names = []
        for name in v:
            if name not in seen:
                seen.add(name)
                unique_names.append(name)
        
        return unique_names


# ==========================================
# DATABASE HELPER FUNCTIONS
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


def update_account_config_in_db(
    account_name: str,
    updates: AccountUpdates,
    session: SASession
) -> db_models.AccountExtractIAM:
    """Update account configuration in database"""
    account = get_account_by_name(account_name, session)
    
    if account is None:
        raise ValueError(f"Account {account_name} not found in database")
    
    # Apply updates only for provided fields
    if updates.iam_version:
        account.iam_version = updates.iam_version
    
    if updates.subnet_count is not None:
        account.subnet_count = updates.subnet_count
    
    if updates.enable_monitoring is not None:
        account.enable_monitoring = updates.enable_monitoring
    
    if updates.tags:
        account.tags = updates.tags
    
    # Update tracking metadata
    account.last_update_requested = datetime.utcnow()
    account.deployment_status = "pending"
    account.deployment_error = None  # Clear previous errors
    
    session.commit()
    session.refresh(account)
    
    logger.info(f"✅ Updated account {account_name} in database")
    return account


def update_deployment_status(
    account_name: str,
    status: str,
    session: SASession,
    error_message: Optional[str] = None,
    activity_id: Optional[str] = None
):
    """Update deployment status and metadata in database"""
    account = get_account_by_name(account_name, session)
    
    if account:
        account.deployment_status = status
        account.last_deployment_date = datetime.utcnow()
        
        if error_message:
            account.deployment_error = error_message
        
        if activity_id:
            account.schematics_activity_id = activity_id
        
        session.commit()
        logger.info(f"📊 Account {account_name} status: {status}")


# ==========================================
# STEP 1: VALIDATE ACCOUNTS
# ==========================================

@step
def check_account_names(
    payload: AccountUpdatePayload = depends(payload_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
) -> bool:
    """
    Validate that all requested accounts exist in database
    
    Returns:
        bool: True if all accounts found, raises exception otherwise
    """
    logger.info(f"🔍 Validating {len(payload.account_names)} accounts...")
    
    missing_accounts: List[str] = []
    
    for account_name in payload.account_names:
        account = get_account_by_name(account_name, db_session)
        if account is None:
            missing_accounts.append(account_name)
            logger.error(f"❌ Account not found: {account_name}")
    
    if missing_accounts:
        raise DeclineDemandException(
            f"The following accounts were not found in database: {', '.join(missing_accounts)}"
        )
    
    logger.info(f"✅ All {len(payload.account_names)} accounts validated successfully")
    return True


# ==========================================
# STEP 2: UPDATE DATABASE
# ==========================================

@step
def update_accounts_database(
    payload: AccountUpdatePayload = depends(payload_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
) -> List[dict]:
    """
    Update all accounts configuration in PostgreSQL database
    
    Returns:
        List[dict]: List of updated account information
    """
    logger.info(f"📝 Updating {len(payload.account_names)} accounts in database...")
    
    updated_accounts = []
    failed_accounts = []
    
    for account_name in payload.account_names:
        try:
            account = update_account_config_in_db(
                account_name=account_name,
                updates=payload.updates,
                session=db_session
            )
            
            # Prepare account info for next steps
            account_info = {
                "account_name": account.account_name,
                "account_number": account.account_number,
                "account_type": account.account_type,
                "environment_type": account.environment_type,
                "code_bu": account.code_bu,
            }
            updated_accounts.append(account_info)
            
        except Exception as e:
            logger.error(f"❌ Failed to update {account_name}: {str(e)}")
            failed_accounts.append(account_name)
            
            # Update status to reflect failure
            update_deployment_status(
                account_name=account_name,
                status="update_failed",
                session=db_session,
                error_message=str(e)
            )
    
    if not updated_accounts:
        raise Exception(f"Failed to update all accounts in database. Failed: {', '.join(failed_accounts)}")
    
    if failed_accounts:
        logger.warning(f"⚠️ Some accounts failed to update: {', '.join(failed_accounts)}")
    
    logger.info(f"✅ Successfully updated {len(updated_accounts)} accounts in database")
    return updated_accounts


# ==========================================
# STEP 3: GET VAULT SECRETS
# ==========================================

@step(hide_output=True)
def get_vault_secrets(
    payload: AccountUpdatePayload = depends(payload_dependency),
    vault: Vault = depends(vault_dependency),
) -> dict:
    """
    Get all necessary secrets from Vault (orchestrator secrets)
    
    Returns:
        dict: Vault secrets including gitlab_token, tfe_secrets
    """
    logger.info("🔐 Retrieving orchestrator secrets from Vault...")
    
    # Determine environment type
    # Assuming you have environment_type in payload or config
    # Adjust based on your actual setup
    
    gitlab_secret = vault.get_secret(
        f"orchestrator/{ENVIRONMENT}/products/account/gitlab"
    )
    
    db_secret = vault.get_secret(
        f"orchestrator/{ENVIRONMENT}/products/account/db"
    )
    
    # Determine TFE environment
    # env_tfe = "prod" if env_type == "PRD" else "nprod"
    # For now, using a default - adjust based on your logic
    tfe_secrets = vault.get_secret(
        f"orchestrator/{ENVIRONMENT}/tfe/nprod"
    )
    
    secrets = {
        "gitlab_token": gitlab_secret["token"],
        "vault_token": vault.vault.token,
        "db_secret": db_secret,
        "tfe_secrets": tfe_secrets,
    }
    
    logger.info("✅ Retrieved orchestrator secrets from Vault")
    return secrets


# ==========================================
# STEP 4: CHECK ACCOUNT API KEYS & GET BUHUB
# ==========================================

@step(hide_output=True)
def check_accounts_in_vault(
    updated_accounts: List[dict] = depends(update_accounts_database),
    vault: Vault = depends(vault_dependency),
) -> bool:
    """
    Verify all accounts have required API keys in Vault
    
    Returns:
        bool: True if all API keys found, raises exception otherwise
    """
    logger.info(f"🔐 Checking Vault for {len(updated_accounts)} account API keys...")
    
    missing_api_keys = []
    
    for account_info in updated_accounts:
        account_number = account_info["account_number"]
        account_name = account_info["account_name"]
        
        try:
            api_key_secret = vault.get_secret(
                f"{account_number}/account-owner",
                mount_point="ibmsid"
            )
            
            if api_key_secret is None or "api_key" not in api_key_secret:
                missing_api_keys.append(f"{account_name} ({account_number})")
                logger.error(f"❌ API key not found for account: {account_name}")
                
        except Exception as e:
            logger.error(f"❌ Error accessing Vault for {account_name}: {str(e)}")
            missing_api_keys.append(f"{account_name} ({account_number})")
    
    if missing_api_keys:
        raise DeclineDemandException(
            f"The following accounts do not have API keys in Vault: {', '.join(missing_api_keys)}"
        )
    
    logger.info(f"✅ All {len(updated_accounts)} accounts have valid API keys in Vault")
    return True


@step
def get_buhub_account_details(
    updated_accounts: List[dict] = depends(update_accounts_database),
    db_session: SASession = depends(sqlalchemy_session_dependency),
) -> AccountDetails | None:
    """
    Get BUHUB account details if working with WKLAPP accounts
    
    This is needed to determine which API key to use for Schematics operations.
    
    Returns:
        AccountDetails | None: BUHUB account details or None
    """
    logger.info("🔍 Checking if BUHUB account is needed...")
    
    # Check if any account is WKLAPP type
    wklapp_accounts = [
        acc for acc in updated_accounts 
        if acc.get("account_type") == "WKLAPP"
    ]
    
    if not wklapp_accounts:
        logger.info("ℹ️ No WKLAPP accounts - BUHUB not needed")
        return None
    
    # Use the first WKLAPP account's code_bu to find BUHUB
    first_wklapp = wklapp_accounts[0]
    code_bu = first_wklapp.get("code_bu")
    
    logger.info(f"🔍 Looking for BUHUB account for code_bu: {code_bu}")
    
    from account.database.utils.account_services import get_account_by_type_and_bu
    
    # Get BUHUB account from database
    buhub_account = get_account_by_type_and_bu(
        "BUHUB", 
        code_bu, 
        db_session
    )
    
    if not buhub_account:
        raise Exception(f"BUHUB account not found for code_bu: {code_bu}")
    
    # Get full account details using ReaderConnector
    from account.connectors.reader import ReaderConnector
    
    reader_connector = ReaderConnector()
    buhub_account_details = reader_connector.get_account_details(
        buhub_account.name
    )
    
    logger.info("✅ BUHUB account details retrieved")
    logger.info(f"BUHUB account: {buhub_account_details.model_dump_json(indent=4)}")
    
    return buhub_account_details


@step(hide_output=True)
def get_buhub_account_api_key(
    buhub_account_details: AccountDetails | None = depends(get_buhub_account_details),
    vault: Vault = depends(vault_dependency),
) -> str:
    """
    Get API key for Schematics operations
    
    Uses BUHUB account if available, otherwise falls back to GLBHUB.
    This follows the exact pattern from account_dmzrc_create workflow.
    
    Returns:
        str: API key for Schematics backend
    """
    logger.info(f"🔑 Retrieving API key for Schematics...")
    logger.info(f"BUHUB account details: {buhub_account_details}")
    
    if buhub_account_details is not None:
        # Use BUHUB account API key
        api_key = vault.get_secret(
            f"{buhub_account_details['number']}/account-owner",
            mount_point="ibmsid"
        )["api_key"]
        
        logger.info(f"✅ Using BUHUB account API key")
        return api_key
    
    # Fallback: Use GLBHUB account
    logger.info("ℹ️ Using GLBHUB account (no BUHUB needed)")
    api_key = vault.get_secret(
        f"{GHUB_ACCOUNT_NUMBER}/account-owner",
        mount_point="ibmsid"
    )["api_key"]
    
    logger.info("✅ Retrieved GLBHUB account API key")
    return api_key


# ==========================================
# STEP 5: GET SCHEMATICS WORKSPACES
# ==========================================

def get_workspace_for_account(
    account_info: dict,
    tf: SchematicsBackend,
    db_session: SASession
) -> db_models.Workspace:
    """
    Get Schematics workspace for a specific account
    
    Returns:
        db_models.Workspace: Workspace object from database
    """
    # Build workspace name using standard naming convention
    workspace_name = (
        f"WS-ACCOUNT-DMZRC-"
        f"{account_info['environment_type'].upper()}-"
        f"{account_info['account_type'].upper()}-"
        f"{account_info['code_bu'].upper()}-"
        f"{account_info['account_name'].upper()}"
    )
    
    logger.info(f"🔍 Looking for workspace: {workspace_name}")
    
    # Query workspace from database
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
    
    # Verify workspace status in Schematics
    workspace = tf.workspaces.get_by_id(workspace_in_db.id)
    status = SchematicsWorkspaceStatus(workspace.status)
    
    logger.info(f"📊 Workspace {workspace_name} status: {status.value}")
    
    if status.value not in ["ACTIVE", "INACTIVE"]:
        raise ValueError(
            f"Workspace {workspace_name} has invalid status: {status.value}"
        )
    
    return workspace_in_db


@step
def get_workspaces_for_accounts(
    updated_accounts: List[dict] = depends(update_accounts_database),
    accounts_in_vault: bool = depends(check_accounts_in_vault),
    schematics_api_key: str = depends(get_buhub_account_api_key),
    vault_secrets: dict = depends(get_vault_secrets),
    tf: SchematicsBackend = depends(schematics_backend_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
) -> List[dict]:
    """
    Get Schematics workspaces for all accounts
    
    Note: schematics_backend_dependency is now properly initialized with the API key
    through the dependency injection system.
    
    Returns:
        List[dict]: Account info with workspace details
    """
    logger.info(f"📁 Getting Schematics workspaces for {len(updated_accounts)} accounts...")
    
    account_workspaces = []
    failed_accounts = []
    
    for account_info in updated_accounts:
        try:
            workspace = get_workspace_for_account(
                account_info=account_info,
                tf=tf,
                db_session=db_session
            )
            
            # Add workspace info to account data
            account_with_workspace = {
                **account_info,
                "workspace_id": workspace.id,
                "workspace_name": workspace.name
            }
            account_workspaces.append(account_with_workspace)
            
            logger.info(f"✅ Found workspace for {account_info['account_name']}")
            
        except Exception as e:
            logger.error(
                f"❌ Failed to get workspace for {account_info['account_name']}: {str(e)}"
            )
            failed_accounts.append(account_info['account_name'])
            
            # Update status in database
            update_deployment_status(
                account_name=account_info['account_name'],
                status="workspace_not_found",
                session=db_session,
                error_message=str(e)
            )
    
    if not account_workspaces:
        raise Exception(
            f"No workspaces found for any accounts. Failed: {', '.join(failed_accounts)}"
        )
    
    if failed_accounts:
        logger.warning(f"⚠️ Some workspaces not found: {', '.join(failed_accounts)}")
    
    logger.info(f"✅ Found {len(account_workspaces)} Schematics workspaces")
    return account_workspaces


# ==========================================
# STEP 6: DEPLOY ACCOUNTS (PARALLEL)
# ==========================================

def deploy_single_account(
    account_workspace: dict,
    payload: AccountUpdatePayload,
    tf: SchematicsBackend,
    db_session: SASession,
    vault: Vault
) -> dict:
    """
    Deploy configuration updates for a single account
    
    Args:
        account_workspace: Account info with workspace details
        payload: Update payload
        tf: SchematicsBackend instance (already initialized)
        db_session: Database session
        vault: Vault instance
    
    Returns:
        dict: Deployment result with status
    """
    account_name = account_workspace["account_name"]
    workspace_id = account_workspace["workspace_id"]
    
    try:
        logger.info(f"🚀 Starting deployment for account: {account_name}")
        
        # 1. Get updated account configuration from database
        account = get_account_by_name(account_name, db_session)
        
        # 2. Prepare variables for Schematics
        variables = {}
        
        if account.iam_version:
            variables["iam_version"] = account.iam_version
        
        if account.subnet_count is not None:
            variables["subnet_count"] = str(account.subnet_count)
        
        if account.enable_monitoring is not None:
            variables["enable_monitoring"] = str(account.enable_monitoring).lower()
        
        if account.tags:
            variables["tags"] = ",".join(account.tags)
        
        logger.info(f"📦 Variables to update: {list(variables.keys())}")
        
        # 3. Update workspace variables in Schematics
        tf.workspaces.update_variables(workspace_id, variables)
        logger.info(f"✅ Updated workspace variables for {account_name}")
        
        # 4. Execute Terraform Plan
        logger.info(f"📝 Creating Terraform plan for {account_name}")
        plan_job = tf.jobs.plan(workspace_id)
        plan_activity_id = plan_job.id
        
        update_deployment_status(
            account_name=account_name,
            status="planning",
            session=db_session,
            activity_id=plan_activity_id
        )
        
        # Wait for plan to complete
        plan_result = tf.jobs.wait_for_completion(
            plan_activity_id, 
            timeout=1800  # 30 minutes
        )
        
        if plan_result.status != "job_finished":
            raise Exception(
                f"Terraform plan failed with status: {plan_result.status}"
            )
        
        logger.info(f"✅ Terraform plan completed for {account_name}")
        
        # 5. Execute Terraform Apply (if requested)
        if payload.execution_mode == "plan_and_apply":
            logger.info(f"🔧 Applying Terraform changes for {account_name}")
            
            apply_job = tf.jobs.apply(workspace_id)
            apply_activity_id = apply_job.id
            
            update_deployment_status(
                account_name=account_name,
                status="applying",
                session=db_session,
                activity_id=apply_activity_id
            )
            
            # Wait for apply to complete
            apply_result = tf.jobs.wait_for_completion(
                apply_activity_id,
                timeout=1800  # 30 minutes
            )
            
            if apply_result.status != "job_finished":
                raise Exception(
                    f"Terraform apply failed with status: {apply_result.status}"
                )
            
            final_status = "deployed"
            logger.info(f"✅ Terraform apply completed for {account_name}")
        else:
            final_status = "plan_completed"
            logger.info(f"✅ Deployment stopped at plan stage (plan_only mode)")
        
        # 6. Update final status in database
        update_deployment_status(
            account_name=account_name,
            status=final_status,
            session=db_session
        )
        
        return {
            "account_name": account_name,
            "workspace_id": workspace_id,
            "status": "success",
            "final_status": final_status
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
            "workspace_id": workspace_id,
            "status": "failed",
            "error": str(e)
        }


@step
def deploy_accounts_parallel(
    account_workspaces: List[dict] = depends(get_workspaces_for_accounts),
    payload: AccountUpdatePayload = depends(payload_dependency),
    schematics_api_key: str = depends(get_buhub_account_api_key),
    db_session: SASession = depends(sqlalchemy_session_dependency),
    vault: Vault = depends(vault_dependency),
) -> dict:
    """
    Deploy configuration updates to all accounts in parallel
    
    Returns:
        dict: Summary of deployment results
    """
    total_accounts = len(account_workspaces)
    max_parallel = payload.max_parallel_executions
    
    logger.info(f"⚡ Starting parallel deployment:")
    logger.info(f"   📊 Total accounts: {total_accounts}")
    logger.info(f"   🔀 Max parallel: {max_parallel}")
    logger.info(f"   🎯 Mode: {payload.execution_mode}")
    
    results = {
        "successful": [],
        "failed": [],
        "total": total_accounts,
        "execution_mode": payload.execution_mode
    }
    
    # Execute deployments in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        # Submit all deployment jobs
        futures = {
            executor.submit(
                deploy_single_account,
                account_workspace,
                payload,
                tf,  # Pass the initialized SchematicsBackend
                db_session,
                vault
            ): account_workspace["account_name"]
            for account_workspace in account_workspaces
        }
        
        # Process results as they complete
        for future in as_completed(futures):
            account_name = futures[future]
            
            try:
                result = future.result()
                
                if result["status"] == "success":
                    results["successful"].append(result)
                    logger.info(f"✅ {account_name}: {result['final_status']}")
                else:
                    results["failed"].append(result)
                    logger.error(f"❌ {account_name}: {result.get('error', 'Unknown error')}")
                    
            except Exception as e:
                # Catch any unexpected errors
                error_result = {
                    "account_name": account_name,
                    "status": "failed",
                    "error": str(e)
                }
                results["failed"].append(error_result)
                logger.error(f"❌ {account_name}: Unexpected error: {str(e)}")
    
    # Log final summary
    logger.info("=" * 60)
    logger.info("📊 DEPLOYMENT SUMMARY")
    logger.info("=" * 60)
    logger.info(f"✅ Successful: {len(results['successful'])}/{total_accounts}")
    logger.info(f"❌ Failed: {len(results['failed'])}/{total_accounts}")
    
    if results['successful']:
        logger.info("Successful accounts:")
        for acc in results['successful']:
            logger.info(f"  ✅ {acc['account_name']} → {acc['final_status']}")
    
    if results['failed']:
        logger.warning("Failed accounts:")
        for acc in results['failed']:
            logger.warning(f"  ❌ {acc['account_name']} → {acc.get('error', 'Unknown')}")
    
    logger.info("=" * 60)
    
    # Fail workflow only if ALL deployments failed
    if len(results["failed"]) == total_accounts:
        raise Exception(
            f"All {total_accounts} account deployments failed. Check logs for details."
        )
    
    return results


# ==========================================
# WORKFLOW DEFINITION
# ==========================================

@workflow
def dag_parallel_update():
    """
    Main workflow for parallel account updates
    
    Workflow phases:
    1. Validate accounts exist in database
    2. Update account configurations in PostgreSQL
    3. Verify API keys exist in Vault
    4. Get Schematics workspaces for accounts
    5. Deploy changes in parallel via Schematics
    
    Returns:
        dict: Deployment summary with successful/failed accounts
    """
    
    # Phase 1: Validate accounts exist
    accounts_valid = check_account_names()
    
    # Phase 2: Update database with new configurations
    updated_accounts = update_accounts_database()
    
    # Phase 3: Get vault secrets (orchestrator level)
    vault_secrets = get_vault_secrets()
    
    # Phase 4: Verify accounts have API keys in Vault
    accounts_in_vault = check_accounts_in_vault(updated_accounts)
    
    # Phase 5: Get BUHUB account details (if needed for WKLAPP)
    buhub_account_details = get_buhub_account_details(updated_accounts)
    
    # Phase 6: Get API key for Schematics (BUHUB or GLBHUB)
    schematics_api_key = get_buhub_account_api_key(buhub_account_details)
    
    # Phase 7: Get Schematics workspaces for accounts
    account_workspaces = get_workspaces_for_accounts(
        updated_accounts, 
        accounts_in_vault,
        schematics_api_key,
        vault_secrets
    )
    
    # Phase 8: Deploy changes in parallel
    results = deploy_accounts_parallel(
        account_workspaces,
        schematics_api_key,
        vault_secrets
    )
    
    # Define dependencies (execution order)
    accounts_valid >> updated_accounts
    updated_accounts >> vault_secrets
    vault_secrets >> accounts_in_vault
    accounts_in_vault >> buhub_account_details
    buhub_account_details >> schematics_api_key
    schematics_api_key >> account_workspaces
    account_workspaces >> results
    
    return results