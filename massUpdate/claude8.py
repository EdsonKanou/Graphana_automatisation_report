"""
DAG: Account Parallel Update - Version corrigée
================================================

Fix de l'erreur: AttributeError: 'dict' object has no attribute 'update_relative'
"""

from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

from sqlalchemy.orm import Session as SASession

# Vos imports existants
from account.models import AccountUpdatePayload, AccountSecrets, AccountDetails
from account.database import db_models
from account.connectors.reader import ReaderConnector
from config import ENVIRONMENT, GLBHUB_ACCOUNT_NUMBER
from dependencies import (
    payload_dependency,
    sqlalchemy_session_dependency,
    vault_dependency,
    schematics_backend_dependency,
)
from exceptions import DeclineDemandException
from vault import Vault
from schematics import SchematicsBackend, SchematicsWorkspaceStatus, TerraformVar
from policies import LinearCooldownPolicy

logger = logging.getLogger(__name__)


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def get_account_by_name(name: str, session: SASession):
    """Retrieve account from database by name"""
    return (
        session.query(db_models.AccountExtractIAM)
        .filter(db_models.AccountExtractIAM.account_name.contains(name))
        .first()
    )


def update_account_config(account_name: str, updates, session: SASession):
    """Update account configuration in database"""
    account = get_account_by_name(account_name, session)
    
    if account is None:
        raise ValueError(f"Account {account_name} not found")
    
    if updates.iam_version:
        account.iam_version = updates.iam_version
    if updates.subnet_count is not None:
        account.subnet_count = updates.subnet_count
    if updates.enable_monitoring is not None:
        account.enable_monitoring = updates.enable_monitoring
    if updates.tags:
        account.tags = updates.tags
    
    account.last_update_requested = datetime.utcnow()
    account.deployment_status = "pending"
    account.deployment_error = None
    
    session.commit()
    session.refresh(account)
    
    return account


def update_deployment_status(
    account_name: str,
    status: str,
    session: SASession,
    error_message: Optional[str] = None,
    activity_id: Optional[str] = None,
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
# DAG DEFINITION
# ==========================================

@product_action(
    action_id=Path(__file__).stem,
    payload=AccountUpdatePayload,
    tags=["account", "admin", "parallel", "update"],
    doc_md="DAG to parallel account update (fixed version)."
)
def dag_parallel_update() -> Dict:

    # ==========================================
    # STEP 1: VALIDATION
    # ==========================================
    
    @step
    def check_accounts_exist(
        payload: AccountUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        """Validate that all accounts exist in database"""
        missing_accounts: list[str] = []

        logger.info(f"🔍 Checking {len(payload.account_names)} accounts in database")

        for account_name in payload.account_names:
            account = get_account_by_name(account_name, db_session)
            if account is None:
                missing_accounts.append(account_name)
                logger.error(f"❌ Account not found: {account_name}")

        if missing_accounts:
            raise DeclineDemandException(
                f"Accounts not found in database: {', '.join(missing_accounts)}"
            )

        logger.info(f"✅ All {len(payload.account_names)} accounts found")
        return True

    # ==========================================
    # STEP 2: DATABASE UPDATE
    # ==========================================
    
    @step
    def update_accounts_database(
        payload: AccountUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> list[dict]:
        """Update all accounts configuration in database"""
        updated_accounts = []

        logger.info(f"📝 Updating {len(payload.account_names)} accounts")

        for account_name in payload.account_names:
            try:
                account = update_account_config(
                    account_name=account_name,
                    updates=payload.updates,
                    session=db_session,
                )

                updated_accounts.append({
                    "account_name": account.name,
                    "account_number": account.number,
                    "account_type": account.type,
                    "environment_type": account.environment_type,
                    "code_bu": account.code_bu,
                })

                logger.info(f"✅ Updated {account_name}")

            except Exception as e:
                logger.error(f"❌ Error updating {account_name}: {str(e)}")
                update_deployment_status(
                    account_name=account_name,
                    status="update_failed",
                    session=db_session,
                    error_message=str(e),
                )

        if not updated_accounts:
            raise Exception("No accounts were successfully updated in database")

        logger.info(f"✅ Updated {len(updated_accounts)} accounts")
        return updated_accounts

    # ==========================================
    # STEP 3: VAULT SECRETS
    # ==========================================
    
    @step(hide_output=True)
    def get_vault_secrets(
        payload: AccountUpdatePayload = depends(payload_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> AccountSecrets:
        """Get orchestrator secrets from Vault"""
        logger.info("🔐 Retrieving orchestrator secrets")

        gitlab_secret = vault.get_secret(
            f"orchestrator/{ENVIRONMENT}/products/account/gitlab"
        )

        tfe_secrets = vault.get_secret(
            f"orchestrator/{ENVIRONMENT}/tfe/hprod"
        )

        logger.info("✅ Retrieved orchestrator secrets")

        return AccountSecrets(
            gitlab_token=gitlab_secret["token"],
            vault_token=vault.vault.token,
            tfe_secrets=tfe_secrets,
        )

    # ==========================================
    # STEP 4: CHECK ACCOUNT API KEYS
    # ==========================================
    
    @step(hide_output=True)
    def check_accounts_in_vault(
        updated_accounts: list[dict],
        vault: Vault = depends(vault_dependency),
    ) -> bool:
        """Verify all accounts have required API keys in Vault"""
        logger.info(f"🔐 Checking API keys for {len(updated_accounts)} accounts")
        
        missing_api_keys = []

        for account_info in updated_accounts:
            account_number = account_info["account_number"]
            account_name = account_info["account_name"]

            try:
                api_key_secret = vault.get_secret(
                    f"{account_number}/account-owner",
                    mount_point="ibmsid",
                )

                if api_key_secret is None or "api_key" not in api_key_secret:
                    missing_api_keys.append(f"{account_name} ({account_number})")

            except Exception as e:
                logger.error(f"❌ Error for {account_name}: {str(e)}")
                missing_api_keys.append(f"{account_name} ({account_number})")

        if missing_api_keys:
            raise DeclineDemandException(
                f"Missing API keys: {', '.join(missing_api_keys)}"
            )

        logger.info("✅ All accounts have valid API keys")
        return True

    # ==========================================
    # STEP 5: BUHUB ACCOUNT DETAILS
    # ==========================================
    
    @step
    def get_buhub_account_details(
        updated_accounts: list[dict],
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> AccountDetails | None:
        """Retrieve BUHUB account details if needed"""
        logger.info("🔍 Checking if BUHUB account is needed")

        wklapp_accounts = [
            acc for acc in updated_accounts
            if acc.get("account_type") == "WKLAPP"
        ]

        if not wklapp_accounts:
            logger.info("ℹ️ No WKLAPP accounts - BUHUB not needed")
            return None

        first_wklapp = wklapp_accounts[0]
        code_bu = first_wklapp.get("code_bu")

        logger.info(f"🔍 Looking for BUHUB for code_bu: {code_bu}")

        from account.database.utils.account_services import get_account_by_type_and_bu

        buhub_account = get_account_by_type_and_bu("BUHUB", code_bu, db_session)

        if not buhub_account:
            raise Exception(f"BUHUB account not found for code_bu: {code_bu}")

        reader_connector = ReaderConnector()
        buhub_account_details = reader_connector.get_account_details(
            buhub_account.name
        )

        logger.info("✅ BUHUB account details retrieved")
        return buhub_account_details

    # ==========================================
    # STEP 6: SCHEMATICS API KEY
    # ==========================================
    
    @step(hide_output=True)
    def get_schematics_api_key(
        buhub_account_details: AccountDetails | None,
        vault: Vault = depends(vault_dependency),
    ) -> str:
        """Get API key for Schematics operations"""
        logger.info("🔑 Retrieving API key for Schematics")

        if buhub_account_details is not None:
            api_key = vault.get_secret(
                f"{buhub_account_details['number']}/account-owner",
                mount_point="ibmsid",
            )["api_key"]

            logger.info("✅ Using BUHUB account API key")
            return api_key

        logger.info("ℹ️ Using GLBHUB account")
        api_key = vault.get_secret(
            f"{GLBHUB_ACCOUNT_NUMBER}/account-owner",
            mount_point="ibmsid",
        )["api_key"]

        logger.info("✅ Retrieved GLBHUB account API key")
        return api_key

    # ==========================================
    # STEP 7: GET WORKSPACES
    # ==========================================
    
    @step
    def get_workspaces_for_accounts(
        updated_accounts: list[dict],
        vault_secrets: AccountSecrets,  # Reçu depuis get_vault_secrets
        tf: SchematicsBackend = depends(schematics_backend_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> list[dict]:
        """Get Schematics workspaces for all accounts"""
        logger.info(f"📁 Looking for {len(updated_accounts)} workspaces")

        account_workspaces = []
        failed_accounts = []

        for account_info in updated_accounts:
            try:
                workspace_name = (
                    f"WS-ACCOUNT-DCLOUD-"
                    f"{account_info['environment_type'].upper()}-"
                    f"{account_info['account_type'].upper()}-"
                    f"{account_info['account_name'].upper()}"
                )

                logger.info(f"🔍 Searching: {workspace_name}")

                workspace_in_db: db_models.Workspace = (
                    db_session.query(db_models.Workspace)
                    .filter(
                        db_models.Workspace.name.contains(workspace_name),
                        db_models.Workspace.type == "schematics",
                    )
                    .one_or_none()
                )

                if not workspace_in_db:
                    raise ValueError(f"Workspace not found: {workspace_name}")

                workspace = tf.workspaces.get_by_id(workspace_in_db.id)
                status = SchematicsWorkspaceStatus(workspace.status)

                logger.info(f"✅ Found: {workspace_name} ({status.value})")

                account_workspaces.append({
                    **account_info,
                    "workspace_id": workspace_in_db.id,
                    "workspace_name": workspace_in_db.name,
                })

            except Exception as e:
                logger.error(f"❌ Error for {account_info['account_name']}: {str(e)}")
                failed_accounts.append(account_info["account_name"])

                update_deployment_status(
                    account_name=account_info["account_name"],
                    status="workspace_not_found",
                    session=db_session,
                    error_message=str(e),
                )

        if not account_workspaces:
            raise Exception(f"No workspaces found. Failed: {', '.join(failed_accounts)}")

        if failed_accounts:
            logger.warning(f"⚠️ Workspaces not found: {', '.join(failed_accounts)}")

        logger.info(f"✅ Found {len(account_workspaces)} workspaces")
        return account_workspaces

    # ==========================================
    # STEP 8: DEPLOY ACCOUNTS (PARALLEL)
    # ==========================================
    
    def deploy_single_account(
        account_workspace: dict,
        vault_secrets: AccountSecrets,
        payload: AccountUpdatePayload,
        db_session: SASession,
        tf: SchematicsBackend,
    ) -> dict:
        """Deploy configuration updates for a single account"""
        account_name = account_workspace["account_name"]
        workspace_id = account_workspace["workspace_id"]

        try:
            logger.info(f"🚀 Starting deployment: {account_name}")

            account = get_account_by_name(account_name, db_session)

            variables = {}

            if payload.updates.iam_version:
                variables["iam_version"] = account.iam_version

            if payload.updates.subnet_count is not None:
                variables["subnet_count"] = str(account.subnet_count)

            if payload.updates.enable_monitoring is not None:
                variables["enable_monitoring"] = str(account.enable_monitoring).lower()

            if payload.updates.tags:
                variables["tags"] = ",".join(account.tags)

            variables["vault_token"] = TerraformVar(vault_secrets.vault_token, True)

            logger.info(f"📦 Variables: {list(variables.keys())}")
            
            tf.workspaces.update_variables(workspace_id, variables)

            logger.info(f"📝 Creating plan")
            
            plan_job = tf.workspaces.plan(
                workspace_id=workspace_id,
                cooldown_policy=LinearCooldownPolicy(delay=180, max_attempts=540),
            )

            update_deployment_status(
                account_name=account_name,
                status="planning",
                session=db_session,
            )

            plan_result = tf.workspaces.wait_after_plan_or_apply(
                workspace_id=workspace_id,
                activity_id=plan_job.id,
            )

            if plan_result.status != "job_finished":
                raise Exception(f"Plan failed: {plan_result.status}")

            logger.info(f"✅ Plan completed")

            if payload.execution_mode == "plan_and_apply":
                logger.info(f"🔧 Applying changes")
                
                apply_job = tf.jobs.apply(workspace_id)
                apply_result = tf.jobs.wait_for_completion(apply_job.id, timeout=1800)

                if apply_result.status != "job_finished":
                    raise Exception(f"Apply failed: {apply_result.status}")

                final_status = "deployed"
                logger.info(f"✅ Apply completed")
            else:
                final_status = "plan_completed"

            update_deployment_status(
                account_name=account_name,
                status=final_status,
                session=db_session,
            )

            return {
                "account_name": account_name,
                "workspace_id": workspace_id,
                "status": "success",
                "final_status": final_status,
            }

        except Exception as e:
            logger.error(f"❌ Deployment failed for {account_name}: {str(e)}")

            update_deployment_status(
                account_name=account_name,
                status="deployment_failed",
                session=db_session,
                error_message=str(e),
            )

            return {
                "account_name": account_name,
                "workspace_id": workspace_id,
                "status": "failed",
                "error": str(e),
            }

    @step
    def deploy_accounts_parallel(
        account_workspaces: list[dict],
        vault_secrets: AccountSecrets,
        payload: AccountUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
        tf: SchematicsBackend = depends(schematics_backend_dependency),
    ) -> dict:
        """Deploy configuration updates to all accounts in parallel"""
        total_accounts = len(account_workspaces)
        max_parallel = payload.max_parallel_executions

        logger.info("🚀 Starting parallel deployment")
        logger.info(f"   Total: {total_accounts}")
        logger.info(f"   Max parallel: {max_parallel}")
        logger.info(f"   Mode: {payload.execution_mode}")

        results = {
            "successful": [],
            "failed": [],
            "total": total_accounts,
            "execution_mode": payload.execution_mode,
        }

        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {
                executor.submit(
                    deploy_single_account,
                    account_workspace,
                    vault_secrets,
                    payload,
                    db_session,
                    tf,
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
                        logger.error(f"❌ {account_name}: {result.get('error')}")
                        
                except Exception as e:
                    results["failed"].append({
                        "account_name": account_name,
                        "status": "failed",
                        "error": str(e),
                    })
                    logger.error(f"❌ {account_name}: Unexpected error: {str(e)}")

        logger.info("=" * 60)
        logger.info("📊 DEPLOYMENT SUMMARY")
        logger.info("=" * 60)
        logger.info(f"✅ Successful: {len(results['successful'])}/{total_accounts}")
        logger.info(f"❌ Failed: {len(results['failed'])}/{total_accounts}")
        logger.info("=" * 60)

        if len(results["failed"]) == total_accounts:
            raise Exception("All deployments failed")

        return results

    # ==========================================
    # ORCHESTRATION
    # ==========================================
    
    # Exécuter les steps
    accounts_valid = check_accounts_exist()
    updated_accounts = update_accounts_database()
    vault_secrets = get_vault_secrets()
    accounts_in_vault = check_accounts_in_vault(updated_accounts)
    buhub_account_details = get_buhub_account_details(updated_accounts)
    schematics_api_key = get_schematics_api_key(buhub_account_details)

    account_workspaces = get_workspaces_for_accounts(
        updated_accounts=updated_accounts,
        vault_secrets=vault_secrets,
    )

    results = deploy_accounts_parallel(
        account_workspaces=account_workspaces,
        vault_secrets=vault_secrets,
    )

    # ==========================================
    # DEPENDENCIES
    # ==========================================
    
    # ✅ CORRECTION: Chaîner uniquement des steps, pas des dicts !
    accounts_valid >> updated_accounts
    updated_accounts >> vault_secrets
    vault_secrets >> accounts_in_vault
    accounts_in_vault >> buhub_account_details
    buhub_account_details >> schematics_api_key
    schematics_api_key >> account_workspaces
    account_workspaces >> results

    return results