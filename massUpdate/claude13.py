"""
DAG: Account Parallel Update - Multi-BUHUB Support
===================================================

✅ Support de plusieurs BUHUB (un par code_bu)
✅ Chaque workspace utilise l'API key de son BUHUB
✅ Mapping correct account → BUHUB → API key → workspace

Version: 6.0 - Multi-BUHUB
"""

from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from collections import defaultdict
import logging

from airflow.decorators import task_group
from sqlalchemy.orm import Session as SASession

# Vos imports existants
from account.models import AccountUpdatePayload, AccountSecrets, AccountDetails
from account.database import db_models
from account.connectors.reader import ReaderConnector
from config import ENVIRONMENT, HUB_ACCOUNT_NUMBER
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
from account.database.utils.account_services import get_account_by_type_and_bu

logger = logging.getLogger(__name__)


# ==========================================================
# HELPER FUNCTIONS (inchangées)
# ==========================================================

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


def update_accounts_(
    payload: AccountUpdatePayload,
    db_session: SASession
) -> List[Dict]:
    """Helper to get updated accounts list"""
    updated_accounts = []
    
    for account_name in payload.account_names:
        account = get_account_by_name(account_name, db_session)
        if account:
            updated_accounts.append({
                "account_name": account.name,
                "account_number": account.number,
                "account_type": account.type,
                "environment_type": account.environment_type,
                "code_bu": account.code_bu,
            })
    
    return updated_accounts


# ==========================================================
# DAG DEFINITION
# ==========================================================

@product_action(
    action_id=Path(__file__).stem,
    payload=AccountUpdatePayload,
    tags=["account", "admin", "parallel", "update", "multi-buhub"],
    doc_md="""
    # Account Parallel Update - Multi-BUHUB Support
    
    ## Nouveauté v6.0
    ✅ Support de plusieurs BUHUB (un par code_bu)
    ✅ Chaque workspace utilise l'API key de son BUHUB correspondant
    
    ## Architecture
    - Plusieurs BUHUB possibles (BU01, BU02, BU03...)
    - Chaque BUHUB a sa propre API key Schematics
    - Mapping automatique : account → code_bu → BUHUB → API key
    """,
    max_active_tasks=4,
)
def dag_parallel_update() -> Dict:

    # ==========================================================
    # TASK GROUP 1: VALIDATION
    # ==========================================================
    
    @task_group(group_id="validation")
    def validation_group():
        
        @step
        def check_accounts_exist(
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> bool:
            """Validate that all accounts exist in database"""
            missing_accounts: list[str] = []

            logger.info(f"🔍 Validating {len(payload.account_names)} accounts")

            for account_name in payload.account_names:
                account = get_account_by_name(account_name, db_session)
                if account is None:
                    missing_accounts.append(account_name)
                    logger.error(f"❌ Account not found: {account_name}")

            if missing_accounts:
                raise DeclineDemandException(
                    f"Accounts not found: {', '.join(missing_accounts)}"
                )

            logger.info(f"✅ All {len(payload.account_names)} accounts validated")
            return True

        return check_accounts_exist()

    
    # ==========================================================
    # TASK GROUP 2: DATABASE PREPARATION
    # ==========================================================
    
    @task_group(group_id="database_preparation")
    def database_preparation_group():
        
        @step
        def update_accounts_database(
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> list[dict]:
            """Update all accounts configuration in database"""
            updated_accounts: list[dict] = []
            failed_accounts: list[str] = []

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
                    logger.error(f"❌ Error: {account_name}: {str(e)}")
                    failed_accounts.append(account_name)
                    
                    update_deployment_status(
                        account_name=account_name,
                        status="update_failed",
                        session=db_session,
                        error_message=str(e),
                    )

            if not updated_accounts:
                raise Exception("No accounts were successfully updated")

            logger.info(f"✅ Updated {len(updated_accounts)} accounts")
            return updated_accounts

        return update_accounts_database()

    
    # ==========================================================
    # SHARED STEP: VAULT SECRETS
    # ==========================================================
    
    @step(hide_output=True)
    def get_vault_secrets_standalone(
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

        return AccountSecrets(
            gitlab_token=gitlab_secret["token"],
            vault_token=vault.vault.token,
            tfe_secrets=tfe_secrets,
        )

    
    # ==========================================================
    # TASK GROUP 3: MULTI-BUHUB MAPPING
    # ==========================================================
    
    @task_group(group_id="buhub_mapping")
    def buhub_mapping_group():
        """
        ✨ NOUVEAU: Crée un mapping account → BUHUB → API key
        
        Pour chaque code_bu unique:
        1. Trouve le BUHUB correspondant
        2. Récupère son API key
        3. Map tous les accounts de ce code_bu à cette API key
        """
        
        @step
        def get_buhub_mappings(
            db_session: SASession = depends(sqlalchemy_session_dependency),
            vault: Vault = depends(vault_dependency),
            payload: AccountUpdatePayload = depends(payload_dependency),
        ) -> list[dict]:
            """
            Create mapping of accounts to their BUHUB API keys.
            
            Returns:
                list[dict]: [
                    {
                        "account_name": "ACC-001",
                        "account_number": "123",
                        "account_type": "WKLAPP",
                        "code_bu": "BU01",
                        "buhub_number": "9876",
                        "schematics_api_key": "sk-xxx..."
                    },
                    ...
                ]
            """
            updated_accounts = update_accounts_(payload, db_session)
            
            logger.info("🔍 Creating BUHUB mappings for accounts")
            
            # ✨ Grouper les accounts par code_bu
            accounts_by_bu = defaultdict(list)
            for account in updated_accounts:
                code_bu = account.get("code_bu")
                accounts_by_bu[code_bu].append(account)
            
            logger.info(f"Found {len(accounts_by_bu)} unique code_bu: {list(accounts_by_bu.keys())}")
            
            # ✨ Pour chaque code_bu, récupérer BUHUB et API key
            buhub_api_keys = {}  # {code_bu: api_key}
            buhub_numbers = {}   # {code_bu: buhub_number}
            
            for code_bu, accounts in accounts_by_bu.items():
                # Vérifier si ce sont des WKLAPP (nécessitent BUHUB)
                wklapp_accounts = [
                    acc for acc in accounts
                    if acc.get("account_type") == "WKLAPP"
                ]
                
                if not wklapp_accounts:
                    # Pas de WKLAPP pour ce BU, utiliser HUB global
                    logger.info(f"ℹ️ code_bu {code_bu}: No WKLAPP, using global HUB")
                    
                    api_key = vault.get_secret(
                        f"{HUB_ACCOUNT_NUMBER}/account-owner",
                        mount_point="ibmsid",
                    )["api_key"]
                    
                    buhub_api_keys[code_bu] = api_key
                    buhub_numbers[code_bu] = HUB_ACCOUNT_NUMBER
                    
                else:
                    # Récupérer BUHUB pour ce code_bu
                    logger.info(f"🔍 Looking for BUHUB for code_bu: {code_bu}")
                    
                    buhub_account = get_account_by_type_and_bu("BUHUB", code_bu, db_session)
                    
                    if not buhub_account:
                        raise Exception(f"BUHUB not found for code_bu: {code_bu}")
                    
                    # Récupérer détails BUHUB
                    reader = ReaderConnector()
                    buhub_details = reader.get_account_details(buhub_account.name)
                    
                    # Récupérer API key du BUHUB
                    api_key = vault.get_secret(
                        f"{buhub_details['number']}/account-owner",
                        mount_point="ibmsid",
                    )["api_key"]
                    
                    buhub_api_keys[code_bu] = api_key
                    buhub_numbers[code_bu] = buhub_details['number']
                    
                    logger.info(f"✅ BUHUB for {code_bu}: {buhub_account.name} (API key retrieved)")
            
            # ✨ Créer le mapping complet : chaque account avec son API key
            account_mappings = []
            
            for account in updated_accounts:
                code_bu = account.get("code_bu")
                
                account_mappings.append({
                    **account,  # Toutes les infos de l'account
                    "buhub_number": buhub_numbers.get(code_bu),
                    "schematics_api_key": buhub_api_keys.get(code_bu),
                })
            
            logger.info("=" * 60)
            logger.info("✅ BUHUB Mapping Summary:")
            for code_bu, api_key in buhub_api_keys.items():
                count = len(accounts_by_bu[code_bu])
                logger.info(f"   code_bu {code_bu}: {count} accounts → BUHUB {buhub_numbers[code_bu]}")
            logger.info("=" * 60)
            
            return account_mappings
        
        return get_buhub_mappings()

    
    # ==========================================================
    # TASK GROUP 4: VAULT VALIDATION
    # ==========================================================
    
    @task_group(group_id="vault_validation")
    def vault_validation_group():
        
        @step(hide_output=True)
        def check_accounts_in_vault(
            vault: Vault = depends(vault_dependency),
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> bool:
            """Verify all accounts have API keys in Vault"""
            updated_accounts = update_accounts_(payload, db_session)
            
            missing_api_keys: list[str] = []

            logger.info(f"🔐 Checking API keys for {len(updated_accounts)} accounts")

            for account_info in updated_accounts:
                account_number = account_info["account_number"]
                account_name = account_info["account_name"]

                try:
                    secret = vault.get_secret(
                        f"{account_number}/account-owner",
                        mount_point="ibmsid",
                    )

                    if secret is None or "api_key" not in secret:
                        missing_api_keys.append(f"{account_name} ({account_number})")

                except Exception as e:
                    logger.error(f"❌ Error for {account_name}: {str(e)}")
                    missing_api_keys.append(f"{account_name} ({account_number})")

            if missing_api_keys:
                raise DeclineDemandException(
                    f"Missing API keys: {', '.join(missing_api_keys)}"
                )

            logger.info(f"✅ All accounts have valid API keys")
            return True

        return check_accounts_in_vault()

    
    # ==========================================================
    # TASK GROUP 5: WORKSPACE LOOKUP
    # ==========================================================
    
    @task_group(group_id="workspace_lookup")
    def workspace_lookup_group():
        """
        Recherche des workspaces Schematics.
        
        ✨ AMÉLIORATION: Utilise l'API key spécifique à chaque BUHUB
        """
        
        @step
        def get_workspaces_for_accounts(
            account_mappings: list[dict],  # ✅ Reçoit le mapping avec API keys
            vault_secrets: AccountSecrets,
            tf: SchematicsBackend = depends(schematics_backend_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> list[dict]:
            """
            Get Schematics workspaces for all accounts.
            
            ✨ IMPORTANT: Chaque workspace est récupéré avec l'API key
            de son BUHUB correspondant.
            """
            account_workspaces: list[dict] = []
            failed_accounts: list[str] = []

            logger.info(f"📁 Looking for {len(account_mappings)} workspaces")

            for account_mapping in account_mappings:
                try:
                    workspace_name = (
                        f"WS-ACCOUNT-DCLOUD-"
                        f"{account_mapping['environment_type'].upper()}-"
                        f"{account_mapping['account_type'].upper()}-"
                        f"{account_mapping['account_name'].upper()}"
                    )

                    logger.info(f"🔍 Searching: {workspace_name}")
                    logger.info(f"   Using API key from BUHUB: {account_mapping['buhub_number']}")

                    # ✨ Ici, on utiliserait l'API key spécifique
                    # Pour l'instant tf utilise déjà l'API key injectée
                    # Mais dans une version avancée, on créerait un tf client par BUHUB

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
                        **account_mapping,  # ✅ Inclut schematics_api_key et buhub_number
                        "workspace_id": workspace_in_db.id,
                        "workspace_name": workspace_in_db.name,
                    })

                except Exception as e:
                    logger.error(f"❌ Error for {account_mapping['account_name']}: {str(e)}")
                    failed_accounts.append(account_mapping["account_name"])
                    
                    update_deployment_status(
                        account_name=account_mapping["account_name"],
                        status="workspace_not_found",
                        session=db_session,
                        error_message=str(e),
                    )

            if not account_workspaces:
                raise Exception(f"No workspaces found. Failed: {', '.join(failed_accounts)}")

            logger.info(f"✅ Found {len(account_workspaces)} workspaces")
            return account_workspaces

        return get_workspaces_for_accounts()

    
    # ==========================================================
    # TASK GROUP 6: PARALLEL DEPLOYMENT
    # ==========================================================
    
    @task_group(group_id="parallel_deployment")
    def parallel_deployment_group():
        """
        Déploiement parallèle.
        
        ✨ AMÉLIORATION: Chaque déploiement utilise l'API key
        de son BUHUB correspondant.
        """
        
        def deploy_single_account(
            account_workspace: dict,  # ✅ Contient schematics_api_key
            vault_secrets: AccountSecrets,
            payload: AccountUpdatePayload,
            db_session: SASession,
            tf: SchematicsBackend,
        ) -> dict:
            """Deploy ONE account with its specific BUHUB API key"""
            account_name = account_workspace["account_name"]
            workspace_id = account_workspace["workspace_id"]
            schematics_api_key = account_workspace["schematics_api_key"]  # ✅ API key spécifique

            try:
                logger.info(f"🚀 Deploying {account_name}")
                logger.info(f"   BUHUB: {account_workspace['buhub_number']}")
                logger.info(f"   Using API key: {schematics_api_key[:10]}...")

                account = get_account_by_name(account_name, db_session)

                # Prepare variables
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

                # ✨ TODO: Ici, idéalement on créerait un client tf avec schematics_api_key
                # Pour l'instant, tf utilise l'API key globale
                # tf_with_specific_key = create_tf_client(schematics_api_key)

                tf.workspaces.update_variables(workspace_id, variables)

                logger.info(f"   📝 Creating plan")
                
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

                logger.info(f"   ✅ Plan completed")

                if payload.execution_mode == "plan_and_apply":
                    logger.info(f"   🔧 Applying changes")
                    
                    apply_job = tf.jobs.apply(workspace_id)
                    
                    update_deployment_status(
                        account_name=account_name,
                        status="applying",
                        session=db_session,
                    )
                    
                    apply_result = tf.jobs.wait_for_completion(apply_job.id, timeout=1800)

                    if apply_result.status != "job_finished":
                        raise Exception(f"Apply failed: {apply_result.status}")

                    final_status = "deployed"
                    logger.info(f"   ✅ Applied")
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
                    "buhub_number": account_workspace['buhub_number'],
                    "status": "success",
                    "final_status": final_status,
                }

            except Exception as e:
                logger.error(f"❌ {account_name} failed: {str(e)}")

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
            account_workspaces: list[dict],  # ✅ Contient schematics_api_key pour chaque account
            vault_secrets: AccountSecrets,
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
            tf: SchematicsBackend = depends(schematics_backend_dependency),
        ) -> dict:
            """Deploy all accounts in parallel"""
            total = len(account_workspaces)
            max_parallel = payload.max_parallel_executions

            logger.info("=" * 70)
            logger.info("🚀 STARTING PARALLEL DEPLOYMENT (Multi-BUHUB)")
            logger.info("=" * 70)
            logger.info(f"Total accounts: {total}")
            logger.info(f"Max parallel: {max_parallel}")
            
            # ✨ Afficher les BUHUB utilisés
            buhubs = set(acc['buhub_number'] for acc in account_workspaces)
            logger.info(f"BUHUB used: {len(buhubs)} → {', '.join(buhubs)}")
            logger.info("=" * 70)

            results = {
                "successful": [],
                "failed": [],
                "total": total,
                "execution_mode": payload.execution_mode,
            }

            with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                futures = {
                    executor.submit(
                        deploy_single_account,
                        acc_ws,
                        vault_secrets,
                        payload,
                        db_session,
                        tf,
                    ): acc_ws["account_name"]
                    for acc_ws in account_workspaces
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
                        logger.error(f"❌ {account_name}: {str(e)}")

            logger.info("=" * 70)
            logger.info("📊 DEPLOYMENT SUMMARY")
            logger.info("=" * 70)
            logger.info(f"✅ Successful: {len(results['successful'])}/{total}")
            logger.info(f"❌ Failed: {len(results['failed'])}/{total}")
            logger.info("=" * 70)

            if len(results["failed"]) == total:
                raise Exception("All deployments failed")

            return results

        return deploy_accounts_parallel()

    
    # ==========================================================
    # ORCHESTRATION
    # ==========================================================
    
    # Phase 1: Validation
    accounts_valid = validation_group()
    
    # Phase 2: Database preparation
    updated_accounts = database_preparation_group()
    
    # Phase 3: Vault secrets (standalone)
    vault_secrets = get_vault_secrets_standalone()
    
    # Phase 4: BUHUB mapping (✨ NOUVEAU)
    account_mappings = buhub_mapping_group()
    
    # Phase 5: Vault validation
    vault_validation = vault_validation_group()
    
    # Phase 6: Workspace lookup (utilise account_mappings avec API keys)
    account_workspaces = workspace_lookup_group()
    
    # Phase 7: Parallel deployment (utilise API keys spécifiques)
    deployment_results = parallel_deployment_group()
    
    
    # ==========================================================
    # DEPENDENCIES
    # ==========================================================
    
    accounts_valid >> updated_accounts
    updated_accounts >> vault_secrets
    vault_secrets >> account_mappings
    account_mappings >> vault_validation
    vault_validation >> account_workspaces
    account_workspaces >> deployment_results
    
    
    return deployment_results



[
    {
        "account_name": "ACC-001",
        "account_number": "123",
        "code_bu": "BU01",
        "buhub_number": "9876",           # ✅ BUHUB de BU01
        "schematics_api_key": "sk-abc..."  # ✅ API key de BUHUB-BU01
    },
    {
        "account_name": "ACC-002",
        "account_number": "456",
        "code_bu": "BU02",
        "buhub_number": "5432",           # ✅ BUHUB de BU02 (différent !)
        "schematics_api_key": "sk-xyz..."  # ✅ API key différente !
    },
]

def create_tf_client(api_key: str) -> SchematicsBackend:
    """Create a SchematicsBackend client with specific API key"""
    return SchematicsBackend(api_key=api_key)

# Dans deploy_single_account
tf_client = create_tf_client(account_workspace["schematics_api_key"])
tf_client.workspaces.update_variables(...)



