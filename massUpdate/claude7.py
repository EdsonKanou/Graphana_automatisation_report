"""
DAG: Account Parallel Update - Version Optimisée
=================================================

Architecture avec TaskGroups et Dynamic Task Mapping pour :
- Visibilité granulaire dans l'UI Airflow
- Retry indépendant par compte
- Parallélisme natif Airflow
- Clean code et maintenabilité

Auteur: Platform Team
Version: 3.0 (Optimized)
"""

from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import logging

from airflow.decorators import task_group
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
# HELPER FUNCTIONS (Inchangées)
# ==========================================

def get_account_by_name(name: str, session: SASession):
    """Retrieve account from database by name"""
    return (
        session.query(db_models.AccountExtractIAM)
        .filter(db_models.AccountExtractIAM.account_name.contains(name))
        .first()
    )


def update_account_config(
    account_name: str,
    updates,
    session: SASession,
):
    """Update account configuration in database"""
    account = get_account_by_name(account_name, session)
    
    if account is None:
        raise ValueError(f"Account {account_name} not found")
    
    # Apply updates
    if updates.iam_version:
        account.iam_version = updates.iam_version
    if updates.subnet_count is not None:
        account.subnet_count = updates.subnet_count
    if updates.enable_monitoring is not None:
        account.enable_monitoring = updates.enable_monitoring
    if updates.tags:
        account.tags = updates.tags
    
    # Tracking
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
    tags=["account", "admin", "parallel", "update", "v3-optimized"],
    doc_md="DAG optimisé pour mise à jour parallèle de comptes avec TaskGroups et Dynamic Mapping."
)
def dag_parallel_update() -> Dict:

    # ==========================================
    # TASK GROUP 1: VALIDATION
    # ==========================================
    
    @task_group(group_id="validation")
    def validation_group():
        """
        Groupe de validation des prérequis.
        
        Vérifie que tous les comptes existent en BD.
        """
        
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
        
        return check_accounts_exist()
    
    
    # ==========================================
    # TASK GROUP 2: DATABASE PREPARATION
    # ==========================================
    
    @task_group(group_id="database_preparation")
    def database_preparation_group():
        """
        Groupe de préparation de la base de données.
        
        Met à jour les configurations dans PostgreSQL.
        """
        
        @step
        def update_accounts_database(
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> list[dict]:
            """Update all accounts configuration in database"""
            updated_accounts = []
            failed_accounts = []

            logger.info(f"📝 Updating {len(payload.account_names)} accounts in database")

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
                    failed_accounts.append(account_name)
                    
                    update_deployment_status(
                        account_name=account_name,
                        status="update_failed",
                        session=db_session,
                        error_message=str(e),
                    )

            if not updated_accounts:
                raise Exception("No accounts were successfully updated")

            if failed_accounts:
                logger.warning(f"⚠️ Failed to update: {', '.join(failed_accounts)}")

            logger.info(f"✅ Updated {len(updated_accounts)}/{len(payload.account_names)} accounts")
            return updated_accounts
        
        return update_accounts_database()
    
    
    # ==========================================
    # TASK GROUP 3: VAULT & SECRETS
    # ==========================================
    
    @task_group(group_id="vault_and_secrets")
    def vault_and_secrets_group():
        """
        Groupe de gestion des secrets Vault.
        
        - Récupère secrets orchestrator
        - Vérifie API keys des comptes
        - Récupère API key Schematics
        """
        
        @step(hide_output=True)
        def get_vault_secrets(
            payload: AccountUpdatePayload = depends(payload_dependency),
            vault: Vault = depends(vault_dependency),
        ) -> AccountSecrets:
            """Get orchestrator secrets from Vault"""
            logger.info("🔐 Retrieving orchestrator secrets from Vault")

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
                    logger.error(f"❌ Vault error for {account_name}: {str(e)}")
                    missing_api_keys.append(f"{account_name} ({account_number})")

            if missing_api_keys:
                raise DeclineDemandException(
                    f"Missing API keys in Vault: {', '.join(missing_api_keys)}"
                )

            logger.info("✅ All accounts have valid API keys")
            return True
        
        
        @step
        def get_buhub_account_details(
            updated_accounts: list[dict],
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> AccountDetails | None:
            """Retrieve BUHUB account details if needed for WKLAPP accounts"""
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
        
        
        @step(hide_output=True)
        def get_schematics_api_key(
            buhub_account_details: AccountDetails | None,
            vault: Vault = depends(vault_dependency),
        ) -> str:
            """Get API key for Schematics (BUHUB or GLBHUB)"""
            logger.info("🔑 Retrieving API key for Schematics")

            if buhub_account_details is not None:
                api_key = vault.get_secret(
                    f"{buhub_account_details['number']}/account-owner",
                    mount_point="ibmsid",
                )["api_key"]

                logger.info("✅ Using BUHUB account API key")
                return api_key

            logger.info("ℹ️ Using GLBHUB account (fallback)")
            api_key = vault.get_secret(
                f"{GLBHUB_ACCOUNT_NUMBER}/account-owner",
                mount_point="ibmsid",
            )["api_key"]

            logger.info("✅ Retrieved GLBHUB account API key")
            return api_key
        
        
        # Flow du groupe
        secrets = get_vault_secrets()
        api_keys_verified = check_accounts_in_vault()
        buhub_details = get_buhub_account_details()
        schematics_key = get_schematics_api_key(buhub_details)
        
        return {
            "vault_secrets": secrets,
            "api_keys_verified": api_keys_verified,
            "schematics_api_key": schematics_key,
        }
    
    
    # ==========================================
    # TASK GROUP 4: WORKSPACE LOOKUP
    # ==========================================
    
    @task_group(group_id="workspace_lookup")
    def workspace_lookup_group():
        """
        Groupe de recherche des workspaces Schematics.
        
        Trouve le workspace pour chaque compte.
        """
        
        @step
        def get_workspaces_for_accounts(
            updated_accounts: list[dict],
            vault_data: dict,
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

                    logger.info(f"✅ Found workspace: {workspace_name} (status: {status.value})")

                    account_workspaces.append({
                        **account_info,
                        "workspace_id": workspace_in_db.id,
                        "workspace_name": workspace_in_db.name,
                    })

                except Exception as e:
                    logger.error(f"❌ Workspace error for {account_info['account_name']}: {str(e)}")
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
        
        return get_workspaces_for_accounts()
    
    
    # ==========================================
    # TASK GROUP 5: PARALLEL DEPLOYMENT
    # ==========================================
    
    @task_group(group_id="parallel_deployment")
    def parallel_deployment_group():
        """
        Groupe de déploiement parallèle.
        
        DEUX OPTIONS:
        A) Dynamic Task Mapping (recommandé) - 1 tâche par compte visible
        B) ThreadPoolExecutor (actuel) - 1 tâche globale
        
        Je fournis les DEUX pour que tu choisisses.
        """
        
        # ==========================================
        # OPTION A: DYNAMIC TASK MAPPING (RECOMMANDÉ)
        # ==========================================
        
        @step
        def deploy_single_account_dynamic(
            account_workspace: dict,
            vault_secrets: AccountSecrets,
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
            tf: SchematicsBackend = depends(schematics_backend_dependency),
        ) -> dict:
            """
            Déploie UN SEUL compte.
            
            Cette fonction sera mappée dynamiquement pour créer
            une tâche par compte dans l'UI Airflow.
            """
            account_name = account_workspace["account_name"]
            workspace_id = account_workspace["workspace_id"]

            try:
                logger.info(f"🚀 Starting deployment for: {account_name}")

                # 1. Get account config
                account = get_account_by_name(account_name, db_session)

                # 2. Prepare variables
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

                # 3. Update workspace
                tf.workspaces.update_variables(workspace_id, variables)
                logger.info(f"✅ Workspace variables updated")

                # 4. Terraform Plan
                logger.info(f"📝 Creating Terraform plan")
                
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

                # 5. Terraform Apply (if requested)
                if payload.execution_mode == "plan_and_apply":
                    logger.info(f"🔧 Applying Terraform changes")
                    
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
                    logger.info(f"✅ Apply completed")
                else:
                    final_status = "plan_completed"

                # 6. Update final status
                update_deployment_status(
                    account_name=account_name,
                    status=final_status,
                    session=db_session,
                )

                logger.info(f"✅ {account_name}: {final_status}")

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
        def aggregate_deployment_results(
            deployment_results: list[dict],
            payload: AccountUpdatePayload = depends(payload_dependency),
        ) -> dict:
            """
            Agrège les résultats de tous les déploiements.
            
            Cette tâche s'exécute APRÈS que toutes les tâches
            deploy_single_account_dynamic soient terminées.
            """
            successful = [r for r in deployment_results if r["status"] == "success"]
            failed = [r for r in deployment_results if r["status"] == "failed"]

            results = {
                "successful": successful,
                "failed": failed,
                "total": len(deployment_results),
                "execution_mode": payload.execution_mode,
            }

            logger.info("=" * 70)
            logger.info("📊 DEPLOYMENT SUMMARY")
            logger.info("=" * 70)
            logger.info(f"Total: {results['total']}")
            logger.info(f"✅ Successful: {len(successful)}")
            logger.info(f"❌ Failed: {len(failed)}")
            logger.info(f"Success rate: {len(successful)/results['total']*100:.1f}%")
            logger.info("=" * 70)

            if len(failed) == results["total"]:
                raise Exception("All deployments failed")

            return results
        
        
        # ==========================================
        # OPTION B: THREADPOOLEXECUTOR (ACTUEL)
        # ==========================================
        
        @step
        def deploy_accounts_with_threadpool(
            account_workspaces: list[dict],
            vault_secrets: AccountSecrets,
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
            tf: SchematicsBackend = depends(schematics_backend_dependency),
        ) -> dict:
            """
            Déploie tous les comptes en parallèle avec ThreadPoolExecutor.
            
            ATTENTION: Cette approche crée UNE SEULE tâche dans l'UI,
            donc pas de visibilité granulaire par compte.
            """
            total_accounts = len(account_workspaces)
            max_parallel = payload.max_parallel_executions

            logger.info("🚀 Starting parallel deployment with ThreadPoolExecutor")
            logger.info(f"Total accounts: {total_accounts}")
            logger.info(f"Max parallel: {max_parallel}")
            logger.info(f"Mode: {payload.execution_mode}")

            results = {
                "successful": [],
                "failed": [],
                "total": total_accounts,
                "execution_mode": payload.execution_mode,
            }

            # Fonction helper pour déployer 1 compte
            def deploy_one_account(acc_ws):
                return deploy_single_account_dynamic(
                    account_workspace=acc_ws,
                    vault_secrets=vault_secrets,
                    payload=payload,
                    db_session=db_session,
                    tf=tf,
                )

            # Exécution parallèle
            with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                futures = {
                    executor.submit(deploy_one_account, acc_ws): acc_ws["account_name"]
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
                        logger.error(f"❌ {account_name}: Unexpected error: {str(e)}")

            logger.info("=" * 70)
            logger.info("📊 DEPLOYMENT SUMMARY")
            logger.info("=" * 70)
            logger.info(f"✅ Successful: {len(results['successful'])}/{total_accounts}")
            logger.info(f"❌ Failed: {len(results['failed'])}/{total_accounts}")
            logger.info("=" * 70)

            if len(results["failed"]) == total_accounts:
                raise Exception("All deployments failed")

            return results
        
        
        # ==========================================
        # RETOURNE L'OPTION CHOISIE
        # ==========================================
        
        # OPTION A (Recommandé): Décommente ces lignes
        # deployments = deploy_single_account_dynamic.expand(
        #     account_workspace=account_workspaces
        # )
        # return aggregate_deployment_results(deployments)
        
        # OPTION B (Actuel): Garde cette ligne
        return deploy_accounts_with_threadpool()
    
    
    # ==========================================
    # ORCHESTRATION GLOBALE
    # ==========================================
    
    # Phase 1: Validation
    accounts_valid = validation_group()
    
    # Phase 2: Database preparation
    updated_accounts = database_preparation_group()
    
    # Phase 3: Vault & secrets
    vault_data = vault_and_secrets_group()
    
    # Phase 4: Workspace lookup
    account_workspaces = workspace_lookup_group()
    
    # Phase 5: Parallel deployment
    deployment_results = parallel_deployment_group()
    
    
    # ==========================================
    # DEPENDENCIES
    # ==========================================
    
    accounts_valid >> updated_accounts
    updated_accounts >> vault_data
    vault_data >> account_workspaces
    account_workspaces >> deployment_results
    
    return deployment_results



# 🚀 Guide de migration - DAG Optimisé

## 📊 Comparaison : Avant vs Après

### **Structure visuelle dans l'UI Airflow**

#### **AVANT (Version actuelle)**
```
dag_parallel_update
├── check_accounts_exist
├── update_accounts_database
├── get_vault_secrets
├── check_accounts_in_vault
├── get_buhub_account_details
├── get_schematics_api_key
├── get_workspaces_for_accounts
└── deploy_accounts_parallel  ← 1 seule tâche pour TOUS les comptes
```

**Problème :** Impossible de voir le détail de chaque compte.

---

#### **APRÈS (Version optimisée)**
```
dag_parallel_update
├── validation
│   └── check_accounts_exist
├── database_preparation
│   └── update_accounts_database
├── vault_and_secrets
│   ├── get_vault_secrets
│   ├── check_accounts_in_vault
│   ├── get_buhub_account_details
│   └── get_schematics_api_key
├── workspace_lookup
│   └── get_workspaces_for_accounts
└── parallel_deployment
    ├── deploy_single_account_dynamic[ACC-001]  ← Tâche visible par compte
    ├── deploy_single_account_dynamic[ACC-002]  ← Chacune indépendante
    ├── deploy_single_account_dynamic[ACC-003]
    └── aggregate_deployment_results
```

**Avantage :** Vision granulaire + retry sélectif !

---

## 🎯 Les deux options de déploiement

### **OPTION A: Dynamic Task Mapping (Recommandé)**

#### **Avantages**
✅ **Visibilité totale** : Chaque compte = 1 tâche dans l'UI  
✅ **Retry indépendant** : Relancer juste ACC-002 si il a échoué  
✅ **Monitoring précis** : Voir en temps réel quel compte est en cours  
✅ **Logs isolés** : Logs séparés par compte  
✅ **Parallélisme natif Airflow** : Géré par `max_active_tis_per_dag`  

#### **Inconvénients**
⚠️ Si tu as **beaucoup de comptes** (50+), l'UI peut devenir chargée  
⚠️ Nécessite Airflow 2.3+ (pour `expand()`)  

#### **Code à utiliser**
```python
@task_group(group_id="parallel_deployment")
def parallel_deployment_group():
    
    @step
    def deploy_single_account_dynamic(account_workspace: dict, ...):
        # Déploie UN compte
        pass
    
    @step
    def aggregate_deployment_results(deployment_results: list[dict]):
        # Agrège tous les résultats
        pass
    
    # MAPPING DYNAMIQUE
    deployments = deploy_single_account_dynamic.expand(
        account_workspace=account_workspaces
    )
    
    return aggregate_deployment_results(deployments)
```

#### **Configuration du parallélisme**
```python
# Dans airflow.cfg ou via UI
[core]
max_active_tasks_per_dag = 5  # Max de tâches en parallèle pour ce DAG
parallelism = 32               # Global Airflow
```

---

### **OPTION B: ThreadPoolExecutor (Actuel)**

#### **Avantages**
✅ **Simple** : Aucun changement de config Airflow  
✅ **Compact dans l'UI** : 1 seule tâche  
✅ **Fonctionne avec n'importe quelle version Airflow**  

#### **Inconvénients**
❌ **Pas de visibilité granulaire** : Tous les comptes dans 1 tâche  
❌ **Retry global** : Si 1 compte échoue et tu retry, TOUS sont relancés  
❌ **Logs mélangés** : Difficile de filtrer par compte  
❌ **Pas de relance sélective** : Impossible de relancer juste ACC-002  

#### **Code (déjà dans ton DAG actuel)**
```python
@step
def deploy_accounts_with_threadpool(...):
    with ThreadPoolExecutor(max_workers=max_parallel) as executor:
        futures = {
            executor.submit(deploy_one_account, acc): acc["name"]
            for acc in account_workspaces
        }
        # ...
```

---

## 🔄 Migration : Comment passer à l'Option A

### **Étape 1 : Vérifier la version Airflow**
```bash
airflow version
# Nécessite >= 2.3.0 pour expand()
```

### **Étape 2 : Modifier le code**

Dans le fichier artifact, cherche cette section :

```python
# OPTION A (Recommandé): Décommente ces lignes
# deployments = deploy_single_account_dynamic.expand(
#     account_workspace=account_workspaces
# )
# return aggregate_deployment_results(deployments)

# OPTION B (Actuel): Garde cette ligne
return deploy_accounts_with_threadpool()
```

**Remplace par :**
```python
# OPTION A (Recommandé): ACTIVÉ
deployments = deploy_single_account_dynamic.expand(
    account_workspace=account_workspaces
)
return aggregate_deployment_results(deployments)

# OPTION B (Actuel): DÉSACTIVÉ
# return deploy_accounts_with_threadpool()
```

### **Étape 3 : Configurer le parallélisme**

Via l'UI Airflow ou `airflow.cfg` :
```ini
[core]
max_active_tasks_per_dag = 5  # Nombre max de comptes en parallèle
```

Ou via le DAG :
```python
@product_action(
    ...
    max_active_tasks=5,  # Limite à 5 comptes en parallèle
)
```

### **Étape 4 : Tester avec 2-3 comptes d'abord**

```json
{
  "account_names": ["ACC-TEST-01", "ACC-TEST-02"],
  "updates": {"iam_version": "2.0"},
  "execution_mode": "plan_only",
  "max_parallel_executions": 2
}
```

### **Étape 5 : Vérifier dans l'UI**

Tu devrais maintenant voir :
```
parallel_deployment
├── deploy_single_account_dynamic[0]  ← ACC-TEST-01
├── deploy_single_account_dynamic[1]  ← ACC-TEST-02
└── aggregate_deployment_results      ← Résumé final
```

---

## 🎨 Améliorations visuelles apportées

### **1. TaskGroups hiérarchiques**

**Avant :**
```
10 tâches au même niveau → Difficile à lire
```

**Après :**
```
5 groupes logiques → Structure claire
```

### **2. Nommage cohérent**

**Avant :**
- `check_accounts_exist`
- `update_accounts_database`
- `check_accounts_in_vault`
- (mélange de styles)

**Après :**
- `validation` → `check_accounts_exist`
- `database_preparation` → `update_accounts_database`
- `vault_and_secrets` → `get_vault_secrets`, `check_accounts_in_vault`, ...
- (groupes cohérents)

### **3. Logs avec emojis**

```python
logger.info("🔍 Checking 5 accounts")      # Recherche
logger.info("✅ All accounts found")        # Succès
logger.error("❌ Account not found")        # Erreur
logger.warning("⚠️ Partial failure")       # Avertissement
logger.info("🚀 Starting deployment")       # Action
logger.info("📊 Summary")                   # Résumé
```

**Avantage :** Scan visuel rapide des logs !

---

## 📈 Impact sur les performances

### **Scénario : 10 comptes à déployer**

#### **Option A: Dynamic Mapping**
```
Temps total = Temps du compte le plus lent
(car parallélisme natif Airflow)

Exemple:
- 5 comptes en parallèle (max_active_tasks=5)
- Batch 1: ACC-001 à ACC-005 (durée: 10 min)
- Batch 2: ACC-006 à ACC-010 (durée: 10 min)
= Total: ~20 minutes
```

#### **Option B: ThreadPoolExecutor**
```
Temps total = Temps du compte le plus lent dans chaque batch
+ overhead ThreadPoolExecutor

Exemple:
- 5 workers (max_parallel_executions=5)
- Batch 1: ACC-001 à ACC-005 (durée: 10 min)
- Batch 2: ACC-006 à ACC-010 (durée: 10 min)
= Total: ~20 minutes + overhead (10-30s)
```

**Conclusion :** Performances similaires, mais **Option A a une bien meilleure observabilité**.

---

## 🔧 Résolution du problème des "lignes visuelles"

### **Problème identifié**

```python
updated_accounts = update_accounts_database()

# updated_accounts utilisé par 3 tâches
check_accounts_in_vault(updated_accounts)
get_buhub_account_details(updated_accounts)
get_workspaces_for_accounts(updated_accounts)
```

**Dans l'UI, ça crée :**
```
updated_accounts
    ├──────> check_accounts_in_vault
    ├──────> get_buhub_account_details
    └──────> get_workspaces_for_accounts
```

### **C'est normal et CORRECT !**

C'est un pattern **fan-out** (une tâche → plusieurs consommateurs).

**Pourquoi c'est bien ?**
- ✅ `check_accounts_in_vault` et `get_buhub_account_details` peuvent s'exécuter **en parallèle**
- ✅ Pas de dépendance inutile entre eux
- ✅ Optimise le temps d'exécution

### **Si tu veux vraiment réduire les lignes visuelles**

**Option 1 : Regrouper dans un dict**
```python
@step
def prepare_vault_data():
    secrets = get_vault_secrets()
    api_keys_ok = check_accounts_in_vault()
    buhub = get_buhub_account_details()
    schematics_key = get_schematics_api_key(buhub)
    
    return {
        "secrets": secrets,
        "api_keys_ok": api_keys_ok,
        "schematics_key": schematics_key,
    }

# Maintenant 1 seule ligne
vault_data = prepare_vault_data()
```

**Inconvénient :** Perd le parallélisme potentiel.

**Ma recommandation :** **Garde les lignes multiples** ! C'est la bonne pratique Airflow.

---

## ✅ Checklist de migration

- [ ] Vérifier version Airflow >= 2.3.0
- [ ] Choisir entre Option A (Dynamic) ou Option B (ThreadPool)
- [ ] Si Option A : Décommenter le code `expand()`
- [ ] Si Option A : Configurer `max_active_tasks_per_dag`
- [ ] Tester avec 2-3 comptes en mode `plan_only`
- [ ] Vérifier les logs et l'UI
- [ ] Tester un retry sélectif (Option A uniquement)
- [ ] Valider en production avec volume réel

---

## 🎓 Bonnes pratiques respectées

✅ **Séparation des responsabilités** : 1 TaskGroup = 1 fonction métier  
✅ **Single Responsibility Principle** : Chaque step fait UNE chose  
✅ **Observabilité** : Logs clairs avec emojis  
✅ **Maintenabilité** : Structure modulaire  
✅ **Testabilité** : Steps isolés testables  
✅ **Evolutivité** : Facile d'ajouter des steps  
✅ **Fan-out pattern** : Parallélisme optimisé  
✅ **Retry granulaire** : Par compte (Option A)  

---

## 📞 Support

Si tu rencontres des problèmes :

1. **Erreur `expand() not found`** → Version Airflow < 2.3 → Utilise Option B
2. **Trop de tâches dans l'UI** → Réduis `max_active_tasks` ou utilise Option B
3. **Logs difficiles à lire** → Active les filtres par `task_id` dans l'UI
4. **Performances** → Ajuste `max_active_tasks_per_dag` et `parallelism`

---

**Prêt à migrer ? Dis-moi quelle option tu préfères !** 🚀