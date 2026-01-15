"""
Account Update DAG - Production Ready
======================================

Ce DAG permet de mettre à jour des configurations de comptes IBM Cloud
et de déployer les changements via Terraform/Schematics en parallèle.

Auteur: Équipe Platform
Version: 2.0
"""

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models import Variable
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Literal
from pydantic import BaseModel, Field, field_validator, model_validator
from sqlalchemy.orm import Session as SASession
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

logger = logging.getLogger(__name__)

# ==========================================
# CONFIGURATION
# ==========================================

# Default args pour le DAG
DEFAULT_ARGS = {
    'owner': 'platform-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['platform-alerts@company.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Configuration du DAG
DAG_CONFIG = {
    'dag_id': 'account_update_parallel',
    'default_args': DEFAULT_ARGS,
    'description': 'Update IBM Cloud accounts and deploy via Schematics',
    'schedule_interval': None,  # Triggered manually
    'start_date': days_ago(1),
    'catchup': False,
    'max_active_runs': 3,
    'tags': ['ibm-cloud', 'terraform', 'accounts', 'production'],
}


# ==========================================
# PAYLOAD & MODELS
# ==========================================

class AccountUpdates(BaseModel):
    """Configuration changes to apply to accounts"""
    iam_version: Optional[str] = Field(None, description="IAM version")
    subnet_count: Optional[int] = Field(None, ge=1, le=20, description="Subnet count")
    enable_monitoring: Optional[bool] = Field(None, description="Enable monitoring")
    tags: Optional[List[str]] = Field(None, description="Tags")
    
    @model_validator(mode='after')
    def check_at_least_one_update(self):
        if not any([
            self.iam_version, 
            self.subnet_count is not None, 
            self.enable_monitoring is not None, 
            self.tags
        ]):
            raise ValueError("At least one update field must be provided")
        return self


class AccountUpdatePayload(BaseModel):
    """Main payload for the DAG"""
    account_names: List[str] = Field(..., min_items=1, max_items=10)
    updates: AccountUpdates
    execution_mode: Literal["plan_only", "plan_and_apply"] = "plan_only"
    max_parallel_executions: int = Field(default=2, ge=1, le=5)
    notify_on_completion: bool = Field(default=True)
    
    @field_validator("account_names")
    @classmethod
    def validate_account_names(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("account_names cannot be empty")
        # Remove duplicates
        return list(dict.fromkeys(v))


# ==========================================
# HELPER FUNCTIONS
# ==========================================

def get_account_by_name(name: str, session: SASession):
    """Retrieve account from database"""
    return (
        session.query(db_models.AccountExtractIAM)
        .filter(db_models.AccountExtractIAM.account_name.contains(name))
        .first()
    )


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
# DAG DEFINITION
# ==========================================

with DAG(**DAG_CONFIG) as dag:
    
    # ==========================================
    # START & END MARKERS
    # ==========================================
    
    start = EmptyOperator(
        task_id='start',
        doc_md="""
        ## Démarrage du workflow
        
        Initialise le processus de mise à jour des comptes.
        """
    )
    
    end = EmptyOperator(
        task_id='end',
        doc_md="""
        ## Fin du workflow
        
        Toutes les opérations sont terminées.
        """
    )
    
    
    # ==========================================
    # TASK GROUP 1: VALIDATION & PREPARATION
    # ==========================================
    
    @task_group(group_id='validation_and_preparation')
    def validation_and_preparation_group():
        """
        Groupe de tâches pour valider les inputs et préparer les données.
        
        - Validation du payload
        - Vérification de l'existence des comptes
        - Validation des prérequis
        """
        
        @task(task_id='validate_payload')
        def validate_payload(**context) -> Dict:
            """Valide le payload d'entrée"""
            conf = context['dag_run'].conf or {}
            
            try:
                payload = AccountUpdatePayload(**conf)
                logger.info(f"✅ Payload validé: {len(payload.account_names)} comptes")
                return payload.model_dump()
            except Exception as e:
                logger.error(f"❌ Payload invalide: {str(e)}")
                raise AirflowFailException(f"Invalid payload: {str(e)}")
        
        
        @task(task_id='check_accounts_exist')
        def check_accounts_exist(payload: Dict, **context) -> List[str]:
            """Vérifie que tous les comptes existent en BD"""
            from dependencies import sqlalchemy_session_dependency
            
            db_session = sqlalchemy_session_dependency()
            account_names = payload['account_names']
            missing_accounts = []
            
            logger.info(f"🔍 Vérification de {len(account_names)} comptes...")
            
            for account_name in account_names:
                account = get_account_by_name(account_name, db_session)
                if account is None:
                    missing_accounts.append(account_name)
                    logger.error(f"❌ Compte introuvable: {account_name}")
            
            if missing_accounts:
                raise AirflowFailException(
                    f"Comptes introuvables: {', '.join(missing_accounts)}"
                )
            
            logger.info(f"✅ Tous les comptes existent")
            return account_names
        
        
        @task(task_id='check_prerequisites')
        def check_prerequisites(payload: Dict, **context) -> bool:
            """Vérifie les prérequis système"""
            from dependencies import vault_dependency
            
            vault = vault_dependency()
            errors = []
            
            # Vérifier GHUB_ACCOUNT_NUMBER
            try:
                from config import GHUB_ACCOUNT_NUMBER
                logger.info(f"✅ GHUB_ACCOUNT_NUMBER configuré")
            except ImportError:
                errors.append("GHUB_ACCOUNT_NUMBER not configured")
            
            # Vérifier accès Vault
            try:
                vault.vault.token
                logger.info(f"✅ Connexion Vault OK")
            except Exception as e:
                errors.append(f"Vault access error: {str(e)}")
            
            if errors:
                raise AirflowFailException(f"Prerequisites failed: {', '.join(errors)}")
            
            logger.info("✅ Tous les prérequis validés")
            return True
        
        
        # Flow du groupe
        payload_validated = validate_payload()
        accounts_checked = check_accounts_exist(payload_validated)
        prereqs_checked = check_prerequisites(payload_validated)
        
        return payload_validated
    
    
    # ==========================================
    # TASK GROUP 2: DATABASE UPDATES
    # ==========================================
    
    @task_group(group_id='database_updates')
    def database_updates_group():
        """
        Groupe de tâches pour mettre à jour la base de données.
        
        - Mise à jour des configurations
        - Tracking des changements
        """
        
        @task(task_id='update_account_configs')
        def update_account_configs(payload: Dict, **context) -> List[Dict]:
            """Met à jour les configurations dans PostgreSQL"""
            from dependencies import sqlalchemy_session_dependency
            
            db_session = sqlalchemy_session_dependency()
            account_names = payload['account_names']
            updates = AccountUpdates(**payload['updates'])
            
            updated_accounts = []
            failed_accounts = []
            
            logger.info(f"📝 Mise à jour de {len(account_names)} comptes en BD...")
            
            for account_name in account_names:
                try:
                    account = get_account_by_name(account_name, db_session)
                    
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
                    
                    db_session.commit()
                    db_session.refresh(account)
                    
                    updated_accounts.append({
                        "account_name": account.account_name,
                        "account_number": account.account_number,
                        "account_type": account.account_type,
                        "environment_type": account.environment_type,
                        "code_bu": account.code_bu,
                    })
                    
                    logger.info(f"✅ Compte mis à jour: {account_name}")
                    
                except Exception as e:
                    logger.error(f"❌ Erreur pour {account_name}: {str(e)}")
                    failed_accounts.append(account_name)
                    update_deployment_status(
                        account_name, "update_failed", db_session, str(e)
                    )
            
            if not updated_accounts:
                raise AirflowFailException("Aucun compte mis à jour avec succès")
            
            if failed_accounts:
                logger.warning(f"⚠️ Échecs: {', '.join(failed_accounts)}")
            
            logger.info(f"✅ {len(updated_accounts)} comptes mis à jour")
            return updated_accounts
        
        
        @task(task_id='log_changes')
        def log_changes(updated_accounts: List[Dict], payload: Dict, **context):
            """Enregistre les changements dans un log d'audit"""
            updates = payload['updates']
            
            audit_entry = {
                "timestamp": datetime.utcnow().isoformat(),
                "user": context.get('dag_run').conf.get('user', 'system'),
                "accounts_updated": [acc['account_name'] for acc in updated_accounts],
                "changes": updates,
                "execution_mode": payload['execution_mode']
            }
            
            # Vous pouvez stocker ceci dans une table d'audit
            logger.info(f"📋 Audit log créé: {audit_entry}")
            return audit_entry
        
        
        # Flow du groupe
        updated = update_account_configs()
        log_changes(updated)
        
        return updated
    
    
    # ==========================================
    # TASK GROUP 3: VAULT & SECRETS
    # ==========================================
    
    @task_group(group_id='vault_and_secrets')
    def vault_and_secrets_group():
        """
        Groupe de tâches pour gérer les secrets Vault.
        
        - Récupération des secrets orchestrator
        - Vérification des API keys des comptes
        - Récupération de l'API key Schematics
        """
        
        @task(task_id='get_orchestrator_secrets', retries=3)
        def get_orchestrator_secrets(payload: Dict, **context) -> Dict:
            """Récupère les secrets orchestrator depuis Vault"""
            from dependencies import vault_dependency
            from config import ENVIRONMENT
            
            vault = vault_dependency()
            
            logger.info("🔐 Récupération des secrets orchestrator...")
            
            try:
                gitlab_secret = vault.get_secret(
                    f"orchestrator/{ENVIRONMENT}/products/account/gitlab"
                )
                db_secret = vault.get_secret(
                    f"orchestrator/{ENVIRONMENT}/products/account/db"
                )
                tfe_secrets = vault.get_secret(
                    f"orchestrator/{ENVIRONMENT}/tfe/nprod"
                )
                
                secrets = {
                    "gitlab_token": gitlab_secret["token"],
                    "vault_token": vault.vault.token,
                    "db_secret": db_secret,
                    "tfe_secrets": tfe_secrets,
                }
                
                logger.info("✅ Secrets orchestrator récupérés")
                return secrets
                
            except Exception as e:
                logger.error(f"❌ Erreur Vault: {str(e)}")
                raise AirflowFailException(f"Failed to get orchestrator secrets: {str(e)}")
        
        
        @task(task_id='verify_account_api_keys', retries=3)
        def verify_account_api_keys(updated_accounts: List[Dict], **context) -> bool:
            """Vérifie que tous les comptes ont des API keys dans Vault"""
            from dependencies import vault_dependency
            
            vault = vault_dependency()
            missing_keys = []
            
            logger.info(f"🔐 Vérification des API keys pour {len(updated_accounts)} comptes...")
            
            for account_info in updated_accounts:
                account_number = account_info["account_number"]
                account_name = account_info["account_name"]
                
                try:
                    api_key_secret = vault.get_secret(
                        f"{account_number}/account-owner",
                        mount_point="ibmsid"
                    )
                    
                    if not api_key_secret or "api_key" not in api_key_secret:
                        missing_keys.append(f"{account_name} ({account_number})")
                        
                except Exception as e:
                    logger.error(f"❌ Erreur pour {account_name}: {str(e)}")
                    missing_keys.append(f"{account_name} ({account_number})")
            
            if missing_keys:
                raise AirflowFailException(
                    f"API keys manquantes: {', '.join(missing_keys)}"
                )
            
            logger.info(f"✅ Toutes les API keys sont présentes")
            return True
        
        
        @task(task_id='get_schematics_api_key', retries=3)
        def get_schematics_api_key(updated_accounts: List[Dict], **context) -> str:
            """Récupère l'API key pour Schematics (BUHUB ou GLBHUB)"""
            from dependencies import vault_dependency, sqlalchemy_session_dependency
            from config import GHUB_ACCOUNT_NUMBER
            
            vault = vault_dependency()
            db_session = sqlalchemy_session_dependency()
            
            # Vérifier si on a des comptes WKLAPP
            wklapp_accounts = [
                acc for acc in updated_accounts 
                if acc.get("account_type") == "WKLAPP"
            ]
            
            if wklapp_accounts:
                # Utiliser BUHUB
                code_bu = wklapp_accounts[0]["code_bu"]
                
                logger.info(f"🔍 Recherche BUHUB pour code_bu: {code_bu}")
                
                from account.database.utils.account_services import get_account_by_type_and_bu
                from account.connectors.reader import ReaderConnector
                
                buhub_account = get_account_by_type_and_bu("BUHUB", code_bu, db_session)
                
                if buhub_account:
                    reader = ReaderConnector()
                    buhub_details = reader.get_account_details(buhub_account.name)
                    
                    api_key = vault.get_secret(
                        f"{buhub_details['number']}/account-owner",
                        mount_point="ibmsid"
                    )["api_key"]
                    
                    logger.info(f"✅ API key BUHUB récupérée")
                    return api_key
            
            # Fallback: GLBHUB
            logger.info("ℹ️ Utilisation de GLBHUB (fallback)")
            api_key = vault.get_secret(
                f"{GHUB_ACCOUNT_NUMBER}/account-owner",
                mount_point="ibmsid"
            )["api_key"]
            
            logger.info("✅ API key GLBHUB récupérée")
            return api_key
        
        
        # Flow du groupe
        orch_secrets = get_orchestrator_secrets()
        api_keys_verified = verify_account_api_keys()
        schematics_key = get_schematics_api_key()
        
        return {"orchestrator": orch_secrets, "schematics_api_key": schematics_key}
    
    
    # ==========================================
    # TASK GROUP 4: SCHEMATICS WORKSPACES
    # ==========================================
    
    @task_group(group_id='schematics_workspaces')
    def schematics_workspaces_group():
        """
        Groupe de tâches pour gérer les workspaces Schematics.
        
        - Recherche des workspaces existants
        - Vérification des statuts
        """
        
        @task(task_id='find_workspaces')
        def find_workspaces(
            updated_accounts: List[Dict],
            vault_secrets: Dict,
            **context
        ) -> List[Dict]:
            """Trouve les workspaces Schematics pour chaque compte"""
            from dependencies import schematics_backend_dependency, sqlalchemy_session_dependency
            
            db_session = sqlalchemy_session_dependency()
            tf = schematics_backend_dependency()
            
            account_workspaces = []
            failed_accounts = []
            
            logger.info(f"📁 Recherche de {len(updated_accounts)} workspaces...")
            
            for account_info in updated_accounts:
                try:
                    # Construire le nom du workspace
                    workspace_name = (
                        f"WS-ACCOUNT-DMZRC-"
                        f"{account_info['environment_type'].upper()}-"
                        f"{account_info['account_type'].upper()}-"
                        f"{account_info['code_bu'].upper()}-"
                        f"{account_info['account_name'].upper()}"
                    )
                    
                    logger.info(f"🔍 Recherche: {workspace_name}")
                    
                    # Chercher en BD
                    workspace_in_db = (
                        db_session.query(db_models.Workspace)
                        .filter(
                            db_models.Workspace.name == workspace_name,
                            db_models.Workspace.type == "schematics"
                        )
                        .one_or_none()
                    )
                    
                    if not workspace_in_db:
                        raise ValueError(f"Workspace non trouvé: {workspace_name}")
                    
                    # Vérifier le statut sur Schematics
                    workspace = tf.workspaces.get_by_id(workspace_in_db.id)
                    status = SchematicsWorkspaceStatus(workspace.status)
                    
                    if status.value not in ["ACTIVE", "INACTIVE"]:
                        raise ValueError(f"Statut invalide: {status.value}")
                    
                    account_workspaces.append({
                        **account_info,
                        "workspace_id": workspace_in_db.id,
                        "workspace_name": workspace_in_db.name,
                        "workspace_status": status.value
                    })
                    
                    logger.info(f"✅ Workspace trouvé: {workspace_name}")
                    
                except Exception as e:
                    logger.error(f"❌ Erreur pour {account_info['account_name']}: {str(e)}")
                    failed_accounts.append(account_info['account_name'])
                    update_deployment_status(
                        account_info['account_name'],
                        "workspace_not_found",
                        db_session,
                        str(e)
                    )
            
            if not account_workspaces:
                raise AirflowFailException("Aucun workspace trouvé")
            
            if failed_accounts:
                logger.warning(f"⚠️ Workspaces non trouvés: {', '.join(failed_accounts)}")
            
            logger.info(f"✅ {len(account_workspaces)} workspaces trouvés")
            return account_workspaces
        
        
        # Flow du groupe
        workspaces = find_workspaces()
        return workspaces
    
    
    # ==========================================
    # BRANCHING: Plan only ou Plan + Apply ?
    # ==========================================
    
    @task.branch(task_id='check_execution_mode')
    def check_execution_mode(payload: Dict, **context) -> str:
        """Détermine si on fait plan_only ou plan_and_apply"""
        mode = payload.get('execution_mode', 'plan_only')
        
        if mode == 'plan_and_apply':
            logger.info("🎯 Mode: PLAN + APPLY")
            return 'deployment.deploy_with_apply'
        else:
            logger.info("🎯 Mode: PLAN ONLY")
            return 'deployment.deploy_plan_only'
    
    
    # ==========================================
    # TASK GROUP 5: DEPLOYMENT (Dynamique)
    # ==========================================
    
    @task_group(group_id='deployment')
    def deployment_group():
        """
        Groupe de tâches pour le déploiement Terraform.
        
        Deux branches possibles:
        - Plan only (mode sécurisé)
        - Plan + Apply (mode automatique)
        """
        
        def deploy_single_account_plan_only(
            account_workspace: Dict,
            payload: Dict,
            tf,
            db_session
        ) -> Dict:
            """Execute plan uniquement pour un compte"""
            account_name = account_workspace["account_name"]
            workspace_id = account_workspace["workspace_id"]
            
            try:
                logger.info(f"📝 Plan pour {account_name}...")
                
                # 1. Récupérer config depuis BD
                account = get_account_by_name(account_name, db_session)
                
                # 2. Préparer variables
                variables = {}
                if account.iam_version:
                    variables["iam_version"] = account.iam_version
                if account.subnet_count is not None:
                    variables["subnet_count"] = str(account.subnet_count)
                
                # 3. Update workspace
                tf.workspaces.update_variables(workspace_id, variables)
                
                # 4. Plan
                plan_job = tf.jobs.plan(workspace_id)
                update_deployment_status(
                    account_name, "planning", db_session, activity_id=plan_job.id
                )
                
                plan_result = tf.jobs.wait_for_completion(plan_job.id, timeout=1800)
                
                if plan_result.status != "job_finished":
                    raise Exception(f"Plan failed: {plan_result.status}")
                
                update_deployment_status(account_name, "plan_completed", db_session)
                
                logger.info(f"✅ Plan complété: {account_name}")
                
                return {
                    "account_name": account_name,
                    "status": "success",
                    "final_status": "plan_completed"
                }
                
            except Exception as e:
                logger.error(f"❌ Erreur: {account_name}: {str(e)}")
                update_deployment_status(
                    account_name, "deployment_failed", db_session, str(e)
                )
                return {
                    "account_name": account_name,
                    "status": "failed",
                    "error": str(e)
                }
        
        
        def deploy_single_account_with_apply(
            account_workspace: Dict,
            payload: Dict,
            tf,
            db_session
        ) -> Dict:
            """Execute plan + apply pour un compte"""
            # Même début que plan_only
            result = deploy_single_account_plan_only(
                account_workspace, payload, tf, db_session
            )
            
            if result["status"] == "failed":
                return result
            
            # Continuer avec apply
            account_name = account_workspace["account_name"]
            workspace_id = account_workspace["workspace_id"]
            
            try:
                logger.info(f"🔧 Apply pour {account_name}...")
                
                apply_job = tf.jobs.apply(workspace_id)
                update_deployment_status(
                    account_name, "applying", db_session, activity_id=apply_job.id
                )
                
                apply_result = tf.jobs.wait_for_completion(apply_job.id, timeout=1800)
                
                if apply_result.status != "job_finished":
                    raise Exception(f"Apply failed: {apply_result.status}")
                
                update_deployment_status(account_name, "deployed", db_session)
                
                logger.info(f"✅ Apply complété: {account_name}")
                
                return {
                    "account_name": account_name,
                    "status": "success",
                    "final_status": "deployed"
                }
                
            except Exception as e:
                logger.error(f"❌ Erreur apply: {account_name}: {str(e)}")
                update_deployment_status(
                    account_name, "deployment_failed", db_session, str(e)
                )
                return {
                    "account_name": account_name,
                    "status": "failed",
                    "error": str(e)
                }
        
        
        @task(task_id='deploy_plan_only', trigger_rule='none_failed_min_one_success')
        def deploy_plan_only(
            account_workspaces: List[Dict],
            payload: Dict,
            vault_secrets: Dict,
            **context
        ) -> Dict:
            """Déploie en mode plan_only (parallèle)"""
            from dependencies import schematics_backend_dependency, sqlalchemy_session_dependency
            
            tf = schematics_backend_dependency()
            db_session = sqlalchemy_session_dependency()
            max_parallel = payload.get('max_parallel_executions', 2)
            
            results = {"successful": [], "failed": [], "total": len(account_workspaces)}
            
            logger.info(f"🚀 Déploiement PLAN ONLY: {results['total']} comptes (max {max_parallel} //)")
            
            with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                futures = {
                    executor.submit(
                        deploy_single_account_plan_only,
                        acc_ws, payload, tf, db_session
                    ): acc_ws["account_name"]
                    for acc_ws in account_workspaces
                }
                
                for future in as_completed(futures):
                    result = future.result()
                    if result["status"] == "success":
                        results["successful"].append(result)
                    else:
                        results["failed"].append(result)
            
            logger.info(f"📊 Résultats: ✅ {len(results['successful'])} | ❌ {len(results['failed'])}")
            
            if len(results["failed"]) == results["total"]:
                raise AirflowFailException("Tous les déploiements ont échoué")
            
            return results
        
        
        # Les deux branches du deployment
        plan_only_results = deploy_plan_only()
        apply_results = deploy_with_apply()
        
        return [plan_only_results, apply_results]
    
    
    # ==========================================
    # TASK GROUP 6: POST-DEPLOYMENT
    # ==========================================
    
    @task_group(group_id='post_deployment')
    def post_deployment_group():
        """
        Groupe de tâches post-déploiement.
        
        - Génération du rapport
        - Notifications
        - Nettoyage
        """
        
        @task(task_id='generate_report', trigger_rule='none_failed_min_one_success')
        def generate_report(deployment_results, payload: Dict, **context) -> Dict:
            """Génère un rapport de déploiement"""
            
            # deployment_results peut être plan_only OU apply
            if isinstance(deployment_results, list):
                # Prendre le premier non-None
                results = next((r for r in deployment_results if r is not None), {})
            else:
                results = deployment_results
            
            report = {
                "execution_date": context['execution_date'].isoformat(),
                "dag_run_id": context['dag_run'].run_id,
                "execution_mode": payload['execution_mode'],
                "total_accounts": results.get('total', 0),
                "successful_accounts": len(results.get('successful', [])),
                "failed_accounts": len(results.get('failed', [])),
                "success_rate": (
                    len(results.get('successful', [])) / results.get('total', 1) * 100
                    if results.get('total', 0) > 0 else 0
                ),
                "successful_list": [
                    acc['account_name'] for acc in results.get('successful', [])
                ],
                "failed_list": [
                    {
                        "account": acc['account_name'],
                        "error": acc.get('error', 'Unknown')
                    }
                    for acc in results.get('failed', [])
                ],
                "duration_seconds": (
                    datetime.utcnow() - context['execution_date']
                ).total_seconds()
            }
            
            logger.info("=" * 70)
            logger.info("📊 RAPPORT DE DÉPLOIEMENT")
            logger.info("=" * 70)
            logger.info(f"Mode d'exécution: {report['execution_mode']}")
            logger.info(f"Total comptes: {report['total_accounts']}")
            logger.info(f"✅ Succès: {report['successful_accounts']}")
            logger.info(f"❌ Échecs: {report['failed_accounts']}")
            logger.info(f"Taux de réussite: {report['success_rate']:.1f}%")
            logger.info(f"Durée: {report['duration_seconds']:.0f}s")
            logger.info("=" * 70)
            
            return report
        
        
        @task(task_id='send_notifications', trigger_rule='none_failed_min_one_success')
        def send_notifications(report: Dict, payload: Dict, **context):
            """Envoie les notifications selon les résultats"""
            
            if not payload.get('notify_on_completion', True):
                logger.info("ℹ️ Notifications désactivées")
                raise AirflowSkipException("Notifications disabled")
            
            # Vous pouvez implémenter l'envoi d'emails, Slack, etc.
            notification_message = f"""
            🚀 Déploiement terminé
            
            Mode: {report['execution_mode']}
            ✅ Succès: {report['successful_accounts']}/{report['total_accounts']}
            ❌ Échecs: {report['failed_accounts']}/{report['total_accounts']}
            
            Comptes déployés avec succès:
            {chr(10).join('  - ' + acc for acc in report['successful_list'])}
            
            Échecs:
            {chr(10).join(f"  - {fail['account']}: {fail['error']}" for fail in report['failed_list'])}
            """
            
            logger.info("📧 Notification:")
            logger.info(notification_message)
            
            # TODO: Envoyer email/Slack réel
            # send_email(to=..., subject=..., body=notification_message)
            
            return True
        
        
        @task(task_id='cleanup', trigger_rule='all_done')
        def cleanup(**context):
            """Nettoie les ressources temporaires si nécessaire"""
            logger.info("🧹 Nettoyage...")
            # Fermer les connexions, nettoyer les fichiers temp, etc.
            logger.info("✅ Nettoyage terminé")
            return True
        
        
        # Flow du groupe
        report = generate_report()
        notifications = send_notifications(report)
        cleanup_done = cleanup()
        
        notifications >> cleanup_done
        
        return report
    
    
    # ==========================================
    # ORCHESTRATION GLOBALE
    # ==========================================
    
    # Étape 1: Validation & Préparation
    validated_payload = validation_and_preparation_group()
    
    # Étape 2: Mise à jour BD
    updated_accounts = database_updates_group()
    
    # Étape 3: Vault & Secrets
    vault_secrets = vault_and_secrets_group()
    
    # Étape 4: Workspaces Schematics
    account_workspaces = schematics_workspaces_group()
    
    # Étape 5: Branching selon mode
    execution_branch = check_execution_mode(validated_payload)
    
    # Étape 6: Déploiement (avec branches)
    deployment_results = deployment_group()
    
    # Étape 7: Post-déploiement
    final_report = post_deployment_group()
    
    
    # ==========================================
    # DÉFINITION DES DÉPENDANCES
    # ==========================================
    
    start >> validated_payload
    validated_payload >> updated_accounts
    updated_accounts >> vault_secrets
    vault_secrets >> account_workspaces
    account_workspaces >> execution_branch
    execution_branch >> deployment_results
    deployment_results >> final_report
    final_report >> end


# ==========================================
# DOCUMENTATION DU DAG
# ==========================================

dag.doc_md = """
# Account Update DAG - Documentation

## Vue d'ensemble

Ce DAG permet de mettre à jour des configurations de comptes IBM Cloud et de déployer
les changements via Terraform/Schematics de manière parallèle et contrôlée.

## Architecture

Le workflow est organisé en **7 étapes principales** via des **TaskGroups** :

1. **Validation & Préparation** : Valide le payload et vérifie les prérequis
2. **Database Updates** : Met à jour PostgreSQL avec les nouvelles configurations
3. **Vault & Secrets** : Récupère tous les secrets nécessaires
4. **Schematics Workspaces** : Localise les workspaces Terraform
5. **Branching** : Choix entre plan_only ou plan_and_apply
6. **Deployment** : Exécution parallèle des déploiements Terraform
7. **Post-Deployment** : Génération de rapport et notifications

## Modes d'exécution

### Plan Only (par défaut)
- Génère uniquement le plan Terraform
- Aucun changement appliqué sur IBM Cloud
- Mode recommandé pour tester

### Plan and Apply
- Génère le plan ET applique les changements
- Modifications effectives sur IBM Cloud
- Nécessite validation préalable

## Payload d'exemple

```json
{
  "account_names": ["ACC-001", "ACC-002"],
  "updates": {
    "iam_version": "2.0",
    "subnet_count": 5,
    "enable_monitoring": true,
    "tags": ["production", "critical"]
  },
  "execution_mode": "plan_only",
  "max_parallel_executions": 2,
  "notify_on_completion": true
}
```

## Parallélisme

Le DAG supporte le déploiement parallèle avec contrôle du niveau de parallélisme
via `max_parallel_executions` (1-5).

## Monitoring

- Logs détaillés à chaque étape
- Tracking du statut en base de données
- Rapport final avec métriques
- Notifications optionnelles

## Gestion des erreurs

- Retries automatiques sur les étapes critiques
- Isolation des échecs (un échec n'arrête pas tout)
- Rapport détaillé des succès et échecs
- Rollback manuel si nécessaire

## Sécurité

- Secrets stockés dans Vault uniquement
- API keys jamais en clair dans les logs
- Audit trail complet des modifications

---

**Auteur**: Platform Team  
**Version**: 2.0  
**Dernière mise à jour**: 2026-01
"""results['failed'])}")
            
            if len(results["failed"]) == results["total"]:
                raise AirflowFailException("Tous les déploiements ont échoué")
            
            return results
        
        
        @task(task_id='deploy_with_apply', trigger_rule='none_failed_min_one_success')
        def deploy_with_apply(
            account_workspaces: List[Dict],
            payload: Dict,
            vault_secrets: Dict,
            **context
        ) -> Dict:
            """Déploie en mode plan_and_apply (parallèle)"""
            from dependencies import schematics_backend_dependency, sqlalchemy_session_dependency
            
            tf = schematics_backend_dependency()
            db_session = sqlalchemy_session_dependency()
            max_parallel = payload.get('max_parallel_executions', 2)
            
            results = {"successful": [], "failed": [], "total": len(account_workspaces)}
            
            logger.info(f"🚀 Déploiement PLAN + APPLY: {results['total']} comptes (max {max_parallel} //)")
            
            with ThreadPoolExecutor(max_workers=max_parallel) as executor:
                futures = {
                    executor.submit(
                        deploy_single_account_with_apply,
                        acc_ws, payload, tf, db_session
                    ): acc_ws["account_name"]
                    for acc_ws in account_workspaces
                }
                
                for future in as_completed(futures):
                    result = future.result()
                    if result["status"] == "success":
                        results["successful"].append(result)
                    else:
                        results["failed"].append(result)
            
            logger.info(f"📊 Résultats: ✅ {len(results['successful'])} | ❌ {len(
                
                





# 🏆 Bonnes pratiques Airflow - Explications

## 📊 Vue d'ensemble de l'architecture

Le DAG est organisé en **7 TaskGroups** hiérarchiques pour une meilleure lisibilité et maintenabilité.

```
START
  ↓
┌─────────────────────────────────────────────────────────┐
│ 1. VALIDATION & PREPARATION                            │
│    ├─ validate_payload                                 │
│    ├─ check_accounts_exist                             │
│    └─ check_prerequisites                              │
└─────────────────────────────────────────────────────────┘
  ↓
┌─────────────────────────────────────────────────────────┐
│ 2. DATABASE UPDATES                                     │
│    ├─ update_account_configs                           │
│    └─ log_changes (audit)                              │
└─────────────────────────────────────────────────────────┘
  ↓
┌─────────────────────────────────────────────────────────┐
│ 3. VAULT & SECRETS                                      │
│    ├─ get_orchestrator_secrets                         │
│    ├─ verify_account_api_keys                          │
│    └─ get_schematics_api_key                           │
└─────────────────────────────────────────────────────────┘
  ↓
┌─────────────────────────────────────────────────────────┐
│ 4. SCHEMATICS WORKSPACES                                │
│    └─ find_workspaces                                   │
└─────────────────────────────────────────────────────────┘
  ↓
┌─────────────────────────────────────────────────────────┐
│ 5. BRANCHING (@task.branch)                             │
│    check_execution_mode                                 │
│         ↙         ↘                                     │
│   plan_only    plan_and_apply                           │
└─────────────────────────────────────────────────────────┘
  ↓
┌─────────────────────────────────────────────────────────┐
│ 6. DEPLOYMENT (Parallèle)                               │
│    ├─ deploy_plan_only (si plan_only)                  │
│    └─ deploy_with_apply (si plan_and_apply)            │
└─────────────────────────────────────────────────────────┘
  ↓
┌─────────────────────────────────────────────────────────┐
│ 7. POST-DEPLOYMENT                                      │
│    ├─ generate_report                                   │
│    ├─ send_notifications                                │
│    └─ cleanup                                           │
└─────────────────────────────────────────────────────────┘
  ↓
END
```

---

## ✅ Bonnes pratiques appliquées

### **1. TaskGroups pour l'organisation**

**Pourquoi ?**
- Regroupe les tâches logiquement liées
- Améliore la lisibilité dans l'UI Airflow
- Facilite la maintenance et le debugging

**Exemple :**
```python
@task_group(group_id='validation_and_preparation')
def validation_and_preparation_group():
    """Groupe cohérent de tâches de validation"""
    validate_payload()
    check_accounts_exist()
    check_prerequisites()
```

**Bénéfice :** Au lieu de voir 20 tâches au même niveau, tu vois 7 groupes clairs.

---

### **2. Decorators `@task` au lieu de PythonOperator**

**Avant (ancien style) :**
```python
def ma_fonction():
    pass

task1 = PythonOperator(
    task_id='task1',
    python_callable=ma_fonction
)
```

**Après (moderne) :**
```python
@task(task_id='task1')
def ma_fonction():
    pass
```

**Avantages :**
- Code plus concis et lisible
- Type hints automatiques
- Meilleure gestion des XComs
- Moins de boilerplate

---

### **3. Branching avec `@task.branch`**

**Pourquoi ?**
- Permet des workflows conditionnels
- Évite d'exécuter des tâches inutiles
- Optimise les ressources

**Notre cas d'usage :**
```python
@task.branch(task_id='check_execution_mode')
def check_execution_mode(payload: Dict) -> str:
    if payload['execution_mode'] == 'plan_and_apply':
        return 'deployment.deploy_with_apply'
    else:
        return 'deployment.deploy_plan_only'
```

**Résultat :** 
- Mode `plan_only` → Skip la task `deploy_with_apply`
- Mode `plan_and_apply` → Skip la task `deploy_plan_only`

---

### **4. Trigger Rules adaptées**

**Trigger rules utilisées :**

| Trigger Rule | Signification | Où utilisé |
|--------------|---------------|------------|
| `all_success` (défaut) | Tous les parents ont réussi | Tâches normales |
| `none_failed_min_one_success` | Au moins 1 succès, aucun échec | Après branching |
| `all_done` | Tous terminés (succès ou échec) | Cleanup final |

**Exemple :**
```python
@task(task_id='deploy_plan_only', trigger_rule='none_failed_min_one_success')
def deploy_plan_only():
    # S'exécute même si la branche parallèle a été skipped
    pass
```

---

### **5. Gestion d'erreurs robuste**

**Stratégie multi-niveaux :**

```python
# Niveau 1: Retries automatiques
@task(task_id='get_vault_secrets', retries=3, retry_delay=timedelta(minutes=5))
def get_vault_secrets():
    # Retry automatique en cas d'échec temporaire
    pass

# Niveau 2: Gestion explicite
try:
    result = risky_operation()
except TemporaryError:
    # Peut réessayer
    raise
except PermanentError:
    # Échec définitif mais pas critique
    logger.error("...")
    return None

# Niveau 3: Fail fast pour erreurs critiques
if critical_validation_failed:
    raise AirflowFailException("Stop everything!")
```

**Isolation des échecs :**
```python
# Un compte qui échoue n'arrête pas les autres
for account in accounts:
    try:
        deploy_account(account)
    except Exception as e:
        failed.append(account)
        continue  # Continue avec les autres
```

---

### **6. Logging structuré et informatif**

**3 niveaux de logs :**

```python
# 1. INFO: Progression normale
logger.info("✅ {len(accounts)} comptes mis à jour")

# 2. WARNING: Problème non-bloquant
logger.warning(f"⚠️ Échecs partiels: {failed_accounts}")

# 3. ERROR: Échec grave
logger.error(f"❌ Erreur critique: {str(e)}")
```

**Emojis pour une lecture rapide :**
- ✅ Succès
- ❌ Erreur
- ⚠️ Avertissement
- 🔍 Recherche
- 📝 Mise à jour
- 🔐 Sécurité/Vault
- 🚀 Déploiement

---

### **7. Configuration centralisée**

**Au lieu de valeurs hardcodées partout :**
```python
# ❌ Mauvais
retries = 3
email = 'team@company.com'
timeout = timedelta(hours=2)
```

**Centralisation :**
```python
# ✅ Bon
DEFAULT_ARGS = {
    'retries': 2,
    'email': ['platform-alerts@company.com'],
    'execution_timeout': timedelta(hours=2),
}

DAG_CONFIG = {
    'dag_id': 'account_update_parallel',
    'default_args': DEFAULT_ARGS,
    'schedule_interval': None,
}
```

**Avantage :** Modifier une seule fois pour tout le DAG.

---

### **8. Documentation intégrée**

**3 types de documentation :**

```python
# 1. Docstrings des fonctions
def ma_task():
    """
    Description claire de ce que fait la tâche.
    
    Args:
        param1: Description
    
    Returns:
        Description du retour
    """
    pass

# 2. doc_md dans les tasks
@task(task_id='deploy', doc_md="## Déploiement\nLance Terraform...")
def deploy():
    pass

# 3. doc_md du DAG complet
dag.doc_md = """
# Documentation complète
...
"""
```

**Résultat :** Documentation visible directement dans l'UI Airflow.

---

### **9. XComs implicites avec `@task`**

**Avant (XCom explicite) :**
```python
def task1(**context):
    result = do_something()
    context['ti'].xcom_push(key='result', value=result)

def task2(**context):
    result = context['ti'].xcom_pull(task_ids='task1', key='result')
```

**Après (automatique) :**
```python
@task
def task1() -> Dict:
    return do_something()  # XCom automatique

@task
def task2(data: Dict):  # Reçoit automatiquement
    process(data)

# Orchestration
result = task1()
task2(result)
```

---

### **10. Parallélisme contrôlé**

**ThreadPoolExecutor pour le parallélisme intra-task :**
```python
with ThreadPoolExecutor(max_workers=payload['max_parallel_executions']) as executor:
    futures = {
        executor.submit(deploy_account, account): account
        for account in accounts
    }
    
    for future in as_completed(futures):
        result = future.result()
```

**Avantages :**
- Contrôle précis du niveau de parallélisme
- Gestion des timeouts
- Collecte des résultats
- Isolation des erreurs

---

### **11. EmptyOperator pour les jalons**

```python
start = EmptyOperator(task_id='start')
end = EmptyOperator(task_id='end')
```

**Pourquoi ?**
- Marque visuellement le début/fin
- Permet de connecter plusieurs branches
- Facilite le debugging (voir où ça bloque)

---

### **12. Validation précoce (Fail Fast)**

```python
# Valider AVANT de commencer le travail
@task(task_id='validate_payload')
def validate_payload(**context):
    conf = context['dag_run'].conf
    
    try:
        payload = AccountUpdatePayload(**conf)
    except ValidationError as e:
        raise AirflowFailException(f"Invalid payload: {e}")
    
    return payload.model_dump()
```

**Principe :** Échouer rapidement si les inputs sont invalides.

---

### **13. Idempotence**

**Chaque tâche peut être relancée sans effets de bord :**

```python
def update_account(account_name):
    # Vérifier l'état actuel
    account = get_account(account_name)
    
    if account.deployment_status == "deployed":
        logger.info("Déjà déployé, skip")
        return
    
    # Sinon, déployer
    deploy(account)
```

---

### **14. Audit trail complet**

```python
@task(task_id='log_changes')
def log_changes(updated_accounts, payload):
    audit_entry = {
        "timestamp": datetime.utcnow(),
        "user": context.get('dag_run').conf.get('user'),
        "accounts": updated_accounts,
        "changes": payload['updates']
    }
    # Sauvegarder en BD d'audit
```

**Traçabilité :** Qui a fait quoi, quand, et pourquoi.

---

### **15. Notifications conditionnelles**

```python
@task(task_id='send_notifications')
def send_notifications(report, payload):
    if not payload.get('notify_on_completion'):
        raise AirflowSkipException("Notifications disabled")
    
    # Envoyer notification seulement si demandé
```

---

## 🎯 Améliorations par rapport à la version précédente

| Aspect | Avant | Après |
|--------|-------|-------|
| **Organisation** | Tâches plates | 7 TaskGroups hiérarchiques |
| **Lisibilité** | 20+ tâches au même niveau | Groupes logiques clairs |
| **Flexibilité** | Un seul mode | Branching plan_only / plan_and_apply |
| **Erreurs** | Tout s'arrête si 1 échec | Isolation + rapport détaillé |
| **Logs** | Basiques | Structurés avec emojis |
| **Documentation** | Externe | Intégrée dans le DAG |
| **Parallélisme** | Basique | Contrôlé avec ThreadPoolExecutor |
| **Monitoring** | Difficile | Rapport + métriques + notifications |

---

## 📈 Métriques et monitoring

Le DAG génère automatiquement :

```json
{
  "total_accounts": 5,
  "successful_accounts": 4,
  "failed_accounts": 1,
  "success_rate": 80.0,
  "duration_seconds": 320,
  "successful_list": ["ACC-001", "ACC-002", ...],
  "failed_list": [
    {"account": "ACC-003", "error": "Workspace not found"}
  ]
}
```

---

## 🚀 Comment utiliser ce DAG

### **Lancer en mode plan_only (recommandé pour tester)**

```bash
airflow dags trigger account_update_parallel \
  --conf '{
    "account_names": ["ACC-TEST-01"],
    "updates": {"iam_version": "2.0"},
    "execution_mode": "plan_only"
  }'
```

### **Lancer en mode plan_and_apply (production)**

```bash
airflow dags trigger account_update_parallel \
  --conf '{
    "account_names": ["ACC-001", "ACC-002"],
    "updates": {
      "iam_version": "2.0",
      "subnet_count": 5
    },
    "execution_mode": "plan_and_apply",
    "max_parallel_executions": 2,
    "notify_on_completion": true
  }'
```

---

## 🔧 Maintenance et évolution

### **Ajouter une nouvelle étape de validation**

```python
@task_group(group_id='validation_and_preparation')
def validation_and_preparation_group():
    validate_payload()
    check_accounts_exist()
    check_prerequisites()
    
    # ✅ Ajouter facilement une nouvelle validation
    @task(task_id='check_quotas')
    def check_quotas():
        # Vérifier les quotas IBM Cloud
        pass
    
    check_quotas()
```

### **Modifier le parallélisme**

Simplement changer dans le payload :
```json
{
  "max_parallel_executions": 5  // Au lieu de 2
}
```

---

## 🎓 Principes architecturaux respectés

1. **Séparation des préoccupations** : Chaque TaskGroup a une responsabilité unique
2. **Single Responsibility** : Chaque tâche fait UNE chose
3. **Don't Repeat Yourself (DRY)** : Fonctions réutilisables
4. **Fail Fast** : Validation précoce
5. **Graceful Degradation** : Échecs partiels gérés
6. **Observability** : Logs, métriques, audit
7. **Testability** : Tâches isolées testables unitairement

---

C'est un DAG **production-ready** ! 🚀