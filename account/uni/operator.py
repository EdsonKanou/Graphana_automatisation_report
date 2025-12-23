"""
DAG unifié pour l'extraction IAM avec branchement conditionnel.

Ce DAG détecte automatiquement le mode d'extraction et exécute le workflow approprié :
- BY_NAMES : Workflow d'extraction par noms de comptes
- BY_FILTERS : Workflow d'extraction par filtres orchestrator/environment

Architecture:
    detect_mode() → branch_by_mode()
                         ↓
        ┌────────────────┴────────────────┐
        │                                 │
    BY_NAMES                         BY_FILTERS
        │                                 │
    check_account_names            validate_filters
        ↓                                 ↓
    get_accounts_by_names          get_accounts_by_filters
        ↓                                 ↓
    create_tmp_dir_names           create_tmp_dir_filters
        ↓                                 ↓
    extract_iam_names              extract_iam_filters
        ↓                                 ↓
    zip_results_names              zip_results_filters
        ↓                                 ↓
    upload_names                   upload_filters
        └────────────────┬────────────────┘
                         ↓
                  final_summary()
"""

import logging
from pathlib import Path
from typing import Any, List, Literal
from sqlalchemy.orm import Session as SASession

from bpzi_airflow_library.decorators import product_action, step, task_group
from bpzi_airflow_library.dependencies import depends
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from account.database_models import AccountExtractIAM
from account.services.extract_iam_account import TMP_DIRECTORY_PATH
from account.schemas.unified_extract_iam_payload import (
    UnifiedExtractIamPayload,
    ExtractionMode
)

logger = logging.getLogger(__name__)


# ============================================================================
# DAG Unifié avec Branchement Conditionnel
# ============================================================================

@product_action(
    action_id=Path(__file__).stem,
    payload=UnifiedExtractIamPayload,
    tags=["account", "iam", "unified", "branching"],
    doc_md="""
    # DAG Extract IAM (Unified with Branching)
    
    Universal DAG for extracting IAM data from AWS accounts.
    Automatically detects the extraction mode and executes the appropriate workflow.
    
    ## Mode 1: BY_NAMES (Extract specific accounts)
    ```json
    {
        "account_names": ["ac0021000259", "ac0021000260"]
    }
    ```
    **Workflow:** check_names → get_by_names → create_dir_names → extract_names → zip_names → upload_names
    
    ## Mode 2: BY_FILTERS (Extract by criteria)
    ```json
    {
        "accounts_orchestrator": "PAASV4",
        "accounts_environment": "PRD"
    }
    ```
    **Workflow:** validate_filters → get_by_filters → create_dir_filters → extract_filters → zip_filters → upload_filters
    
    ## Architecture
    The DAG branches after mode detection:
    - `detect_mode()` → determines BY_NAMES or BY_FILTERS
    - `branch_by_mode()` → routes execution to the appropriate workflow
    - Both workflows converge at `final_summary()`
    """
)
def dag_extract_iam_create() -> None:
    """
    DAG unifié avec branchement conditionnel selon le mode d'extraction.
    """
    
    # ========================================================================
    # STEP 0: Détection du mode d'extraction
    # ========================================================================
    
    @step
    def detect_extraction_mode(
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
    ) -> dict[str, Any]:
        """
        Détecte automatiquement le mode d'extraction basé sur le payload.
        
        Returns:
            dict: Informations sur le mode détecté
        """
        logger.info("=" * 80)
        logger.info("🚀 DAG Extract IAM - Starting Mode Detection")
        logger.info("=" * 80)
        
        mode = payload.get_extraction_mode()
        
        mode_info = {
            "mode": mode.value,
            "mode_enum": mode,
        }
        
        if mode == ExtractionMode.BY_NAMES:
            account_names = payload.get_account_names()
            mode_info.update({
                "account_names": account_names,
                "account_count": len(account_names)
            })
            
            logger.info(f"🔍 Mode detected: BY_NAMES")
            logger.info(f"   Accounts to extract: {len(account_names)}")
            for idx, name in enumerate(account_names, 1):
                logger.info(f"   [{idx}] {name}")
            
            if payload.accounts_orchestrator != "ALL" or payload.accounts_environment != "ALL":
                logger.warning(
                    "⚠ Note: Filter parameters present but IGNORED "
                    "(account_names takes priority)"
                )
        
        else:  # BY_FILTERS
            mode_info.update({
                "orchestrator": payload.accounts_orchestrator,
                "environment": payload.accounts_environment,
                "is_full_extraction": payload.is_full_extraction()
            })
            
            logger.info(f"🔍 Mode detected: BY_FILTERS")
            logger.info(f"   Orchestrator: {payload.accounts_orchestrator}")
            logger.info(f"   Environment: {payload.accounts_environment}")
            
            if payload.is_full_extraction():
                logger.warning("⚠ Full extraction mode (ALL + ALL)")
        
        logger.info("✓ Mode detection completed")
        logger.info(f"→ Routing to: {mode.value.upper()} workflow")
        logger.info("=" * 80)
        
        return mode_info
    
    # ========================================================================
    # BRANCHING LOGIC: Décision du chemin à suivre
    # ========================================================================
    
    def branch_by_mode(**context) -> str:
        """
        Fonction de branchement : décide quel chemin suivre.
        
        Returns:
            str: ID de la prochaine task à exécuter
        """
        # Récupération du mode depuis XCom
        ti = context['ti']
        mode_info = ti.xcom_pull(task_ids='detect_extraction_mode')
        
        mode = mode_info['mode']
        
        logger.info(f"🔀 Branching decision: {mode.upper()}")
        
        if mode == 'by_names':
            next_task = 'by_names_workflow.check_account_names'
            logger.info(f"→ Routing to BY_NAMES workflow")
        else:
            next_task = 'by_filters_workflow.validate_filters'
            logger.info(f"→ Routing to BY_FILTERS workflow")
        
        return next_task
    
    # Opérateur de branchement
    branch_operator = BranchPythonOperator(
        task_id='branch_by_mode',
        python_callable=branch_by_mode,
        provide_context=True
    )
    
    # ========================================================================
    # WORKFLOW 1: Extraction BY_NAMES
    # ========================================================================
    
    @task_group(group_id='by_names_workflow')
    def by_names_workflow():
        """Workflow complet pour l'extraction par noms de comptes."""
        
        @step
        def check_account_names(
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> dict[str, Any]:
            """Vérifie l'existence des comptes demandés."""
            logger.info("=" * 80)
            logger.info("📋 BY_NAMES Workflow - Step 1: Check Account Names")
            logger.info("=" * 80)
            
            account_names = payload.get_account_names()
            
            # Vérification en base
            query = db_session.query(AccountExtractIAM).filter(
                AccountExtractIAM.account_name.in_(account_names)
            )
            found_accounts = query.all()
            found_names = {acc.account_name for acc in found_accounts}
            missing_names = set(account_names) - found_names
            
            if not found_accounts:
                error_msg = f"No accounts found with names: {account_names}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"✓ Found {len(found_accounts)}/{len(account_names)} account(s)")
            
            if missing_names:
                logger.warning(f"⚠ Missing accounts: {sorted(missing_names)}")
            
            return {
                "requested": account_names,
                "found_count": len(found_accounts),
                "missing_count": len(missing_names),
                "missing_names": list(missing_names)
            }
        
        @step
        def get_accounts_by_names(
            check_result: dict[str, Any],
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> List[AccountExtractIAM]:
            """Récupère les comptes par leurs noms."""
            logger.info("📋 BY_NAMES Workflow - Step 2: Get Accounts by Names")
            
            account_names = payload.get_account_names()
            
            query = db_session.query(AccountExtractIAM).filter(
                AccountExtractIAM.account_name.in_(account_names)
            )
            account_list = query.all()
            
            logger.info(f"✓ Retrieved {len(account_list)} account(s)")
            
            return account_list
        
        @step
        def create_tmp_directory_names(
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
        ) -> str:
            """Crée le répertoire temporaire pour BY_NAMES."""
            logger.info("📋 BY_NAMES Workflow - Step 3: Create Temporary Directory")
            
            directory_path = generate_directory_path(
                payload.subscription_id,
                TMP_DIRECTORY_PATH,
                suffix="by_names"  # Suffixe pour différencier
            )
            
            Path(directory_path).mkdir(parents=True, exist_ok=True)
            logger.info(f"✓ Directory created: {directory_path}")
            
            return directory_path
        
        @step
        def extract_iam_to_excel_names(
            account_list: List[AccountExtractIAM],
            directory_path: str,
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
            vault: Vault = depends(vault_dependency),
        ) -> dict[str, Any]:
            """Extrait les IAM pour les comptes nommés."""
            logger.info("=" * 80)
            logger.info("📋 BY_NAMES Workflow - Step 4: Extract IAM to Excel")
            logger.info("=" * 80)
            
            extraction_results = []
            successful = 0
            
            for idx, account in enumerate(account_list, start=1):
                try:
                    logger.info(
                        f"[{idx}/{len(account_list)}] Extracting: {account.account_name}"
                    )
                    
                    apikey = get_account_api_key(
                        account_number=account.account_number,
                        vault=vault
                    )
                    
                    excel_file_path = extract_iam_data_to_excel(
                        account=account,
                        api_key=apikey,
                        output_directory=directory_path
                    )
                    
                    extraction_results.append({
                        "account_name": account.account_name,
                        "file_path": excel_file_path,
                        "status": "success"
                    })
                    
                    successful += 1
                    logger.info(f"   ✓ Success")
                    
                except Exception as e:
                    logger.error(f"   ✗ Failed: {e}")
                    extraction_results.append({
                        "account_name": account.account_name,
                        "status": "failed",
                        "error": str(e)
                    })
                    raise
            
            summary = {
                "workflow": "BY_NAMES",
                "total": len(account_list),
                "successful": successful,
                "results": extraction_results,
                "directory": directory_path
            }
            
            logger.info(f"✓ Extraction completed: {successful}/{len(account_list)}")
            logger.info("=" * 80)
            
            return summary
        
        @step
        def zip_results_names(
            extraction_summary: dict[str, Any],
        ) -> str:
            """Compresse les résultats BY_NAMES."""
            logger.info("📋 BY_NAMES Workflow - Step 5: Compress Results")
            
            directory_path = extraction_summary["directory"]
            zip_path = f"{directory_path}.zip"
            
            import shutil
            shutil.make_archive(directory_path, 'zip', directory_path)
            
            logger.info(f"✓ ZIP created: {zip_path}")
            
            return zip_path
        
        @step
        def upload_to_cos_names(
            zip_path: str,
            extraction_summary: dict[str, Any],
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
        ) -> dict[str, Any]:
            """Upload les résultats BY_NAMES vers COS."""
            logger.info("📋 BY_NAMES Workflow - Step 6: Upload to COS")
            
            cos_url = upload_file_to_cos(
                file_path=zip_path,
                subscription_id=payload.subscription_id
            )
            
            logger.info(f"✓ Upload completed: {cos_url}")
            
            return {
                "workflow": "BY_NAMES",
                "extraction": extraction_summary,
                "upload": {
                    "zip_path": zip_path,
                    "cos_url": cos_url,
                    "status": "success"
                }
            }
        
        # Flux du workflow BY_NAMES
        check_result = check_account_names()
        accounts = get_accounts_by_names(check_result)
        directory = create_tmp_directory_names()
        extraction = extract_iam_to_excel_names(accounts, directory)
        zip_file = zip_results_names(extraction)
        final_result = upload_to_cos_names(zip_file, extraction)
        
        return final_result
    
    # ========================================================================
    # WORKFLOW 2: Extraction BY_FILTERS
    # ========================================================================
    
    @task_group(group_id='by_filters_workflow')
    def by_filters_workflow():
        """Workflow complet pour l'extraction par filtres."""
        
        @step
        def validate_filters(
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
        ) -> dict[str, Any]:
            """Valide les filtres orchestrator/environment."""
            logger.info("=" * 80)
            logger.info("🔧 BY_FILTERS Workflow - Step 1: Validate Filters")
            logger.info("=" * 80)
            
            logger.info(f"Filters to apply:")
            logger.info(f"   Orchestrator: {payload.accounts_orchestrator}")
            logger.info(f"   Environment: {payload.accounts_environment}")
            logger.info(f"   Scope: {payload._get_extraction_scope()}")
            
            return {
                "orchestrator": payload.accounts_orchestrator,
                "environment": payload.accounts_environment,
                "is_full": payload.is_full_extraction()
            }
        
        @step
        def get_accounts_by_filters(
            filter_info: dict[str, Any],
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> List[AccountExtractIAM]:
            """Récupère les comptes selon les filtres."""
            logger.info("🔧 BY_FILTERS Workflow - Step 2: Get Accounts by Filters")
            
            orchestrator_values = payload.get_orchestrator_filter_values()
            environment_values = payload.get_environment_filter_values()
            
            logger.info(f"Applying filters:")
            logger.info(f"   account_type IN {orchestrator_values}")
            logger.info(f"   account_env IN {environment_values}")
            
            query = db_session.query(AccountExtractIAM)
            query = query.filter(AccountExtractIAM.account_type.in_(orchestrator_values))
            query = query.filter(AccountExtractIAM.account_env.in_(environment_values))
            
            account_list = query.all()
            
            if not account_list:
                error_msg = (
                    f"No accounts found: orchestrator={filter_info['orchestrator']}, "
                    f"environment={filter_info['environment']}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"✓ Retrieved {len(account_list)} account(s)")
            
            # Breakdown
            orch_counts = {}
            env_counts = {}
            for acc in account_list:
                orch_counts[acc.account_type] = orch_counts.get(acc.account_type, 0) + 1
                env_counts[acc.account_env] = env_counts.get(acc.account_env, 0) + 1
            
            logger.info(f"   By orchestrator: {orch_counts}")
            logger.info(f"   By environment: {env_counts}")
            
            return account_list
        
        @step
        def create_tmp_directory_filters(
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
        ) -> str:
            """Crée le répertoire temporaire pour BY_FILTERS."""
            logger.info("🔧 BY_FILTERS Workflow - Step 3: Create Temporary Directory")
            
            directory_path = generate_directory_path(
                payload.subscription_id,
                TMP_DIRECTORY_PATH,
                suffix="by_filters"  # Suffixe pour différencier
            )
            
            Path(directory_path).mkdir(parents=True, exist_ok=True)
            logger.info(f"✓ Directory created: {directory_path}")
            
            return directory_path
        
        @step
        def extract_iam_to_excel_filters(
            account_list: List[AccountExtractIAM],
            directory_path: str,
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
            vault: Vault = depends(vault_dependency),
        ) -> dict[str, Any]:
            """Extrait les IAM pour les comptes filtrés."""
            logger.info("=" * 80)
            logger.info("🔧 BY_FILTERS Workflow - Step 4: Extract IAM to Excel")
            logger.info("=" * 80)
            
            extraction_results = []
            successful = 0
            
            for idx, account in enumerate(account_list, start=1):
                try:
                    logger.info(
                        f"[{idx}/{len(account_list)}] Extracting: {account.account_name} "
                        f"({account.account_type}/{account.account_env})"
                    )
                    
                    apikey = get_account_api_key(
                        account_number=account.account_number,
                        vault=vault
                    )
                    
                    excel_file_path = extract_iam_data_to_excel(
                        account=account,
                        api_key=apikey,
                        output_directory=directory_path
                    )
                    
                    extraction_results.append({
                        "account_name": account.account_name,
                        "account_type": account.account_type,
                        "account_env": account.account_env,
                        "file_path": excel_file_path,
                        "status": "success"
                    })
                    
                    successful += 1
                    logger.info(f"   ✓ Success")
                    
                except Exception as e:
                    logger.error(f"   ✗ Failed: {e}")
                    extraction_results.append({
                        "account_name": account.account_name,
                        "status": "failed",
                        "error": str(e)
                    })
                    raise
            
            # Breakdown par type/env
            orch_counts = {}
            env_counts = {}
            for result in extraction_results:
                if result["status"] == "success":
                    orch_counts[result["account_type"]] = orch_counts.get(result["account_type"], 0) + 1
                    env_counts[result["account_env"]] = env_counts.get(result["account_env"], 0) + 1
            
            summary = {
                "workflow": "BY_FILTERS",
                "total": len(account_list),
                "successful": successful,
                "orchestrator_breakdown": orch_counts,
                "environment_breakdown": env_counts,
                "results": extraction_results,
                "directory": directory_path
            }
            
            logger.info(f"✓ Extraction completed: {successful}/{len(account_list)}")
            logger.info(f"   Orchestrator: {orch_counts}")
            logger.info(f"   Environment: {env_counts}")
            logger.info("=" * 80)
            
            return summary
        
        @step
        def zip_results_filters(
            extraction_summary: dict[str, Any],
        ) -> str:
            """Compresse les résultats BY_FILTERS."""
            logger.info("🔧 BY_FILTERS Workflow - Step 5: Compress Results")
            
            directory_path = extraction_summary["directory"]
            zip_path = f"{directory_path}.zip"
            
            import shutil
            shutil.make_archive(directory_path, 'zip', directory_path)
            
            logger.info(f"✓ ZIP created: {zip_path}")
            
            return zip_path
        
        @step
        def upload_to_cos_filters(
            zip_path: str,
            extraction_summary: dict[str, Any],
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
        ) -> dict[str, Any]:
            """Upload les résultats BY_FILTERS vers COS."""
            logger.info("🔧 BY_FILTERS Workflow - Step 6: Upload to COS")
            
            cos_url = upload_file_to_cos(
                file_path=zip_path,
                subscription_id=payload.subscription_id
            )
            
            logger.info(f"✓ Upload completed: {cos_url}")
            
            return {
                "workflow": "BY_FILTERS",
                "extraction": extraction_summary,
                "upload": {
                    "zip_path": zip_path,
                    "cos_url": cos_url,
                    "status": "success"
                }
            }
        
        # Flux du workflow BY_FILTERS
        filter_validation = validate_filters()
        accounts = get_accounts_by_filters(filter_validation)
        directory = create_tmp_directory_filters()
        extraction = extract_iam_to_excel_filters(accounts, directory)
        zip_file = zip_results_filters(extraction)
        final_result = upload_to_cos_filters(zip_file, extraction)
        
        return final_result
    
    # ========================================================================
    # CONVERGENCE: Point de jonction des deux workflows
    # ========================================================================
    
    # Dummy operators pour la convergence
    join_operator = DummyOperator(
        task_id='join_workflows',
        trigger_rule='none_failed_or_skipped'
    )
    
    @step
    def final_summary(**context) -> dict[str, Any]:
        """
        Résumé final unifié (appelé après convergence des workflows).
        """
        ti = context['ti']
        
        # Tentative de récupérer les résultats des deux workflows
        try:
            by_names_result = ti.xcom_pull(task_ids='by_names_workflow.upload_to_cos_names')
        except:
            by_names_result = None
        
        try:
            by_filters_result = ti.xcom_pull(task_ids='by_filters_workflow.upload_to_cos_filters')
        except:
            by_filters_result = None
        
        # Le workflow exécuté est celui qui a un résultat
        executed_workflow = by_names_result or by_filters_result
        
        logger.info("=" * 80)
        logger.info("🎉 DAG Extract IAM - COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Workflow executed: {executed_workflow['workflow']}")
        logger.info(f"Total accounts processed: {executed_workflow['extraction']['total']}")
        logger.info(f"Successful extractions: {executed_workflow['extraction']['successful']}")
        logger.info(f"COS URL: {executed_workflow['upload']['cos_url']}")
        logger.info("=" * 80)
        
        return executed_workflow
    
    # ========================================================================
    # DÉFINITION DU GRAPHE COMPLET
    # ========================================================================
    
    # Détection du mode
    mode_detection = detect_extraction_mode()
    
    # Instanciation des workflows
    names_wf = by_names_workflow()
    filters_wf = by_filters_workflow()
    
    # Résumé final
    summary = final_summary()
    
    # DÉFINITION DES DÉPENDANCES
    mode_detection >> branch_operator
    branch_operator >> [names_wf, filters_wf]
    [names_wf, filters_wf] >> join_operator >> summary


# ============================================================================
# Exécution du DAG
# ============================================================================

dag_extract_iam_create()