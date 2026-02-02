"""
DAG unifié pour l'extraction IAM avec branchement minimal et parallélisation.

Architecture:
    detect_mode()
        ↓
    branch_by_mode()
        ↓
    ┌───────────────┴────────────────┐
    │                                │
get_accounts_by_names     get_accounts_by_filters
    │                                │
    └───────────────┬────────────────┘
                    ↓
            join_get_accounts
                    ↓
          create_tmp_directory()
                    ↓
    extract_iam_for_account.expand(account=accounts) ← PARALLÈLE !
                    ↓
            zip_all_results()
                    ↓
             upload_to_cos()

Avantages:
- Branchement UNIQUEMENT pour la récupération des comptes
- Workflow commun après récupération (pas de duplication)
- Extraction PARALLÈLE pour tous les comptes
- Graphe clair montrant les 2 chemins possibles
"""

import logging
from pathlib import Path
from typing import Any, List, Dict
from sqlalchemy.orm import Session as SASession

from bpzi_airflow_library.decorators import product_action, step
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
# DAG Unifié avec Branchement Minimal + Parallélisation
# ============================================================================

@product_action(
    action_id=Path(__file__).stem,
    payload=UnifiedExtractIamPayload,
    tags=["account", "iam", "branching", "parallel"],
    doc_md="""
    # DAG Extract IAM (Branching + Parallelization)
    
    Unified DAG with minimal branching for account retrieval and parallel extraction.
    
    ## Architecture
    
    ```
    detect_mode → branch → [get_by_names OR get_by_filters] → join
                                    ↓
                          create_directory
                                    ↓
                    extract_iam.expand(accounts) ← PARALLEL
                                    ↓
                              zip_results
                                    ↓
                              upload_to_cos
    ```
    
    ## Mode 1: BY_NAMES
    ```json
    {
        "account_names": ["ac0021000259", "ac0021000260"]
    }
    ```
    **Path:** get_accounts_by_names → common workflow
    
    ## Mode 2: BY_FILTERS
    ```json
    {
        "accounts_orchestrator": "PAASV4",
        "accounts_environment": "PRD"
    }
    ```
    **Path:** get_accounts_by_filters → common workflow
    
    ## Key Features
    - Minimal branching (only for account retrieval query)
    - Parallel extraction (one task per account)
    - Common workflow after account retrieval (no duplication)
    - Scalable up to 100+ accounts
    """
)
def dag_extract_iam_create() -> None:
    """
    DAG unifié avec branchement minimal et extraction parallèle.
    """
    
    # ========================================================================
    # STEP 0: Détection du mode d'extraction
    # ========================================================================
    
    @step
    def detect_extraction_mode(
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
    ) -> Dict[str, Any]:
        """
        Détecte automatiquement le mode d'extraction.
        
        Returns:
            dict: Informations sur le mode détecté
        """
        logger.info("=" * 80)
        logger.info("🚀 DAG Extract IAM - Starting Mode Detection")
        logger.info("=" * 80)
        
        mode = payload.get_extraction_mode()
        
        mode_info = {
            "mode": mode.value,
        }
        
        if mode == ExtractionMode.BY_NAMES:
            account_names = payload.get_account_names()
            mode_info.update({
                "account_names": account_names,
                "count": len(account_names)
            })
            
            logger.info(f"🔍 Mode detected: BY_NAMES")
            logger.info(f"   Requested accounts: {len(account_names)}")
            for idx, name in enumerate(account_names, 1):
                logger.info(f"     [{idx}] {name}")
        
        else:  # BY_FILTERS
            mode_info.update({
                "orchestrator": payload.accounts_orchestrator,
                "environment": payload.accounts_environment,
            })
            
            logger.info(f"🔍 Mode detected: BY_FILTERS")
            logger.info(f"   Orchestrator: {payload.accounts_orchestrator}")
            logger.info(f"   Environment: {payload.accounts_environment}")
            logger.info(f"   Scope: {payload._get_extraction_scope()}")
            
            if payload.is_full_extraction():
                logger.warning("⚠ Full extraction mode (ALL + ALL)")
        
        logger.info(f"→ Routing to: {mode.value.upper()} path")
        logger.info("=" * 80)
        
        return mode_info
    
    # ========================================================================
    # BRANCHING: Décision du chemin à suivre
    # ========================================================================
    
    def branch_by_mode(**context) -> str:
        """
        Fonction de branchement : décide quel chemin suivre.
        
        Returns:
            str: ID de la prochaine task à exécuter
        """
        ti = context['ti']
        mode_info = ti.xcom_pull(task_ids='detect_extraction_mode')
        
        mode = mode_info['mode']
        
        logger.info(f"🔀 Branching decision: {mode.upper()}")
        
        if mode == 'by_names':
            next_task = 'get_accounts_by_names'
            logger.info(f"→ Routing to get_accounts_by_names")
        else:
            next_task = 'get_accounts_by_filters'
            logger.info(f"→ Routing to get_accounts_by_filters")
        
        return next_task
    
    # Opérateur de branchement
    branch_operator = BranchPythonOperator(
        task_id='branch_by_mode',
        python_callable=branch_by_mode,
        provide_context=True
    )
    
    # ========================================================================
    # BRANCHE 1: Récupération des comptes par NOMS
    # ========================================================================
    
    @step
    def get_accounts_by_names(
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> List[Dict[str, Any]]:
        """
        Récupère les comptes AWS par leurs noms.
        
        Returns:
            List[Dict]: Liste des comptes avec leurs métadonnées
        """
        logger.info("=" * 80)
        logger.info("📋 BY_NAMES Path: Retrieving accounts by names")
        logger.info("=" * 80)
        
        account_names = payload.get_account_names()
        logger.info(f"Searching for {len(account_names)} account(s) by name:")
        for idx, name in enumerate(account_names, 1):
            logger.info(f"  [{idx}] {name}")
        
        try:
            # Query SQL: filtre par account_name
            query = db_session.query(AccountExtractIAM).filter(
                AccountExtractIAM.account_name.in_(account_names)
            )
            
            logger.debug(f"SQL Query: {query}")
            
            account_list_orm = query.all()
            
            # Vérification
            if not account_list_orm:
                error_msg = f"No accounts found with names: {account_names}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"✓ Found {len(account_list_orm)} account(s)")
            
            # Vérification des comptes manquants
            found_names = {acc.account_name for acc in account_list_orm}
            missing_names = set(account_names) - found_names
            
            if missing_names:
                logger.warning(
                    f"⚠ WARNING: {len(missing_names)} account(s) not found in database:"
                )
                for name in sorted(missing_names):
                    logger.warning(f"   - {name}")
                logger.info(f"Proceeding with {len(account_list_orm)} found account(s)")
            
            # Conversion en dict pour l'expand
            account_list = []
            for account in account_list_orm:
                account_list.append({
                    "account_id": account.account_id,
                    "account_number": account.account_number,
                    "account_name": account.account_name,
                    "account_type": account.account_type,
                    "account_env": account.account_env,
                })
            
            logger.info("=" * 80)
            
            return account_list
        
        except Exception as e:
            logger.error(f"Failed to retrieve accounts: {str(e)}")
            raise
    
    # ========================================================================
    # BRANCHE 2: Récupération des comptes par FILTRES
    # ========================================================================
    
    @step
    def get_accounts_by_filters(
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> List[Dict[str, Any]]:
        """
        Récupère les comptes AWS par filtres orchestrator/environment.
        
        Returns:
            List[Dict]: Liste des comptes avec leurs métadonnées
        """
        logger.info("=" * 80)
        logger.info("🔧 BY_FILTERS Path: Retrieving accounts by filters")
        logger.info("=" * 80)
        
        orchestrator_values = payload.get_orchestrator_filter_values()
        environment_values = payload.get_environment_filter_values()
        
        logger.info(f"Applying filters:")
        logger.info(f"   - account_type IN {orchestrator_values}")
        logger.info(f"   - account_env IN {environment_values}")
        
        try:
            # Query SQL: filtre par account_type et account_env
            query = db_session.query(AccountExtractIAM)
            query = query.filter(AccountExtractIAM.account_type.in_(orchestrator_values))
            query = query.filter(AccountExtractIAM.account_env.in_(environment_values))
            
            logger.debug(f"SQL Query: {query}")
            
            account_list_orm = query.all()
            
            # Vérification
            if not account_list_orm:
                error_msg = (
                    f"No accounts found matching criteria: "
                    f"orchestrator={payload.accounts_orchestrator}, "
                    f"environment={payload.accounts_environment}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"✓ Found {len(account_list_orm)} account(s)")
            
            # Conversion en dict pour l'expand
            account_list = []
            for account in account_list_orm:
                account_list.append({
                    "account_id": account.account_id,
                    "account_number": account.account_number,
                    "account_name": account.account_name,
                    "account_type": account.account_type,
                    "account_env": account.account_env,
                })
            
            # Breakdown par orchestrator et environment
            orch_counts = {}
            env_counts = {}
            for acc in account_list:
                orch_counts[acc["account_type"]] = orch_counts.get(acc["account_type"], 0) + 1
                env_counts[acc["account_env"]] = env_counts.get(acc["account_env"], 0) + 1
            
            logger.info(f"Breakdown by orchestrator: {orch_counts}")
            logger.info(f"Breakdown by environment: {env_counts}")
            logger.info("=" * 80)
            
            return account_list
        
        except Exception as e:
            logger.error(f"Failed to retrieve accounts: {str(e)}")
            raise
    
    # ========================================================================
    # CONVERGENCE: Point de jonction des deux branches
    # ========================================================================
    
    join_get_accounts = DummyOperator(
        task_id='join_get_accounts',
        trigger_rule='none_failed_or_skipped'
    )
    
    # ========================================================================
    # WORKFLOW COMMUN: Après convergence (pas de duplication)
    # ========================================================================
    
    @step
    def create_tmp_directory(
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
    ) -> str:
        """
        Crée le répertoire temporaire pour stocker les fichiers Excel.
        
        Returns:
            str: Chemin absolu du répertoire créé
        """
        logger.info("=" * 80)
        logger.info("📁 Common Workflow - Step 1: Create Temporary Directory")
        logger.info("=" * 80)
        
        directory_path = generate_directory_path(
            payload.subscription_id,
            TMP_DIRECTORY_PATH
        )
        
        # Création du répertoire
        Path(directory_path).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"✓ Directory created: {directory_path}")
        logger.info("=" * 80)
        
        return directory_path
    
    @step
    def extract_iam_for_account(
        account: Dict[str, Any],
        directory_path: str,
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> Dict[str, Any]:
        """
        Extrait les données IAM pour UN SEUL compte.
        
        Cette fonction sera exécutée en PARALLÈLE pour chaque compte
        grâce à .expand(account=accounts).
        
        Args:
            account: Dictionnaire contenant les infos du compte
            directory_path: Chemin du répertoire de sortie
            payload: Payload validé
            vault: Service Vault pour l'API key
        
        Returns:
            dict: Résultat de l'extraction pour ce compte
        """
        account_name = account["account_name"]
        account_number = account["account_number"]
        account_type = account["account_type"]
        account_env = account["account_env"]
        
        logger.info("=" * 80)
        logger.info(f"🔄 Common Workflow - Step 2: Extract IAM for {account_name}")
        logger.info(f"   Account Number: {account_number}")
        logger.info(f"   Type: {account_type} | Env: {account_env}")
        logger.info("=" * 80)
        
        try:
            # Récupération de l'API key depuis Vault
            logger.info("   Retrieving API key from Vault...")
            apikey = get_account_api_key(
                account_number=account_number,
                vault=vault
            )
            
            # Extraction des données IAM
            logger.info("   Extracting IAM data...")
            excel_file_path = extract_iam_data_to_excel(
                account=account,
                api_key=apikey,
                output_directory=directory_path
            )
            
            file_name = Path(excel_file_path).name
            file_size_mb = Path(excel_file_path).stat().st_size / (1024 * 1024)
            
            result = {
                "account_name": account_name,
                "account_number": account_number,
                "account_type": account_type,
                "account_env": account_env,
                "file_path": excel_file_path,
                "file_name": file_name,
                "file_size_mb": round(file_size_mb, 2),
                "status": "success"
            }
            
            logger.info(f"   ✓ Extraction successful")
            logger.info(f"     File: {file_name} ({file_size_mb:.2f} MB)")
            logger.info("=" * 80)
            
            return result
            
        except Exception as e:
            error_msg = str(e)
            
            logger.error(f"   ✗ Extraction failed: {error_msg}")
            logger.info("=" * 80)
            
            result = {
                "account_name": account_name,
                "account_number": account_number,
                "account_type": account_type,
                "account_env": account_env,
                "status": "failed",
                "error": error_msg
            }
            
            # Lever l'erreur pour arrêter le workflow
            raise
    
    @step
    def zip_all_results(
        extraction_results: List[Dict[str, Any]],
        directory_path: str,
        mode_info: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Agrège les résultats de toutes les extractions et compresse.
        
        Args:
            extraction_results: Liste des résultats de toutes les extractions
            directory_path: Chemin du répertoire contenant les fichiers
            mode_info: Informations sur le mode d'extraction
        
        Returns:
            dict: Résumé complet de l'extraction + chemin du ZIP
        """
        logger.info("=" * 80)
        logger.info("📦 Common Workflow - Step 3: Compress Results")
        logger.info("=" * 80)
        
        # Calcul des statistiques
        total_accounts = len(extraction_results)
        successful = sum(1 for r in extraction_results if r["status"] == "success")
        failed = sum(1 for r in extraction_results if r["status"] == "failed")
        success_rate = (successful / total_accounts * 100) if total_accounts > 0 else 0
        
        logger.info(f"Extraction statistics:")
        logger.info(f"   Total accounts: {total_accounts}")
        logger.info(f"   Successful: {successful}")
        logger.info(f"   Failed: {failed}")
        logger.info(f"   Success rate: {success_rate:.1f}%")
        
        # Breakdown par type/env (mode BY_FILTERS uniquement)
        mode = ExtractionMode(mode_info["mode"])
        breakdown = {}
        
        if mode == ExtractionMode.BY_FILTERS:
            orch_counts = {}
            env_counts = {}
            
            for result in extraction_results:
                if result["status"] == "success":
                    orch = result["account_type"]
                    env = result["account_env"]
                    orch_counts[orch] = orch_counts.get(orch, 0) + 1
                    env_counts[env] = env_counts.get(env, 0) + 1
            
            breakdown = {
                "orchestrator": orch_counts,
                "environment": env_counts
            }
            
            logger.info(f"   By orchestrator: {orch_counts}")
            logger.info(f"   By environment: {env_counts}")
        
        # Compression du répertoire
        logger.info(f"Compressing directory: {directory_path}")
        
        zip_path = f"{directory_path}.zip"
        
        import shutil
        shutil.make_archive(
            base_name=directory_path,
            format='zip',
            root_dir=directory_path
        )
        
        # Vérification et taille du ZIP
        if not Path(zip_path).exists():
            raise FileNotFoundError(f"ZIP file not created: {zip_path}")
        
        zip_size_mb = Path(zip_path).stat().st_size / (1024 * 1024)
        
        logger.info(f"✓ ZIP created successfully:")
        logger.info(f"   Path: {zip_path}")
        logger.info(f"   Size: {zip_size_mb:.2f} MB")
        logger.info(f"   Files: {successful}")
        logger.info("=" * 80)
        
        # Résumé complet
        summary = {
            "mode": mode_info["mode"],
            "total_accounts": total_accounts,
            "successful_extractions": successful,
            "failed_extractions": failed,
            "success_rate": round(success_rate, 2),
            "results": extraction_results,
            "breakdown": breakdown,
            "zip_path": zip_path,
            "zip_size_mb": round(zip_size_mb, 2),
            "output_directory": directory_path
        }
        
        return summary
    
    @step
    def upload_to_cos(
        compression_summary: Dict[str, Any],
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
    ) -> Dict[str, Any]:
        """
        Upload le fichier ZIP vers Cloud Object Storage (COS).
        
        Args:
            compression_summary: Résumé de l'extraction et compression
            payload: Payload validé
        
        Returns:
            dict: Résumé final complet avec URL COS
        """
        logger.info("=" * 80)
        logger.info("☁️  Common Workflow - Step 4: Upload to Cloud Object Storage")
        logger.info("=" * 80)
        
        zip_path = compression_summary["zip_path"]
        
        logger.info(f"File to upload: {zip_path}")
        logger.info(f"Subscription ID: {payload.subscription_id}")
        
        try:
            # Upload vers COS
            cos_url = upload_file_to_cos(
                file_path=zip_path,
                subscription_id=payload.subscription_id
            )
            
            logger.info(f"✓ Upload successful")
            logger.info(f"   COS URL: {cos_url}")
            
            upload_status = "success"
            
        except Exception as e:
            logger.error(f"✗ Upload failed: {str(e)}")
            cos_url = None
            upload_status = "failed"
            raise
        
        # Résumé final global
        final_summary = {
            "extraction_mode": compression_summary["mode"],
            "total_accounts": compression_summary["total_accounts"],
            "successful_extractions": compression_summary["successful_extractions"],
            "failed_extractions": compression_summary["failed_extractions"],
            "success_rate": compression_summary["success_rate"],
            "breakdown": compression_summary["breakdown"],
            "zip_path": zip_path,
            "zip_size_mb": compression_summary["zip_size_mb"],
            "cos_url": cos_url,
            "upload_status": upload_status
        }
        
        logger.info("=" * 80)
        logger.info("🎉 DAG Extract IAM - COMPLETED")
        logger.info("=" * 80)
        logger.info(f"Mode: {compression_summary['mode'].upper()}")
        logger.info(f"Accounts processed: {compression_summary['successful_extractions']}/{compression_summary['total_accounts']}")
        logger.info(f"Success rate: {compression_summary['success_rate']}%")
        logger.info(f"COS URL: {cos_url}")
        logger.info("=" * 80)
        
        return final_summary
    
    # ========================================================================
    # DÉFINITION DU GRAPHE COMPLET
    # ========================================================================
    
    # Step 0: Détection du mode
    mode_info = detect_extraction_mode()
    
    # Branchement
    mode_info >> branch_operator
    
    # Branches
    accounts_by_names = get_accounts_by_names()
    accounts_by_filters = get_accounts_by_filters()
    
    branch_operator >> [accounts_by_names, accounts_by_filters]
    
    # Convergence
    [accounts_by_names, accounts_by_filters] >> join_get_accounts
    
    # NOTE IMPORTANTE: Pour passer les accounts au workflow commun,
    # on doit récupérer le résultat via XCom dans create_tmp_directory
    # ou créer une step intermédiaire qui récupère les accounts
    
    # Workflow commun
    directory = create_tmp_directory()
    
    # ASTUCE: Pour utiliser .expand() après une convergence,
    # on doit définir explicitement quelle sortie utiliser
    # Ici, on utilise un custom operator ou une fonction pour merger
    
    @step
    def get_accounts_from_branch(**context) -> List[Dict[str, Any]]:
        """
        Récupère les accounts depuis la branche exécutée.
        """
        ti = context['ti']
        
        # Tenter de récupérer depuis BY_NAMES
        try:
            accounts = ti.xcom_pull(task_ids='get_accounts_by_names')
            if accounts:
                logger.info(f"Retrieved {len(accounts)} accounts from BY_NAMES path")
                return accounts
        except:
            pass
        
        # Sinon récupérer depuis BY_FILTERS
        try:
            accounts = ti.xcom_pull(task_ids='get_accounts_by_filters')
            if accounts:
                logger.info(f"Retrieved {len(accounts)} accounts from BY_FILTERS path")
                return accounts
        except:
            pass
        
        raise ValueError("Failed to retrieve accounts from any branch")
    
    accounts_merged = get_accounts_from_branch()
    
    # Extraction parallèle
    join_get_accounts >> accounts_merged >> directory
    
    extraction_results = extract_iam_for_account.expand(
        account=accounts_merged
    ).partial(
        directory_path=directory
    )
    
    # Compression et upload
    compression_summary = zip_all_results(
        extraction_results,
        directory,
        mode_info
    )
    
    final_summary = upload_to_cos(compression_summary)


# ============================================================================
# Exécution du DAG
# ============================================================================

dag_extract_iam_create()