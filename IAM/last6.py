"""
DAG unifié optimisé pour l'extraction IAM avec parallélisation et gestion des échecs partiels.

Architecture optimisée:
    detect_mode()
        ↓
    get_accounts_list() (query conditionnelle BY_NAMES ou BY_FILTERS)
        ↓
    create_tmp_directory()
        ↓
    extract_iam_for_account.expand(account=accounts) ← PARALLÈLE !
        ↓
    zip_all_results() ← Continue même si certaines extractions ont échoué
        ↓
    upload_to_cos()

Gestion des échecs:
- Si 10/11 extractions réussissent → le DAG continue avec les 10 succès
- zip_all_results utilise trigger_rule='none_failed_min_one_success'
- Les comptes en échec sont logués mais n'arrêtent pas le workflow
"""

import logging
from pathlib import Path
from typing import Any, List, Dict
from sqlalchemy.orm import Session as SASession

from bpzi_airflow_library.decorators import product_action, step
from bpzi_airflow_library.dependencies import depends
from airflow.exceptions import AirflowFailException
from account.database_models import AccountExtractIAM
from account.services.extract_iam_account import TMP_DIRECTORY_PATH
from account.schemas.unified_extract_iam_payload import (
    UnifiedExtractIamPayload,
    ExtractionMode
)

logger = logging.getLogger(__name__)


# ============================================================================
# DAG Unifié Optimisé avec Parallélisation et Gestion des Échecs Partiels
# ============================================================================

@product_action(
    action_id=Path(__file__).stem,
    payload=UnifiedExtractIamPayload,
    tags=["account", "iam", "parallel", "fault-tolerant"],
    # Configuration du parallélisme au niveau du DAG
    max_active_tis_per_dag=20,  # Max 20 tasks actives en même temps dans ce DAG
    doc_md="""
    # DAG Extract IAM (Optimized with Fault Tolerance)
    
    Unified DAG for extracting IAM data from AWS accounts with:
    - Parallel execution (one task per account)
    - Fault tolerance (continues even if some accounts fail)
    - Automatic mode detection
    
    ## Key Features
    - **Parallel extraction**: Each account processed in a separate task
    - **Fault tolerant**: If 10/11 accounts succeed, the DAG continues with the 10
    - **Failed accounts are logged**: You can retry them later
    - **No data loss**: Successful extractions are always saved
    
    ## Supported Modes
    
    ### Mode 1: BY_NAMES
    ```json
    {
        "account_names": ["ac0021000259", "ac0021000260"]
    }
    ```
    
    ### Mode 2: BY_FILTERS
    ```json
    {
        "accounts_orchestrator": "PAASV4",
        "accounts_environment": "PRD"
    }
    ```
    
    ## Fault Tolerance Example
    - 11 accounts to process
    - 10 succeed, 1 fails
    - Result: ZIP with 10 files is created and uploaded
    - The failed account is logged for retry
    """
)
def dag_extract_iam_create() -> None:
    """
    DAG unifié optimisé avec extraction parallèle et tolérance aux pannes.
    """
    
    # ========================================================================
    # STEP 1: Détection du mode d'extraction
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
        logger.info("🚀 DAG Extract IAM - Starting")
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
            
            logger.info(f"🔍 Mode: BY_NAMES (no filters)")
            logger.info(f"   Requested accounts: {len(account_names)}")
            for idx, name in enumerate(account_names, 1):
                logger.info(f"     [{idx}] {name}")
        
        elif mode == ExtractionMode.BY_NAMES_WITH_FILTERS:
            account_names = payload.get_account_names()
            mode_info.update({
                "account_names": account_names,
                "count": len(account_names),
                "orchestrator": payload.accounts_orchestrator,
                "environment": payload.accounts_environment,
            })
            
            logger.info(f"🔍 Mode: BY_NAMES_WITH_FILTERS")
            logger.info(f"   Requested accounts: {len(account_names)}")
            for idx, name in enumerate(account_names, 1):
                logger.info(f"     [{idx}] {name}")
            logger.info(f"   Filters to validate:")
            logger.info(f"     - Orchestrator: {payload.accounts_orchestrator}")
            logger.info(f"     - Environment: {payload.accounts_environment}")
            logger.warning(f"   ⚠ Only accounts matching filters will be processed")
        
        else:  # BY_FILTERS
            mode_info.update({
                "orchestrator": payload.accounts_orchestrator,
                "environment": payload.accounts_environment,
            })
            
            logger.info(f"🔍 Mode: BY_FILTERS")
            logger.info(f"   Orchestrator: {payload.accounts_orchestrator}")
            logger.info(f"   Environment: {payload.accounts_environment}")
            logger.info(f"   Scope: {payload._get_extraction_scope()}")
            
            if payload.is_full_extraction():
                logger.warning("⚠ Full extraction mode (ALL + ALL)")
        
        logger.info("=" * 80)
        
        return mode_info
    
    # ========================================================================
    # STEP 2: Récupération des comptes (logique conditionnelle unifiée)
    # ========================================================================
    
    @step
    def get_accounts_list(
        mode_info: Dict[str, Any],
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> List[Dict[str, Any]]:
        """
        Récupère les comptes AWS selon le mode détecté.
        
        Modes supportés:
        - BY_NAMES : filtre par account_name uniquement
        - BY_FILTERS : filtre par account_type et account_env
        - BY_NAMES_WITH_FILTERS : filtre par account_name puis valide avec les filtres
        
        Returns:
            List[Dict]: Liste des comptes avec leurs métadonnées
        
        Raises:
            ValueError: Si aucun compte trouvé
        """
        logger.info("=" * 80)
        logger.info("📋 Step 1: Retrieving accounts from database")
        logger.info("=" * 80)
        
        mode = ExtractionMode(mode_info["mode"])
        
        try:
            query = db_session.query(AccountExtractIAM)
            
            # Logique selon le mode
            if mode == ExtractionMode.BY_NAMES:
                # Mode BY_NAMES : uniquement les noms, pas de filtres
                account_names = mode_info["account_names"]
                logger.info(f"Mode: BY_NAMES (no filters)")
                logger.info(f"Filtering by account names (count: {len(account_names)})")
                
                query = query.filter(
                    AccountExtractIAM.account_name.in_(account_names)
                )
            
            elif mode == ExtractionMode.BY_NAMES_WITH_FILTERS:
                # Mode BY_NAMES_WITH_FILTERS : noms + validation par filtres
                account_names = mode_info["account_names"]
                
                logger.info(f"Mode: BY_NAMES_WITH_FILTERS")
                logger.info(f"Input: {len(account_names)} account name(s)")
                logger.info(f"Filters to apply:")
                
                orchestrator_values = payload.get_orchestrator_filter_values()
                environment_values = payload.get_environment_filter_values()
                
                logger.info(f"   - account_type IN {orchestrator_values}")
                logger.info(f"   - account_env IN {environment_values}")
                
                # Query : noms + filtres
                query = query.filter(
                    AccountExtractIAM.account_name.in_(account_names)
                )
                query = query.filter(
                    AccountExtractIAM.account_type.in_(orchestrator_values)
                )
                query = query.filter(
                    AccountExtractIAM.account_env.in_(environment_values)
                )
            
            else:  # BY_FILTERS
                # Mode BY_FILTERS : uniquement les filtres
                orchestrator_values = payload.get_orchestrator_filter_values()
                environment_values = payload.get_environment_filter_values()
                
                logger.info(f"Mode: BY_FILTERS")
                logger.info(f"Filtering by:")
                logger.info(f"   - account_type IN {orchestrator_values}")
                logger.info(f"   - account_env IN {environment_values}")
                
                query = query.filter(
                    AccountExtractIAM.account_type.in_(orchestrator_values)
                )
                query = query.filter(
                    AccountExtractIAM.account_env.in_(environment_values)
                )
            
            # Exécution de la query
            logger.debug(f"SQL Query: {query}")
            account_list_orm = query.all()
            
            # Vérification
            if not account_list_orm:
                if mode == ExtractionMode.BY_NAMES:
                    error_msg = f"No accounts found with names: {mode_info['account_names']}"
                elif mode == ExtractionMode.BY_NAMES_WITH_FILTERS:
                    error_msg = (
                        f"No accounts found matching names AND filters. "
                        f"Input names: {mode_info['account_names']}, "
                        f"Filters: orchestrator={payload.accounts_orchestrator}, "
                        f"environment={payload.accounts_environment}"
                    )
                else:
                    error_msg = (
                        f"No accounts found matching filters: "
                        f"orchestrator={mode_info['orchestrator']}, "
                        f"environment={mode_info['environment']}"
                    )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"✓ Found {len(account_list_orm)} account(s) in database")
            
            # Vérification des comptes exclus (mode BY_NAMES_WITH_FILTERS uniquement)
            if mode == ExtractionMode.BY_NAMES_WITH_FILTERS:
                requested_names = set(mode_info["account_names"])
                found_names = {acc.account_name for acc in account_list_orm}
                excluded_names = requested_names - found_names
                
                if excluded_names:
                    logger.warning("=" * 80)
                    logger.warning(f"⚠ FILTER VALIDATION: {len(excluded_names)} account(s) EXCLUDED")
                    logger.warning(f"   Reason: Do not match filter criteria")
                    logger.warning(f"   Filters: orchestrator={payload.accounts_orchestrator}, environment={payload.accounts_environment}")
                    logger.warning(f"   Excluded accounts:")
                    for name in sorted(excluded_names):
                        logger.warning(f"     - {name}")
                    logger.warning(f"   Input: {len(requested_names)} account(s)")
                    logger.warning(f"   Valid: {len(found_names)} account(s)")
                    logger.warning(f"   Excluded: {len(excluded_names)} account(s)")
                    logger.warning("=" * 80)
                    
                    logger.info(f"Proceeding with {len(found_names)} account(s) that match filters")
            
            # Vérification des comptes manquants en DB (mode BY_NAMES uniquement)
            elif mode == ExtractionMode.BY_NAMES:
                found_names = {acc.account_name for acc in account_list_orm}
                missing_names = set(mode_info["account_names"]) - found_names
                
                if missing_names:
                    logger.warning(
                        f"⚠ WARNING: {len(missing_names)} account(s) not found in database:"
                    )
                    for name in sorted(missing_names):
                        logger.warning(f"   - {name}")
            
            # Conversion en dict pour faciliter l'expand
            account_list = []
            for account in account_list_orm:
                account_list.append({
                    "account_id": account.account_id,
                    "account_number": account.account_number,
                    "account_name": account.account_name,
                    "account_type": account.account_type,
                    "account_env": account.account_env,
                })
            
            # Breakdown (mode BY_FILTERS ou BY_NAMES_WITH_FILTERS)
            if mode in [ExtractionMode.BY_FILTERS, ExtractionMode.BY_NAMES_WITH_FILTERS]:
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
    # STEP 3: Création du répertoire temporaire
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
        logger.info("📁 Step 2: Creating temporary directory")
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
    
    # ========================================================================
    # STEP 4: Extraction IAM PARALLÈLE avec gestion des erreurs
    # ========================================================================
    
    @step
    def extract_iam_for_account(
        account: Dict[str, Any],
        directory_path: str,
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> Dict[str, Any]:
        """
        Extrait les données IAM pour UN SEUL compte.
        
        IMPORTANT: En cas d'erreur, cette fonction lève AirflowFailException
        qui marque la task comme FAILED dans l'UI mais permet aux autres
        mapped tasks de continuer leur exécution.
        
        Args:
            account: Dictionnaire contenant les infos du compte
            directory_path: Chemin du répertoire de sortie
            payload: Payload validé
            vault: Service Vault pour l'API key
        
        Returns:
            dict: Résultat de l'extraction (status="success" uniquement)
        
        Raises:
            AirflowFailException: En cas d'erreur (n'arrête pas les autres tasks)
        """
        account_name = account["account_name"]
        account_number = account["account_number"]
        account_type = account["account_type"]
        account_env = account["account_env"]
        
        logger.info("=" * 80)
        logger.info(f"🔄 Extracting IAM for: {account_name}")
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
            logger.error(f"   This task will be marked as FAILED")
            logger.error(f"   Other accounts will continue processing")
            logger.info("=" * 80)
            
            # IMPORTANT: AirflowFailException marque la task comme FAILED
            # mais permet aux autres mapped tasks de continuer
            raise AirflowFailException(
                f"Failed to extract IAM for {account_name}: {error_msg}"
            )
    
    # ========================================================================
    # STEP 5: Agrégation et compression (avec trigger_rule spécial)
    # ========================================================================
    
    @step(trigger_rule='none_failed_min_one_success')
    def zip_all_results(
        extraction_results: List[Dict[str, Any]],
        directory_path: str,
        mode_info: Dict[str, Any],
        **kwargs
    ) -> Dict[str, Any]:
        """
        Agrège les résultats de toutes les extractions et compresse.
        
        IMPORTANT: Cette task utilise trigger_rule='none_failed_min_one_success'
        pour s'exécuter même si certaines mapped tasks ont échoué.
        
        Elle récupère les résultats via XCom pour gérer les tasks failed.
        
        Args:
            extraction_results: Liste des résultats (uniquement les succès via expand)
            directory_path: Chemin du répertoire contenant les fichiers
            mode_info: Informations sur le mode d'extraction
            kwargs: Contient 'ti' pour accéder aux XComs
        
        Returns:
            dict: Résumé complet de l'extraction + chemin du ZIP
        """
        logger.info("=" * 80)
        logger.info("📦 Step 4: Compressing results")
        logger.info("=" * 80)
        
        ti = kwargs.get('ti')
        
        # Récupération de TOUS les résultats (succès + échecs) via XCom
        # Les tasks failed n'ont pas de XCom, donc on compte manuellement
        
        # Nombre total de mapped tasks lancées
        # On récupère depuis get_accounts_list
        accounts_xcom = ti.xcom_pull(task_ids='get_accounts_list')
        total_accounts = len(accounts_xcom) if accounts_xcom else len(extraction_results)
        
        # Les résultats reçus sont UNIQUEMENT les succès (tasks qui n'ont pas raise)
        successful_results = extraction_results
        successful = len(successful_results)
        
        # Calcul des échecs par différence
        failed = total_accounts - successful
        
        success_rate = (successful / total_accounts * 100) if total_accounts > 0 else 0
        
        logger.info(f"Extraction statistics:")
        logger.info(f"   Total accounts: {total_accounts}")
        logger.info(f"   Successful: {successful}")
        logger.info(f"   Failed: {failed}")
        logger.info(f"   Success rate: {success_rate:.1f}%")
        
        # Log des comptes en échec (on ne connait pas les noms, juste le nombre)
        if failed > 0:
            logger.warning(f"⚠ {failed} account(s) failed (check individual task logs)")
        
        # Vérification qu'il y a au moins 1 succès
        if successful == 0:
            error_msg = "All extractions failed. No files to compress."
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Breakdown par type/env (mode BY_FILTERS ou BY_NAMES_WITH_FILTERS)
        mode = ExtractionMode(mode_info["mode"])
        breakdown = {}
        
        if mode in [ExtractionMode.BY_FILTERS, ExtractionMode.BY_NAMES_WITH_FILTERS]:
            orch_counts = {}
            env_counts = {}
            
            for result in successful_results:
                orch = result["account_type"]
                env = result["account_env"]
                orch_counts[orch] = orch_counts.get(orch, 0) + 1
                env_counts[env] = env_counts.get(env, 0) + 1
            
            breakdown = {
                "orchestrator": orch_counts,
                "environment": env_counts
            }
            
            logger.info(f"   Successful by orchestrator: {orch_counts}")
            logger.info(f"   Successful by environment: {env_counts}")
        
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
        
        if failed > 0:
            logger.warning(f"⚠ {failed} account(s) were skipped due to errors")
        
        logger.info("=" * 80)
        
        # Résumé complet
        summary = {
            "mode": mode_info["mode"],
            "total_accounts": total_accounts,
            "successful_extractions": successful,
            "failed_extractions": failed,
            "success_rate": round(success_rate, 2),
            "successful_results": successful_results,
            "breakdown": breakdown,
            "zip_path": zip_path,
            "zip_size_mb": round(zip_size_mb, 2),
            "output_directory": directory_path
        }
        
        return summary
    
    # ========================================================================
    # STEP 6: Upload vers COS
    # ========================================================================
    
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
        logger.info("☁️  Step 5: Uploading to Cloud Object Storage")
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
        failed_count = compression_summary["failed_extractions"]
        
        final_summary = {
            "extraction_mode": compression_summary["mode"],
            "total_accounts": compression_summary["total_accounts"],
            "successful_extractions": compression_summary["successful_extractions"],
            "failed_extractions": failed_count,
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
        
        if failed_count > 0:
            logger.warning(f"⚠ {failed_count} account(s) failed (check individual task logs)")
        
        logger.info(f"COS URL: {cos_url}")
        logger.info("=" * 80)
        
        return final_summary
    
    # ========================================================================
    # DÉFINITION DU WORKFLOW (avec parallélisation et tolérance aux pannes)
    # ========================================================================
    
    # Step 1: Détection du mode
    mode_info = detect_extraction_mode()
    
    # Step 2: Récupération des comptes (query conditionnelle)
    accounts = get_accounts_list(mode_info)
    
    # Step 3: Création du répertoire temporaire
    directory = create_tmp_directory()
    
    # Step 4: Extraction PARALLÈLE (une task par compte)
    # IMPORTANT: extract_iam_for_account lève AirflowFailException en cas d'erreur
    # Cela marque la task comme FAILED mais permet aux autres de continuer
    
    # Configuration du parallélisme via max_active_tis_per_dagrun
    extraction_results = extract_iam_for_account.expand(
        account=accounts
    ).partial(
        directory_path=directory,
        max_active_tis_per_dagrun=10  # Max 10 extractions en parallèle simultanément
    )
    
    # Step 5: Compression des résultats
    # IMPORTANT: trigger_rule='none_failed_min_one_success'
    # S'exécute si au moins 1 extraction a réussi
    compression_summary = zip_all_results(
        extraction_results,
        directory,
        mode_info
    )
    
    # Step 6: Upload vers COS
    final_summary = upload_to_cos(compression_summary)
    
    # Définition des dépendances
    mode_info >> accounts >> directory >> extraction_results >> compression_summary >> final_summary


# ============================================================================
# Exécution du DAG
# ============================================================================

dag_extract_iam_create()