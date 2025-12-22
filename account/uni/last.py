"""
DAGs pour l'extraction IAM des comptes AWS.

Ce fichier contient 2 DAGs distincts :
1. dag_extract_iam_by_account : Extraction par liste de noms de comptes
2. dag_extract_iam_by_filter : Extraction par filtres orchestrator/environment

Les deux utilisent le même payload unifié (UnifiedExtractIamPayload).
"""

import logging
from pathlib import Path
from typing import Any
from sqlalchemy.orm import Session as SASession

from bpzi_airflow_library.decorators import product_action, step
from bpzi_airflow_library.dependencies import depends
from account.database_models import AccountExtractIAM
from account.services.extract_iam_account import TMP_DIRECTORY_PATH
from account.schemas.unified_extract_iam_payload import UnifiedExtractIamPayload

logger = logging.getLogger(__name__)


# ============================================================================
# DAG 1: Extraction par noms de comptes (BY_NAMES)
# ============================================================================

@product_action(
    action_id="dag_extract_iam_by_account",
    payload=UnifiedExtractIamPayload,
    tags=["account", "iam", "by_names"],
    doc_md="""
    # DAG Extract IAM by Account Names
    
    Extract IAM data for specific AWS accounts by their names.
    
    ## Required Payload
    ```json
    {
        "account_names": ["ac0021000259", "ac0021000260"]
    }
    ```
    
    ## Notes
    - This DAG uses BY_NAMES mode
    - If `accounts_orchestrator` or `accounts_environment` are provided, they are IGNORED
    - `account_names` takes priority over any filter parameters
    """
)
def dag_extract_iam_by_account_create() -> None:
    """
    DAG pour extraire les IAM de comptes spécifiques par leurs noms.
    
    Workflow:
    1. Validation du mode BY_NAMES
    2. Vérification de l'existence des comptes
    3. Création du répertoire temporaire
    4. Extraction des IAM vers Excel
    5. Compression du répertoire
    6. Upload vers COS
    """
    
    @step
    def check_account_names(
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> dict[str, Any]:
        """
        Vérifie que le payload est en mode BY_NAMES et que les comptes existent.
        
        Returns:
            dict: Métadonnées de validation
        
        Raises:
            ValueError: Si mode incorrect ou comptes introuvables
        """
        logger.info("=" * 80)
        logger.info("DAG 1: Extract IAM by Account Names - Starting validation")
        logger.info("=" * 80)
        
        # Vérification du mode
        if not payload.is_by_names_mode():
            error_msg = (
                "This DAG requires BY_NAMES mode. "
                "Please provide 'account_names' in the payload. "
                "Example: {\"account_names\": [\"ac0021000259\"]}"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        account_names = payload.get_account_names()
        logger.info(f"Validating {len(account_names)} account name(s):")
        for name in account_names:
            logger.info(f"  - {name}")
        
        # Vérification de l'existence des comptes en base
        try:
            query = db_session.query(AccountExtractIAM).filter(
                AccountExtractIAM.account_name.in_(account_names)
            )
            
            found_accounts = query.all()
            found_names = {acc.account_name for acc in found_accounts}
            
            if not found_accounts:
                error_msg = f"No accounts found with names: {account_names}"
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"✓ Found {len(found_accounts)} account(s) in database")
            
            # Vérification des comptes manquants
            missing_names = set(account_names) - found_names
            if missing_names:
                logger.warning(f"⚠ WARNING: {len(missing_names)} account(s) not found:")
                for name in missing_names:
                    logger.warning(f"  - {name}")
            
        except Exception as e:
            logger.error(f"Account validation failed: {str(e)}")
            raise
        
        validation_result = {
            "mode": "BY_NAMES",
            "requested_count": len(account_names),
            "found_count": len(found_accounts),
            "missing_count": len(missing_names),
            "account_names": account_names
        }
        
        logger.info("✓ Account validation completed successfully")
        logger.info("=" * 80)
        
        return validation_result
    
    @step
    def create_tmp_directory(
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
    ) -> str:
        """
        Crée le répertoire temporaire pour stocker les fichiers Excel.
        
        Returns:
            str: Chemin du répertoire créé
        """
        logger.info("Creating temporary directory...")
        
        directory_path = generate_directory_path(
            payload.subscription_id,
            TMP_DIRECTORY_PATH
        )
        
        # Création du répertoire s'il n'existe pas
        Path(directory_path).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"✓ Directory created: {directory_path}")
        return directory_path
    
    @step
    def extract_iam_to_excel(
        directory_path: str,
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> dict[str, Any]:
        """
        Extrait les IAM pour chaque compte et génère les fichiers Excel.
        
        Args:
            directory_path: Chemin du répertoire de destination
        
        Returns:
            dict: Résumé de l'extraction
        """
        logger.info("=" * 80)
        logger.info("Starting IAM extraction to Excel")
        logger.info("=" * 80)
        
        # Récupération des comptes
        account_names = payload.get_account_names()
        
        query = db_session.query(AccountExtractIAM).filter(
            AccountExtractIAM.account_name.in_(account_names)
        )
        account_list = query.all()
        
        logger.info(f"Processing {len(account_list)} account(s)")
        
        extraction_results = []
        
        for idx, account in enumerate(account_list, start=1):
            try:
                logger.info(
                    f"[{idx}/{len(account_list)}] Extracting IAM for: "
                    f"{account.account_name} ({account.account_type}/{account.account_env})"
                )
                
                # Récupération de l'API key
                apikey = get_account_api_key(
                    account_number=account.account_number,
                    vault=vault
                )
                
                # Extraction IAM (appel à ton service d'extraction)
                excel_file_path = extract_iam_data_to_excel(
                    account=account,
                    api_key=apikey,
                    output_directory=directory_path
                )
                
                extraction_results.append({
                    "account_name": account.account_name,
                    "account_number": account.account_number,
                    "file_path": excel_file_path,
                    "status": "success"
                })
                
                logger.info(f"  ✓ Extraction completed: {excel_file_path}")
                
            except Exception as e:
                logger.error(f"  ✗ Extraction failed for {account.account_name}: {e}")
                extraction_results.append({
                    "account_name": account.account_name,
                    "account_number": account.account_number,
                    "status": "failed",
                    "error": str(e)
                })
                # Option: continuer ou stopper selon ton besoin
                raise
        
        summary = {
            "total_accounts": len(account_list),
            "successful_extractions": len([r for r in extraction_results if r["status"] == "success"]),
            "failed_extractions": len([r for r in extraction_results if r["status"] == "failed"]),
            "results": extraction_results
        }
        
        logger.info("=" * 80)
        logger.info(f"Extraction completed: {summary['successful_extractions']}/{summary['total_accounts']} successful")
        logger.info("=" * 80)
        
        return summary
    
    @step
    def zip_generated_directory(
        directory_path: str,
    ) -> str:
        """
        Compresse le répertoire contenant les fichiers Excel.
        
        Args:
            directory_path: Chemin du répertoire à compresser
        
        Returns:
            str: Chemin du fichier ZIP créé
        """
        logger.info(f"Compressing directory: {directory_path}")
        
        zip_path = f"{directory_path}.zip"
        
        import shutil
        shutil.make_archive(directory_path, 'zip', directory_path)
        
        logger.info(f"✓ ZIP created: {zip_path}")
        return zip_path
    
    @step
    def upload_to_cos_task(
        zip_path: str,
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
    ) -> dict[str, str]:
        """
        Upload le fichier ZIP vers Cloud Object Storage (COS).
        
        Args:
            zip_path: Chemin du fichier ZIP à uploader
        
        Returns:
            dict: Informations sur l'upload (URL, etc.)
        """
        logger.info(f"Uploading to COS: {zip_path}")
        
        # Appel à ton service d'upload COS
        cos_url = upload_file_to_cos(
            file_path=zip_path,
            subscription_id=payload.subscription_id
        )
        
        logger.info(f"✓ Upload completed: {cos_url}")
        
        return {
            "zip_path": zip_path,
            "cos_url": cos_url,
            "upload_status": "success"
        }
    
    # ========================================================================
    # Main Task Flow (DAG 1)
    # ========================================================================
    
    validation_result = check_account_names()
    directory_path = create_tmp_directory()
    extract_iam = extract_iam_to_excel(directory_path)
    zip_path = zip_generated_directory(directory_path)
    upload_results = upload_to_cos_task(zip_path)
    
    # Dependencies
    validation_result >> directory_path >> extract_iam >> zip_path >> upload_results


# ============================================================================
# DAG 2: Extraction par filtres orchestrator/environment (BY_FILTERS)
# ============================================================================

@product_action(
    action_id="dag_extract_iam_by_filter",
    payload=UnifiedExtractIamPayload,
    tags=["account", "iam", "by_filters"],
    doc_md="""
    # DAG Extract IAM by Filters
    
    Extract IAM data for AWS accounts filtered by orchestrator and/or environment.
    
    ## Payload Examples
    
    ### All accounts (default)
    ```json
    {}
    ```
    
    ### Filter by orchestrator
    ```json
    {
        "accounts_orchestrator": "PAASV4"
    }
    ```
    
    ### Filter by environment
    ```json
    {
        "accounts_environment": "PRD"
    }
    ```
    
    ### Both filters
    ```json
    {
        "accounts_orchestrator": "DMZRC",
        "accounts_environment": "NPR"
    }
    ```
    
    ## Notes
    - This DAG uses BY_FILTERS mode
    - If `account_names` is provided, this DAG will reject the payload
    - Default values: orchestrator=ALL, environment=ALL
    """
)
def dag_extract_iam_by_filter_create() -> None:
    """
    DAG pour extraire les IAM de comptes par filtres.
    
    Workflow:
    1. Validation du mode BY_FILTERS
    2. Récupération des comptes selon les filtres
    3. Création du répertoire temporaire
    4. Extraction des IAM vers Excel
    5. Compression du répertoire
    6. Upload vers COS
    """
    
    @step
    def validate_filter_mode(
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
    ) -> dict[str, Any]:
        """
        Vérifie que le payload est en mode BY_FILTERS.
        
        Returns:
            dict: Métadonnées de validation
        
        Raises:
            ValueError: Si mode incorrect
        """
        logger.info("=" * 80)
        logger.info("DAG 2: Extract IAM by Filters - Starting validation")
        logger.info("=" * 80)
        
        # Vérification du mode
        if not payload.is_by_filters_mode():
            error_msg = (
                "This DAG requires BY_FILTERS mode. "
                "Your payload contains 'account_names' which takes priority. "
                "Please either:\n"
                "  1. Remove 'account_names' from the payload, OR\n"
                "  2. Use 'dag_extract_iam_by_account' instead"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        logger.info(f"Payload validated:")
        logger.info(f"  - accounts_orchestrator: {payload.accounts_orchestrator}")
        logger.info(f"  - accounts_environment: {payload.accounts_environment}")
        logger.info(f"  - Extraction scope: {payload._get_extraction_scope()}")
        
        if payload.is_full_extraction():
            logger.warning("⚠ WARNING: Full extraction (ALL + ALL) - may process many accounts")
        
        validation_result = {
            "mode": "BY_FILTERS",
            "orchestrator": payload.accounts_orchestrator,
            "environment": payload.accounts_environment,
            "is_full_extraction": payload.is_full_extraction()
        }
        
        logger.info("✓ Mode validation completed successfully")
        logger.info("=" * 80)
        
        return validation_result
    
    @step
    def create_tmp_directory(
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
    ) -> str:
        """Crée le répertoire temporaire."""
        logger.info("Creating temporary directory...")
        
        directory_path = generate_directory_path(
            payload.subscription_id,
            TMP_DIRECTORY_PATH
        )
        
        Path(directory_path).mkdir(parents=True, exist_ok=True)
        
        logger.info(f"✓ Directory created: {directory_path}")
        return directory_path
    
    @step
    def extract_iam_to_excel(
        directory_path: str,
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> dict[str, Any]:
        """
        Extrait les IAM pour les comptes filtrés.
        
        Args:
            directory_path: Chemin du répertoire de destination
        
        Returns:
            dict: Résumé de l'extraction
        """
        logger.info("=" * 80)
        logger.info("Starting IAM extraction to Excel")
        logger.info("=" * 80)
        
        # Récupération des filtres
        orchestrator_values = payload.get_orchestrator_filter_values()
        environment_values = payload.get_environment_filter_values()
        
        logger.info(f"Applying filters:")
        logger.info(f"  - account_type IN {orchestrator_values}")
        logger.info(f"  - account_env IN {environment_values}")
        
        # Query avec filtres dynamiques
        try:
            query = db_session.query(AccountExtractIAM)
            query = query.filter(AccountExtractIAM.account_type.in_(orchestrator_values))
            query = query.filter(AccountExtractIAM.account_env.in_(environment_values))
            
            account_list = query.all()
            
            if not account_list:
                error_msg = (
                    f"No accounts found matching criteria: "
                    f"orchestrator={payload.accounts_orchestrator}, "
                    f"environment={payload.accounts_environment}"
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            logger.info(f"✓ Found {len(account_list)} account(s) matching criteria")
            
        except Exception as e:
            logger.error(f"Database query failed: {str(e)}")
            raise
        
        # Extraction pour chaque compte
        extraction_results = []
        
        for idx, account in enumerate(account_list, start=1):
            try:
                logger.info(
                    f"[{idx}/{len(account_list)}] Extracting IAM for: "
                    f"{account.account_name} ({account.account_type}/{account.account_env})"
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
                    "account_number": account.account_number,
                    "account_type": account.account_type,
                    "account_env": account.account_env,
                    "file_path": excel_file_path,
                    "status": "success"
                })
                
                logger.info(f"  ✓ Extraction completed: {excel_file_path}")
                
            except Exception as e:
                logger.error(f"  ✗ Extraction failed for {account.account_name}: {e}")
                extraction_results.append({
                    "account_name": account.account_name,
                    "account_number": account.account_number,
                    "status": "failed",
                    "error": str(e)
                })
                raise
        
        # Summary avec breakdown
        summary = {
            "total_accounts": len(account_list),
            "successful_extractions": len([r for r in extraction_results if r["status"] == "success"]),
            "failed_extractions": len([r for r in extraction_results if r["status"] == "failed"]),
            "results": extraction_results
        }
        
        # Breakdown par orchestrator
        orchestrator_counts = {}
        for result in extraction_results:
            if result["status"] == "success":
                orch = result.get("account_type", "unknown")
                orchestrator_counts[orch] = orchestrator_counts.get(orch, 0) + 1
        
        summary["orchestrator_breakdown"] = orchestrator_counts
        
        # Breakdown par environment
        env_counts = {}
        for result in extraction_results:
            if result["status"] == "success":
                env = result.get("account_env", "unknown")
                env_counts[env] = env_counts.get(env, 0) + 1
        
        summary["environment_breakdown"] = env_counts
        
        logger.info("=" * 80)
        logger.info(f"Extraction completed: {summary['successful_extractions']}/{summary['total_accounts']} successful")
        logger.info(f"Orchestrator breakdown: {orchestrator_counts}")
        logger.info(f"Environment breakdown: {env_counts}")
        logger.info("=" * 80)
        
        return summary
    
    @step
    def zip_generated_directory(
        directory_path: str,
    ) -> str:
        """Compresse le répertoire."""
        logger.info(f"Compressing directory: {directory_path}")
        
        zip_path = f"{directory_path}.zip"
        
        import shutil
        shutil.make_archive(directory_path, 'zip', directory_path)
        
        logger.info(f"✓ ZIP created: {zip_path}")
        return zip_path
    
    @step
    def upload_to_cos_task(
        zip_path: str,
        payload: UnifiedExtractIamPayload = depends(payload_dependency),
    ) -> dict[str, str]:
        """Upload vers COS."""
        logger.info(f"Uploading to COS: {zip_path}")
        
        cos_url = upload_file_to_cos(
            file_path=zip_path,
            subscription_id=payload.subscription_id
        )
        
        logger.info(f"✓ Upload completed: {cos_url}")
        
        return {
            "zip_path": zip_path,
            "cos_url": cos_url,
            "upload_status": "success"
        }
    
    # ========================================================================
    # Main Task Flow (DAG 2)
    # ========================================================================
    
    validation_result = validate_filter_mode()
    directory_path = create_tmp_directory()
    extract_iam = extract_iam_to_excel(directory_path)
    zip_path = zip_generated_directory(directory_path)
    upload_results = upload_to_cos_task(zip_path)
    
    # Dependencies
    validation_result >> directory_path >> extract_iam >> zip_path >> upload_results


# ============================================================================
# Exécution des DAGs
# ============================================================================

# DAG 1: Extraction par noms de comptes
dag_extract_iam_by_account_create()

# DAG 2: Extraction par filtres
dag_extract_iam_by_filter_create()