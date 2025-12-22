import logging
from typing import Any
from sqlalchemy.orm import Session as SASession
from account.database_models import AccountExtractIAM
from account.services.extract_iam_account import TMP_DIRECTORY_PATH

logger = logging.getLogger(__name__)


# ============================================================================
# DAG 1: Extraction par noms de comptes (BY_NAMES)
# ============================================================================

def dag_extract_iam_by_account_create() -> None:
    """
    DAG pour extraire les IAM de comptes spécifiques par leurs noms.
    
    Payload attendu:
        {"account_names": ["ac0021000259", "ac0021000260"]}
    """
    
    @task_group
    def extract_iam_by_names_task_group(arg, **kwargs) -> None:
        
        @step
        def get_account_map_list_by_names(
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
            vault: Vault = depends(vault_dependency),
        ) -> list[dict[str, Any]]:
            """
            Récupère les comptes AWS par leurs noms (mode BY_NAMES).
            
            Args:
                payload: Payload avec account_names
                db_session: Session SQLAlchemy
                vault: Service Vault pour les API keys
            
            Returns:
                list[dict]: Liste des comptes avec leurs métadonnées
            
            Raises:
                ValueError: Si payload n'est pas en mode BY_NAMES
                ValueError: Si aucun compte trouvé
            """
            logger.info("=" * 80)
            logger.info("DAG 1: Extract IAM by account names")
            logger.info("=" * 80)
            
            # Vérification du mode
            if not payload.is_by_names_mode():
                error_msg = (
                    "This DAG requires BY_NAMES mode. "
                    "Please provide 'account_names' in the payload."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Récupération des noms de comptes
            account_names = payload.get_account_names()
            logger.info(f"Searching for {len(account_names)} account(s) by name:")
            for name in account_names:
                logger.info(f"  - {name}")
            
            # Query SQL: filtrage par noms
            try:
                query = db_session.query(AccountExtractIAM).filter(
                    AccountExtractIAM.account_name.in_(account_names)
                )
                
                logger.debug(f"SQL Query: {query}")
                
                account_list: list[AccountExtractIAM] = query.all()
                
                # Vérification des résultats
                if not account_list:
                    error_msg = f"No accounts found with names: {account_names}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                
                logger.info(f"✓ Found {len(account_list)} account(s)")
                
                # Vérification des comptes manquants
                found_names = {acc.account_name for acc in account_list}
                missing_names = set(account_names) - found_names
                
                if missing_names:
                    logger.warning(
                        f"⚠ WARNING: {len(missing_names)} account(s) not found in database:"
                    )
                    for name in missing_names:
                        logger.warning(f"  - {name}")
                
            except Exception as e:
                logger.error(f"Database query failed: {str(e)}")
                raise
            
            # Génération du chemin de stockage
            directory_path = generate_directory_path(
                payload.subscription_id,
                TMP_DIRECTORY_PATH
            )
            logger.info(f"Files will be stored in: {directory_path}")
            
            # Construction de la liste des comptes
            account_map_list = []
            
            for idx, account in enumerate(account_list, start=1):
                try:
                    logger.info(
                        f"Processing account {idx}/{len(account_list)}: "
                        f"{account.account_name} ({account.account_type}/{account.account_env})"
                    )
                    
                    apikey = get_account_api_key(
                        account_number=account.account_number,
                        vault=vault
                    )
                    
                    account_info = {
                        "api_key": apikey,
                        "account_id": account.account_id,
                        "account_number": account.account_number,
                        "account_name": account.account_name,
                        "account_type": account.account_type,
                        "account_env": account.account_env,
                        "directory_path": directory_path
                    }
                    
                    account_map_list.append(account_info)
                    logger.debug(f"  ✓ Account {account.account_number} processed")
                    
                except Exception as e:
                    logger.error(f"  ✗ Failed to process {account.account_number}: {e}")
                    raise
            
            # Summary
            logger.info("=" * 80)
            logger.info(f"Extraction completed - {len(account_map_list)} account(s) processed")
            logger.info("=" * 80)
            
            return account_map_list


# ============================================================================
# DAG 2: Extraction par filtres orchestrator/environment (BY_FILTERS)
# ============================================================================

def dag_extract_iam_by_account_filter_create() -> None:
    """
    DAG pour extraire les IAM de comptes par filtres.
    
    Payloads attendus:
        {} → ALL + ALL
        {"accounts_orchestrator": "PAASV4"} → PAASV4 + ALL
        {"accounts_environment": "PRD"} → ALL + PRD
        {"accounts_orchestrator": "DMZRC", "accounts_environment": "NPR"}
    """
    
    @task_group
    def extract_iam_by_filters_task_group(arg, **kwargs) -> None:
        
        @step
        def get_account_map_list_by_filters(
            payload: UnifiedExtractIamPayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
            vault: Vault = depends(vault_dependency),
        ) -> list[dict[str, Any]]:
            """
            Récupère les comptes AWS par filtres (mode BY_FILTERS).
            
            Args:
                payload: Payload avec accounts_orchestrator et/ou accounts_environment
                db_session: Session SQLAlchemy
                vault: Service Vault pour les API keys
            
            Returns:
                list[dict]: Liste des comptes avec leurs métadonnées
            
            Raises:
                ValueError: Si payload est en mode BY_NAMES
                ValueError: Si aucun compte trouvé
            """
            logger.info("=" * 80)
            logger.info("DAG 2: Extract IAM by filters")
            logger.info("=" * 80)
            
            # Vérification du mode
            if not payload.is_by_filters_mode():
                error_msg = (
                    "This DAG requires BY_FILTERS mode. "
                    "Payload contains 'account_names' which takes priority. "
                    "Remove 'account_names' or use DAG 1 instead."
                )
                logger.error(error_msg)
                raise ValueError(error_msg)
            
            # Log du payload
            logger.info(f"Payload received:")
            logger.info(f"  - accounts_orchestrator: {payload.accounts_orchestrator}")
            logger.info(f"  - accounts_environment: {payload.accounts_environment}")
            logger.info(f"  - Extraction scope: {payload._get_extraction_scope()}")
            
            # Récupération des filtres
            orchestrator_values = payload.get_orchestrator_filter_values()
            environment_values = payload.get_environment_filter_values()
            
            logger.info(f"Applied filters:")
            logger.info(f"  - account_type IN {orchestrator_values}")
            logger.info(f"  - account_env IN {environment_values}")
            
            # Construction de la query dynamique
            try:
                query = db_session.query(AccountExtractIAM)
                
                # Filtre sur account_type (orchestrator)
                query = query.filter(AccountExtractIAM.account_type.in_(orchestrator_values))
                
                # Filtre sur account_env (environment)
                query = query.filter(AccountExtractIAM.account_env.in_(environment_values))
                
                logger.debug(f"SQL Query: {query}")
                
                # Exécution
                account_list: list[AccountExtractIAM] = query.all()
                
                # Vérification
                if not account_list:
                    error_msg = (
                        f"No accounts found matching criteria: "
                        f"orchestrator={payload.accounts_orchestrator}, "
                        f"environment={payload.accounts_environment}"
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                
                logger.info(f"✓ Found {len(account_list)} account(s)")
                
            except Exception as e:
                logger.error(f"Database query failed: {str(e)}")
                raise
            
            # Génération du chemin de stockage
            directory_path = generate_directory_path(
                payload.subscription_id,
                TMP_DIRECTORY_PATH
            )
            logger.info(f"Files will be stored in: {directory_path}")
            
            # Construction de la liste
            account_map_list = []
            
            for idx, account in enumerate(account_list, start=1):
                try:
                    logger.info(
                        f"Processing account {idx}/{len(account_list)}: "
                        f"{account.account_name} ({account.account_type}/{account.account_env})"
                    )
                    
                    apikey = get_account_api_key(
                        account_number=account.account_number,
                        vault=vault
                    )
                    
                    account_info = {
                        "api_key": apikey,
                        "account_id": account.account_id,
                        "account_number": account.account_number,
                        "account_name": account.account_name,
                        "account_type": account.account_type,
                        "account_env": account.account_env,
                        "directory_path": directory_path
                    }
                    
                    account_map_list.append(account_info)
                    logger.debug(f"  ✓ Account {account.account_number} processed")
                    
                except Exception as e:
                    logger.error(f"  ✗ Failed to process {account.account_number}: {e}")
                    raise
            
            # Summary final
            logger.info("=" * 80)
            logger.info("Extraction completed successfully")
            logger.info(f"Total accounts processed: {len(account_map_list)}")
            
            # Décompte par orchestrator
            orchestrator_counts = {}
            for account in account_map_list:
                orch = account["account_type"]
                orchestrator_counts[orch] = orchestrator_counts.get(orch, 0) + 1
            
            logger.info(f"Breakdown by orchestrator:")
            for orch, count in orchestrator_counts.items():
                logger.info(f"  - {orch}: {count} account(s)")
            
            # Décompte par environment
            env_counts = {}
            for account in account_map_list:
                env = account["account_env"]
                env_counts[env] = env_counts.get(env, 0) + 1
            
            logger.info(f"Breakdown by environment:")
            for env, count in env_counts.items():
                logger.info(f"  - {env}: {count} account(s)")
            
            logger.info("=" * 80)
            
            return account_map_list