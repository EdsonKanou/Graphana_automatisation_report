import logging
from typing import Any
from sqlalchemy.orm import Session as SASession
from account.database_models import AccountExtractIAM
from account.services.extract_iam_account import TMP_DIRECTORY_PATH

logger = logging.getLogger(__name__)


@task_group
def extract_iam_to_excel_task_group(arg, **kwargs) -> None:
    
    @step
    def get_account_map_list(
        payload: AccountsExtractIamPayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> list[dict[str, Any]]:
        """
        Récupère la liste des comptes AWS à extraire selon les filtres du payload.
        
        Args:
            payload: Payload validé contenant orchestrator et environment
            db_session: Session SQLAlchemy pour les requêtes DB
            vault: Service Vault pour récupérer les API keys
        
        Returns:
            list[dict]: Liste des comptes avec leurs métadonnées
            
        Raises:
            ValueError: Si aucun compte ne correspond aux critères
        """
        logger.info("=" * 80)
        logger.info("Starting account extraction process")
        logger.info("=" * 80)
        
        # Log du payload reçu
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
            
            # Log de la query SQL générée (debugging)
            logger.debug(f"SQL Query: {query}")
            
            # Exécution de la query
            account_list: list[AccountExtractIAM] = query.all()
            
            # Vérification des résultats
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
        
        # Génération du chemin de stockage
        directory_path = generate_directory_path(
            payload.subscription_id,
            TMP_DIRECTORY_PATH
        )
        logger.info(f"Files will be stored in: {directory_path}")
        
        # Construction de la liste des comptes avec leurs métadonnées
        account_map_list = []
        
        for idx, account in enumerate(account_list, start=1):
            try:
                logger.info(f"Processing account {idx}/{len(account_list)}: {account.account_name}")
                
                # Récupération de l'API key depuis Vault
                apikey = get_account_api_key(
                    account_number=account.account_number,
                    vault=vault
                )
                
                account_info = {
                    "api_key": apikey,
                    "account_id": account.account_id,
                    "account_number": account.account_number,
                    "account_name": account.account_name,
                    "account_type": account.account_type,  # PAASV4 ou DMZRC
                    "account_env": account.account_env,    # NPR ou PRD
                    "directory_path": directory_path
                }
                
                account_map_list.append(account_info)
                
                logger.debug(f"  ✓ Account {account.account_number} processed successfully")
                
            except Exception as e:
                logger.error(
                    f"  ✗ Failed to process account {account.account_number}: {str(e)}"
                )
                # Option: continuer ou lever l'erreur selon ton besoin
                raise  # On lève l'erreur pour arrêter le process
        
        # Logs de summary final
        logger.info("=" * 80)
        logger.info("Account extraction completed successfully")
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