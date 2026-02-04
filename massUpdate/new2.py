@product_action(
    action_id=Path(__file__).stem,
    payload=AccountUpdatePayload,
    tags=["account", "admin", "parallel", "update"],
    doc_md="DAG to parallel account update.",
    max_active_tasks=4,
)
def dag_parallel_update() -> None:
    
    @task_group(group_id="validation")
    def validation_group():
        @step
        def check_accounts_exist(
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> bool:
            """Validate that all accounts exist in database"""
            missing_accounts: list[str] = []
            for account_name in payload.account_names:
                account = get_account_by_name(account_name, db_session)
                if account is None:
                    missing_accounts.append(account_name)
            
            if missing_accounts:
                raise DeclineDemandException(
                    f"Accounts not found in database: {', '.join(missing_accounts)}"
                )
            logger.info(f"All {len(payload.account_names)} accounts found in database")
            return True
        
        return check_accounts_exist()

    @task_group(group_id="database_preparation")
    def database_preparation_group():
        """Update accounts configuration in database"""
        
        @step
        def update_accounts_database(
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> List[dict]:
            """Update all accounts configuration in database and return account details"""
            updated_accounts = []
            for account_name in payload.account_names:
                try:
                    account = update_account_config(
                        account_name=account_name,
                        iam_version=payload.iam_version,
                        enable_cbr=payload.enable_cbr,
                        session=db_session
                    )
                    updated_accounts.append({
                        "account_name": account.name,
                        "account_number": account.number,
                        "account_type": account.type,
                        "environment_type": account.environment_type,
                        "code_bu": account.code_bu,
                    })
                    logger.info(f"OK - Updated {account_name} in database")
                except Exception as e:
                    logger.error(f"NO - Error updating {account_name}: {str(e)}")
                    update_deployment_status(
                        account_name=account_name,
                        status="update_failed",
                        session=db_session,
                        error_message=str(e)
                    )
            
            if not updated_accounts:
                raise Exception("No accounts were successfully updated in database")
            logger.info(f"OK - Updated {len(updated_accounts)} accounts in database")
            return updated_accounts
        
        return update_accounts_database()

    @task_group(group_id="vault_and_secrets")
    def vault_and_secrets_group(updated_accounts: List[dict]):
        """Retrieve vault secrets and API keys"""
        
        @step(hide_output=True)
        def check_accounts_in_vault(
            updated_accounts: List[dict],
            vault: Vault = depends(vault_dependency),
        ) -> bool:
            """Verify all accounts have required API keys in Vault"""
            logger.info(f"Checking Vault for {len(updated_accounts)} account API keys...")
            
            missing_api_keys = []
            for account_info in updated_accounts:
                account_number = account_info["account_number"]
                account_name = account_info["account_name"]
                try:
                    api_key_secret = vault.get_secret(
                        f"{account_number}/account-owner",
                        mount_point="ibmsid"
                    )
                    if api_key_secret is None or "api_key" not in api_key_secret:
                        missing_api_keys.append(f"{account_name} ({account_number})")
                        logger.error(f"API key not found for account: {account_name}")
                except Exception as e:
                    logger.error(f"Error accessing Vault for {account_name}: {str(e)}")
                    missing_api_keys.append(f"{account_name} ({account_number})")
            
            if missing_api_keys:
                raise DeclineDemandException(
                    f"The following accounts do not have API keys in Vault: {', '.join(missing_api_keys)}"
                )
            logger.info(f"OK - All accounts have valid API keys in Vault")
            return True

        @step
        def get_buhub_account_details(
            updated_accounts: List[dict],
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> AccountDetails | None:
            """Get BUHUB account details if working with WLAPP accounts"""
            logger.info("Checking if BUHUB account is needed...")
            
            # Check if any account is WLAPP type
            wlapp_accounts = [
                acc for acc in updated_accounts if acc.get("account_type") == "WKLAPP"
            ]
            
            if not wlapp_accounts:
                logger.info("No WKLAPP accounts - BUHUB not needed")
                return None
            
            # Use the first WKLAPP account's code_bu to find BUHUB
            first_wklapp = wlapp_accounts[0]
            code_bu = first_wklapp.get("code_bu")
            logger.info(f"Looking for BUHUB account for code_bu: {code_bu}")
            
            # Get BUHUB account from database
            buhub_account = get_account_by_type_and_bu("BUHUB", code_bu, db_session)
            
            if not buhub_account:
                raise Exception(f"BUHUB account not found for code_bu: {code_bu}")
            
            reader_connector = ReaderConnector()
            buhub_account_details = reader_connector.get_account_details(buhub_account.name)
            logger.info(f"BUHUB account details retrieved")
            return buhub_account_details

        @step(hide_output=True)
        def get_schematics_api_key(
            buhub_account_details: AccountDetails | None,
            vault: Vault = depends(vault_dependency),
        ) -> str:
            """Get API key for Schematics operations"""
            logger.info("Retrieving API key for Schematics...")
            
            # Use GLBHUB account
            logger.info("Using GLBHUB account")
            api_key = vault.get_secret(
                f"{HUB_ACCOUNT_NUMBER}/account-owner",
                mount_point="ibmsid"
            )["api_key"]
            
            logger.info("OK - Retrieved GLBHUB account API key")
            return api_key

        @step(hide_output=True)
        def get_vault_token(
            vault: Vault = depends(vault_dependency),
        ) -> str:
            """Get vault token for Terraform variables"""
            logger.info("Retrieving vault token")
            return vault.vault.token
        
        # Parallelize vault operations
        api_keys_verified = check_accounts_in_vault(updated_accounts)
        buhub_details = get_buhub_account_details(updated_accounts)
        vault_token = get_vault_token()
        
        # Get schematics API key (depends on buhub_details)
        schematics_api_key = get_schematics_api_key(buhub_details)
        
        return {
            "schematics_api_key": schematics_api_key,
            "vault_token": vault_token,
            "updated_accounts": updated_accounts  # On repasse les comptes
        }

    @task_group(group_id="workspace_lookup_parallel")
    def workspace_lookup_group(vault_data: dict):
        """Lookup workspaces in parallel for each account"""
        
        @step(
            map_index_template="{{ account_workspace_index }}"
        )
        def get_workspace_for_single_account(
            account_info: dict,
            schematics_api_key: str,  # Nécessaire pour initialiser le backend
            tf: SchematicsBackend = depends(schematics_backend_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> dict:
            """Get workspace for a single account"""
            from airflow.operators.python import get_current_context
            
            account_name = account_info['account_name']
            
            # Set custom map index
            airflow_context = get_current_context()
            airflow_context["account_workspace_index"] = f"account: {account_name}"
            
            try:
                # Get workspace from database
                workspace_in_db = get_souscription_by_name(account_name, db_session)
                
                if not workspace_in_db:
                    raise ValueError(f"Workspace not found in database for {account_name}")
                
                # Verify workspace status in Schematics
                workspace = tf.workspaces.get_by_id(workspace_in_db.id)
                status = SchematicsWorkspaceStatus(workspace.status)
                logger.info(f"OK - Workspace {workspace_in_db.name} status: {status.value}")
                
                if status.value not in ["ACTIVE", "INACTIVE"]:
                    raise ValueError(
                        f"Workspace {workspace_in_db.name} has invalid status: {status.value}"
                    )
                
                # Combine account info with workspace data
                return {
                    **account_info,
                    "workspace_id": workspace_in_db.id,
                    "workspace_name": workspace_in_db.name,
                    "workspace_status": status.value
                }
                
            except Exception as e:
                logger.error(f"NO - Failed to get workspace for {account_name}: {str(e)}")
                # Update status in database
                update_deployment_status(
                    account_name=account_name,
                    status="workspace_not_found",
                    session=db_session,
                    error_message=str(e)
                )
                raise

        @step
        def prepare_workspace_lookup_data(
            vault_data: dict,
        ) -> List[dict]:
            """Prepare data for parallel workspace lookup"""
            updated_accounts = vault_data["updated_accounts"]
            schematics_api_key = vault_data["schematics_api_key"]
            
            # Create a list with all needed data for each account
            lookup_data = []
            for account_info in updated_accounts:
                lookup_data.append({
                    "account_info": account_info,
                    "schematics_api_key": schematics_api_key
                })
            
            logger.info(f"Prepared {len(lookup_data)} accounts for parallel workspace lookup")
            return lookup_data

        @step
        def filter_valid_workspaces(
            workspace_results: List[dict],
        ) -> List[dict]:
            """Filter out failed workspace lookups and return only valid ones"""
            valid_workspaces = [w for w in workspace_results if w is not None]
            
            if not valid_workspaces:
                raise Exception("No valid workspaces found for any accounts")
            
            logger.info(f"OK - Found {len(valid_workspaces)} valid Schematics workspaces")
            return valid_workspaces
        
        # Prepare data first (this is a regular step, not expanded)
        lookup_data = prepare_workspace_lookup_data(vault_data)
        
        # Then expand on the prepared data
        workspace_lookups = get_workspace_for_single_account.expand_kwargs(
            lookup_data
        )
        
        return filter_valid_workspaces(workspace_lookups)

    @task_group(group_id="parallel_deployment")
    def parallel_deployment_group(
        account_workspaces: List[dict],
        vault_data: dict
    ):
        """Deploy to all accounts in parallel"""
        
        @step(
            map_index_template="{{ deployment_index }}"
        )
        def deploy_single_account(
            account_workspace: dict,
            vault_token: str,
            schematics_api_key: str,  # Nécessaire pour initialiser le backend
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
            tf: SchematicsBackend = depends(schematics_backend_dependency),
        ) -> dict:
            """Deploy to a single account"""
            from airflow.operators.python import get_current_context
            
            account_name = account_workspace["account_name"]
            workspace_id = account_workspace["workspace_id"]
            
            # Set custom map index
            airflow_context = get_current_context()
            airflow_context["deployment_index"] = f"deploying: {account_name}"
            
            try:
                logger.info(f"Starting deployment for account: {account_name}")
                logger.info(f"Workspace ID: {workspace_id}")
                
                # Get updated account configuration from database
                account = get_account_by_name(account_name, db_session)
                
                # Prepare variables for Schematics
                variables = {}
                if payload.iam_version:
                    variables["iam_version"] = account.iam_version
                if payload.enable_cbr is not None:
                    variables["enable_cbr"] = str(account.enable_cbr).lower()
                
                variables["vault_token"] = TerraformVar(vault_token, True)
                
                logger.info(f"Variables to update: {list(variables.keys())}")
                
                # Update workspace variables in Schematics
                tf.workspaces.update_variables(workspace_id, variables)
                logger.info(f"OK - Updated workspace variables for {account_name}")
                
                # Execute Terraform Plan
                logger.info(f"Creating Terraform plan for {account_name}")
                plan_job = tf.workspaces.plan(
                    workspace_id=workspace_id,
                    cooldown_policy=LinearCooldownPolicy(delay=180, max_attempts=540),
                )
                
                plan_activity_id = plan_job.id
                update_deployment_status(
                    account_name=account_name,
                    status="planning",
                    session=db_session,
                )
                
                # Wait for plan to complete
                plan_result = tf.workspaces._wait_after_plan_or_apply(
                    workspace_id=workspace_id,
                    activity_id=plan_activity_id,
                )
                
                if plan_result.status != "job_finished":
                    raise Exception(
                        f"Terraform plan failed with status: {plan_result.status}"
                    )
                
                logger.info(f"OK - Terraform plan completed for {account_name}")
                
                # Execute Terraform Apply (if requested)
                final_status = "plan_completed"
                if payload.execution_mode == "plan_and_apply":
                    logger.info(f"Applying Terraform changes for {account_name}")
                    apply_job = tf.workspaces.apply(
                        workspace_id=workspace_id,
                        cooldown_policy=LinearCooldownPolicy(delay=360, max_attempts=540),
                    )
                    apply_activity_id = apply_job.id
                    
                    update_deployment_status(
                        account_name=account_name,
                        status="applying",
                        session=db_session,
                        activity_id=apply_activity_id
                    )
                    
                    # Wait for apply to complete
                    apply_result = tf.jobs.wait_for_completion(
                        apply_activity_id,
                        timeout=1800
                    )
                    
                    if apply_result.status != "job_finished":
                        raise Exception(
                            f"Terraform apply failed with status: {apply_result.status}"
                        )
                    
                    final_status = "deployed"
                    logger.info(f"OK - Terraform apply completed for {account_name}")
                else:
                    logger.info(f"OK - Deployment stopped at plan stage (plan_only mode)")
                
                # Update final status in database
                update_deployment_status(
                    account_name=account_name,
                    status=final_status,
                    session=db_session
                )
                
                return {
                    "account_name": account_name,
                    "workspace_id": workspace_id,
                    "status": "success",
                    "final_status": final_status
                }
                
            except Exception as e:
                logger.error(f"NO - Deployment failed for {account_name}: {str(e)}")
                update_deployment_status(
                    account_name=account_name,
                    status="deployment_failed",
                    session=db_session,
                    error_message=str(e)
                )
                return {
                    "account_name": account_name,
                    "workspace_id": workspace_id,
                    "status": "failed",
                    "error": str(e)
                }

        @step
        def prepare_deployment_data(
            account_workspaces: List[dict],
            vault_data: dict,
        ) -> List[dict]:
            """Prepare data for parallel deployment"""
            vault_token = vault_data["vault_token"]
            schematics_api_key = vault_data["schematics_api_key"]
            
            # Create a list with all needed data for each deployment
            deployment_data = []
            for account_workspace in account_workspaces:
                deployment_data.append({
                    "account_workspace": account_workspace,
                    "vault_token": vault_token,
                    "schematics_api_key": schematics_api_key
                })
            
            logger.info(f"Prepared {len(deployment_data)} accounts for parallel deployment")
            return deployment_data

        @step
        def aggregate_deployment_results(
            deployment_results: List[dict],
            payload: AccountUpdatePayload = depends(payload_dependency),
        ) -> dict:
            """Aggregate all deployment results"""
            successful = [r for r in deployment_results if r["status"] == "success"]
            failed = [r for r in deployment_results if r["status"] == "failed"]
            
            results = {
                "successful": successful,
                "failed": failed,
                "total": len(deployment_results),
                "execution_mode": payload.execution_mode,
            }
            
            logger.info("=" * 70)
            logger.info("DEPLOYMENT SUMMARY")
            logger.info("=" * 70)
            logger.info(f"Total: {results['total']}")
            logger.info(f"Successful: {len(successful)}")
            logger.info(f"Failed: {len(failed)}")
            logger.info("=" * 70)
            if results['total'] > 0:
                logger.info(f"Success rate: {len(successful)/results['total']*100:.1f}%")
            
            if len(failed) == results["total"]:
                raise Exception("All deployments failed")
            
            return results
        
        # Prepare data first
        deployment_data = prepare_deployment_data(account_workspaces, vault_data)
        
        # Then expand on the prepared data
        deployments = deploy_single_account.expand_kwargs(deployment_data)
        
        return aggregate_deployment_results(deployments)

    # =================================================================
    # DAG FLOW - Pipeline principal
    # =================================================================
    
    # Phase 1: Validation
    accounts_valid = validation_group()
    
    # Phase 2: Database preparation
    updated_accounts = database_preparation_group()
    
    # Phase 3: Vault & secrets (reçoit updated_accounts)
    vault_data = vault_and_secrets_group(updated_accounts)
    
    # Phase 4: Parallel workspace lookup (reçoit vault_data qui contient tout)
    account_workspaces = workspace_lookup_group(vault_data)
    
    # Phase 5: Parallel deployment (reçoit account_workspaces et vault_data)
    deployment_results = parallel_deployment_group(account_workspaces, vault_data)
    
    # Define dependencies
    accounts_valid >> updated_accounts >> vault_data >> account_workspaces >> deployment_results
    
    return deployment_results

dag_parallel_update()