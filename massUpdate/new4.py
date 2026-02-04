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
        def check_accounts_exist_and_validate(
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> List[str]:
            """Validate that all accounts exist and have valid account_number"""
            missing_accounts: list[str] = []
            invalid_account_numbers: list[str] = []
            valid_accounts: list[str] = []
            
            for account_name in payload.account_names:
                account = get_account_by_name(account_name, db_session)
                
                # Check if account exists
                if account is None:
                    missing_accounts.append(account_name)
                    continue
                
                # Check if account_number is valid
                if not account.number or not str(account.number).strip():
                    logger.warning(
                        f"Account {account_name} has no valid account_number - "
                        f"will be skipped from processing"
                    )
                    invalid_account_numbers.append(account_name)
                    continue
                
                # Account is valid
                valid_accounts.append(account_name)
            
            # Accounts not found should block the workflow
            if missing_accounts:
                raise DeclineDemandException(
                    f"Accounts not found in database: {', '.join(missing_accounts)}"
                )
            
            # No valid accounts to process
            if not valid_accounts:
                raise DeclineDemandException(
                    "No valid accounts to process. All accounts have invalid account_number."
                )
            
            # Log warnings for invalid account numbers
            if invalid_account_numbers:
                logger.warning(
                    f"The following accounts will be skipped (no valid account_number): "
                    f"{', '.join(invalid_account_numbers)}"
                )
            
            logger.info(
                f"Validation complete: {len(valid_accounts)} valid accounts, "
                f"{len(invalid_account_numbers)} skipped"
            )
            
            return valid_accounts
        
        return check_accounts_exist_and_validate()

    @task_group(group_id="database_preparation")
    def database_preparation_group(valid_account_names: List[str]):
        """Update accounts configuration in database"""
        
        @step
        def update_accounts_database(
            valid_account_names: List[str],
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> List[dict]:
            """Update all valid accounts configuration in database and return account details"""
            updated_accounts = []
            
            for account_name in valid_account_names:
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
        
        return update_accounts_database(valid_account_names)

    @task_group(group_id="vault_and_secrets")
    def vault_and_secrets_group(updated_accounts: List[dict]):
        """Retrieve vault secrets and API keys"""
        
        @step(hide_output=True)
        def check_accounts_in_vault(
            updated_accounts: List[dict],
            vault: Vault = depends(vault_dependency),
        ) -> List[dict]:
            """Verify accounts have required API keys in Vault and filter out those without"""
            logger.info(f"Checking Vault for {len(updated_accounts)} account API keys...")
            
            accounts_with_api_keys = []
            accounts_without_api_keys = []
            
            for account_info in updated_accounts:
                account_number = account_info["account_number"]
                account_name = account_info["account_name"]
                
                try:
                    api_key_secret = vault.get_secret(
                        f"{account_number}/account-owner",
                        mount_point="ibmsid"
                    )
                    
                    if api_key_secret is None or "api_key" not in api_key_secret:
                        logger.warning(
                            f"API key not found for account {account_name} ({account_number}) - "
                            f"will be skipped from processing"
                        )
                        accounts_without_api_keys.append(account_name)
                    else:
                        accounts_with_api_keys.append(account_info)
                        
                except Exception as e:
                    logger.warning(
                        f"Error accessing Vault for {account_name}: {str(e)} - "
                        f"will be skipped from processing"
                    )
                    accounts_without_api_keys.append(account_name)
            
            # If no accounts have API keys, we should stop
            if not accounts_with_api_keys:
                raise DeclineDemandException(
                    "No accounts have valid API keys in Vault. Cannot proceed."
                )
            
            # Log summary
            if accounts_without_api_keys:
                logger.warning(
                    f"The following accounts will be skipped (no API key in Vault): "
                    f"{', '.join(accounts_without_api_keys)}"
                )
            
            logger.info(
                f"OK - {len(accounts_with_api_keys)} accounts have valid API keys in Vault"
            )
            
            return accounts_with_api_keys

        @step
        def get_buhub_account_details(
            accounts_with_api_keys: List[dict],
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> AccountDetails | None:
            """Get BUHUB account details if working with WLAPP accounts"""
            logger.info("Checking if BUHUB account is needed...")
            
            # Check if any account is WLAPP type
            wlapp_accounts = [
                acc for acc in accounts_with_api_keys if acc.get("account_type") == "WKLAPP"
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

        @step
        def combine_vault_data(
            accounts_with_api_keys: List[dict],
            schematics_api_key: str,
            vault_token: str,
        ) -> dict:
            """Combine all vault data into a single dict for passing to next groups"""
            logger.info("Combining vault data for downstream tasks")
            logger.info(f"Processing {len(accounts_with_api_keys)} accounts with valid API keys")
            
            return {
                "schematics_api_key": schematics_api_key,
                "vault_token": vault_token,
                "updated_accounts": accounts_with_api_keys
            }
        
        # Filter accounts that have API keys
        accounts_with_api_keys = check_accounts_in_vault(updated_accounts)
        
        # Execute vault operations in parallel
        buhub_details = get_buhub_account_details(accounts_with_api_keys)
        vault_token = get_vault_token()
        
        # Get schematics API key (depends on buhub_details)
        schematics_api_key = get_schematics_api_key(buhub_details)
        
        # Combine everything into a single return value
        vault_data = combine_vault_data(accounts_with_api_keys, schematics_api_key, vault_token)
        
        return vault_data

    @task_group(group_id="workspace_lookup_parallel")
    def workspace_lookup_group(vault_data: dict):
        """Lookup workspaces in parallel for each account"""
        
        @step(
            map_index_template="{{ account_workspace_index }}"
        )
        def get_workspace_for_single_account(
            account_info: dict,
            schematics_api_key: str,
            tf: SchematicsBackend = depends(schematics_backend_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
        ) -> dict:
            """Get workspace for a single account"""
            from airflow.operators.python import get_current_context
            
            account_name = account_info['account_name']
            
            airflow_context = get_current_context()
            airflow_context["account_workspace_index"] = f"account: {account_name}"
            
            try:
                workspace_in_db = get_souscription_by_name(account_name, db_session)
                
                if not workspace_in_db:
                    raise ValueError(f"Workspace not found in database for {account_name}")
                
                workspace = tf.workspaces.get_by_id(workspace_in_db.id)
                status = SchematicsWorkspaceStatus(workspace.status)
                logger.info(f"OK - Workspace {workspace_in_db.name} status: {status.value}")
                
                if status.value not in ["ACTIVE", "INACTIVE"]:
                    raise ValueError(
                        f"Workspace {workspace_in_db.name} has invalid status: {status.value}"
                    )
                
                return {
                    **account_info,
                    "workspace_id": workspace_in_db.id,
                    "workspace_name": workspace_in_db.name,
                    "workspace_status": status.value
                }
                
            except Exception as e:
                logger.error(f"NO - Failed to get workspace for {account_name}: {str(e)}")
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
            # Note: avec expand_kwargs et raise dans get_workspace_for_single_account,
            # les échecs vont arrêter la tâche. Si tu veux continuer malgré les échecs,
            # il faudrait ne pas raise dans get_workspace_for_single_account
            valid_workspaces = [w for w in workspace_results if w is not None]
            
            if not valid_workspaces:
                raise Exception("No valid workspaces found for any accounts")
            
            logger.info(f"OK - Found {len(valid_workspaces)} valid Schematics workspaces")
            return valid_workspaces
        
        lookup_data = prepare_workspace_lookup_data(vault_data)
        workspace_lookups = get_workspace_for_single_account.expand_kwargs(lookup_data)
        valid_workspaces = filter_valid_workspaces(workspace_lookups)
        
        return valid_workspaces

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
            schematics_api_key: str,
            payload: AccountUpdatePayload = depends(payload_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency),
            tf: SchematicsBackend = depends(schematics_backend_dependency),
        ) -> dict:
            """Deploy to a single account"""
            from airflow.operators.python import get_current_context
            
            account_name = account_workspace["account_name"]
            workspace_id = account_workspace["workspace_id"]
            
            airflow_context = get_current_context()
            airflow_context["deployment_index"] = f"deploying: {account_name}"
            
            try:
                logger.info(f"Starting deployment for account: {account_name}")
                logger.info(f"Workspace ID: {workspace_id}")
                
                account = get_account_by_name(account_name, db_session)
                
                variables = {}
                if payload.iam_version:
                    variables["iam_version"] = account.iam_version
                if payload.enable_cbr is not None:
                    variables["enable_cbr"] = str(account.enable_cbr).lower()
                
                variables["vault_token"] = TerraformVar(vault_token, True)
                
                logger.info(f"Variables to update: {list(variables.keys())}")
                
                tf.workspaces.update_variables(workspace_id, variables)
                logger.info(f"OK - Updated workspace variables for {account_name}")
                
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
                
                plan_result = tf.workspaces._wait_after_plan_or_apply(
                    workspace_id=workspace_id,
                    activity_id=plan_activity_id,
                )
                
                if plan_result.status != "job_finished":
                    raise Exception(
                        f"Terraform plan failed with status: {plan_result.status}"
                    )
                
                logger.info(f"OK - Terraform plan completed for {account_name}")
                
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
        
        deployment_data = prepare_deployment_data(account_workspaces, vault_data)
        deployments = deploy_single_account.expand_kwargs(deployment_data)
        results = aggregate_deployment_results(deployments)
        
        return results

    # =================================================================
    # DAG FLOW - Pipeline principal
    # =================================================================
    
    # Phase 1: Validation - retourne la liste des accounts valides
    valid_account_names = validation_group()
    
    # Phase 2: Database preparation - reçoit les noms des accounts valides
    updated_accounts = database_preparation_group(valid_account_names)
    
    # Phase 3: Vault & secrets - filtre les accounts sans API key
    vault_data = vault_and_secrets_group(updated_accounts)
    
    # Phase 4: Parallel workspace lookup
    account_workspaces = workspace_lookup_group(vault_data)
    
    # Phase 5: Parallel deployment
    deployment_results = parallel_deployment_group(account_workspaces, vault_data)
    
    # Define dependencies
    valid_account_names >> updated_accounts >> vault_data >> account_workspaces >> deployment_results

dag_parallel_update()