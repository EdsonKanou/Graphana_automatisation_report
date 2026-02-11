@product_action(
    action_id=Path(__file__).stem,
    payload=AccountUpdatePayload,
    tags=["account", "admin", "parallel", "update"],
    doc_md="DAG to parallel account update",
    max_active_tasks=4,
)
def dag_parallel_update() -> None:

    # =========================
    # STEP 1 - VALIDATION
    # =========================
    @step
    def check_accounts_exist_and_validate(
        payload: AccountUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> list[str]:
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
                    f"Account {account_name} has no valid account_number - will be skipped"
                )
                invalid_account_numbers.append(account_name)
                continue

            valid_accounts.append(account_name)

        if missing_accounts:
            raise DeclineDemandException(
                f"Accounts not found in database: {', '.join(missing_accounts)}"
            )

        if not valid_accounts:
            raise DeclineDemandException(
                "No valid accounts to process. All accounts have invalid account_number."
            )

        if invalid_account_numbers:
            logger.warning(
                "The following accounts will be skipped (no valid account_number): "
                f"{', '.join(invalid_account_numbers)}"
            )

        logger.info(
            f"Validation complete: {len(valid_accounts)} valid accounts, "
            f"{len(invalid_account_numbers)} skipped"
        )

        return valid_accounts

    # =========================
    # STEP 2 - UPDATE DATABASE
    # =========================
    @step
    def update_accounts_database(
        valid_account_names: list[str],
        payload: AccountUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> list[dict]:
        """Update all accounts configuration in database"""

        updated_accounts: list[dict] = []

        for account_name in valid_account_names:
            try:
                account = update_account_config(
                    account_name=account_name,
                    iam_version=payload.iam_version,
                    enable_cbr=payload.enable_cbr,
                    session=db_session,
                )

                updated_accounts.append(
                    {
                        "account_name": account.name,
                        "account_number": account.number,
                        "account_type": account.type,
                        "environment_type": account.environment_type,
                        "code_bu": account.code_bu,
                    }
                )

                logger.info(f"OK - Updated {account_name} in database")

            except Exception as e:
                logger.error(f"NO - Error updating {account_name}: {str(e)}")

                update_deployment_status(
                    account_name=account_name,
                    status="update_failed",
                    session=db_session,
                    error_message=str(e),
                )

        if not updated_accounts:
            raise Exception("No accounts were successfully updated in database")

        logger.info(f"OK - Updated {len(updated_accounts)} accounts in database")

        return updated_accounts

    # =========================
    # STEP 3 - CHECK VAULT & GET SCHEMATICS API KEY
    # =========================
    @step(hide_output=True)
    def check_vault_and_get_schematics_key(
        updated_accounts: list[dict],
        vault: Vault = depends(vault_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> dict:
        """Verify accounts have API keys in Vault and get Schematics API key"""

        # Check all accounts have API keys
        logger.info(f"Checking Vault for {len(updated_accounts)} account API keys...")
        
        accounts_with_api_keys: list[dict] = []
        accounts_without_api_keys: list[str] = []

        for account_info in updated_accounts:
            account_number = account_info["account_number"]
            account_name = account_info["account_name"]

            try:
                api_key_secret = vault.get_secret(
                    f"{account_number}/account-owner",
                    mount_point="ibmsid",
                )

                if api_key_secret is None or "api_key" not in api_key_secret:
                    logger.warning(
                        f"API key not found for account {account_name} ({account_number}) - "
                        f"will be skipped from processing"
                    )
                    accounts_without_api_keys.append(account_name)
                else:
                    # Add the API key to account info
                    account_with_key = {
                        **account_info,
                        "api_key": api_key_secret["api_key"]
                    }
                    accounts_with_api_keys.append(account_with_key)

            except Exception as e:
                logger.warning(
                    f"Error accessing Vault for {account_name}: {str(e)} - "
                    f"will be skipped from processing"
                )
                accounts_without_api_keys.append(account_name)

        # Check if we have at least one account with API key
        if not accounts_with_api_keys:
            raise DeclineDemandException(
                "No accounts have valid API keys in Vault. Cannot proceed."
            )

        if accounts_without_api_keys:
            logger.warning(
                f"The following accounts will be skipped (no API key in Vault): "
                f"{', '.join(accounts_without_api_keys)}"
            )

        logger.info(
            f"OK - {len(accounts_with_api_keys)} accounts have valid API keys in Vault"
        )

        # Get BUHUB details if needed (for WKLAPP accounts)
        wlapp_accounts = [
            acc for acc in accounts_with_api_keys if acc.get("account_type") == "WKLAPP"
        ]

        buhub_account_details = None
        if wlapp_accounts:
            logger.info("WKLAPP accounts detected - checking for BUHUB account...")
            first_wklapp = wlapp_accounts[0]
            code_bu = first_wklapp.get("code_bu")

            buhub_account = get_account_by_type_and_bu("BUHUB", code_bu, db_session)

            if buhub_account:
                reader_connector = ReaderConnector()
                buhub_account_details = reader_connector.get_account_details(
                    buhub_account.name
                )
                logger.info("BUHUB account details retrieved")

        # Get Schematics API key (GLBHUB)
        logger.info("Retrieving Schematics API key from GLBHUB account")
        schematics_api_key = vault.get_secret(
            f"{HUB_ACCOUNT_NUMBER}/account-owner",
            mount_point="ibmsid"
        )["api_key"]
        logger.info("OK - Retrieved GLBHUB account API key for Schematics")

        return {
            "accounts_with_api_keys": accounts_with_api_keys,
            "schematics_api_key": schematics_api_key,
        }

    # =========================
    # STEP 4 - PREPARE DEPLOYMENT DATA
    # =========================
    @step
    def prepare_deployment_data(
        vault_data: dict,
    ) -> list[dict]:
        """Prepare data for parallel deployment"""

        accounts = vault_data["accounts_with_api_keys"]
        schematics_api_key = vault_data["schematics_api_key"]

        deployment_data: list[dict] = []

        for account_info in accounts:
            deployment_data.append({
                "account_info": account_info,
                "schematics_api_key": schematics_api_key,
            })

        logger.info(f"Prepared {len(deployment_data)} accounts for parallel deployment")

        return deployment_data

    # =========================
    # STEP 5 - WORKSPACE + PLAN/APPLY (PARALLEL)
    # =========================
    @step(
        map_index_template="{{ deployment_index }}"
    )
    def get_workspace_and_update_account(
        account_info: dict,
        schematics_api_key: str,
        tf: SchematicsBackend = depends(schematics_backend_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
        vault: Vault = depends(vault_dependency),
        payload: AccountUpdatePayload = depends(payload_dependency),
    ) -> dict:
        """Get workspace and execute plan/apply for a single account"""
        from airflow.operators.python import get_current_context

        account_name = account_info["account_name"]
        
        # Set custom map index for better tracking
        airflow_context = get_current_context()
        airflow_context["deployment_index"] = f"deploying: {account_name}"

        try:
            # Get workspace from database
            logger.info(f"Retrieving workspace for account: {account_name}")
            workspace = get_subscription_by_name(account_name, db_session)

            if not workspace:
                raise ValueError(
                    f"Workspace not found in database for {account_name}"
                )

            workspace_id = workspace.id
            logger.info(f"Found workspace: {workspace.name} (ID: {workspace_id})")

            # Verify workspace status in Schematics
            workspace_obj = tf.workspaces.get_by_id(workspace_id)
            status = SchematicsWorkspaceStatus(workspace_obj.status)
            logger.info(f"Workspace status: {status.value}")

            if status.value not in ["ACTIVE", "INACTIVE"]:
                raise ValueError(
                    f"Workspace {workspace.name} has invalid status: {status.value}"
                )

            # Get account details from database for variables
            logger.info(f"Starting deployment for account: {account_name}")
            logger.info(f"Workspace ID: {workspace_id}")

            account = get_account_by_name(account_name, db_session)

            # Prepare Terraform variables
            variables = {}
            if payload.iam_version:
                variables["iam_version"] = account.iam_version
            if payload.enable_cbr is not None:
                variables["enable_cbr"] = str(account.enable_cbr).lower()

            # Add vault token
            vault_token = vault.vault.token
            variables["vault_token"] = TerraformVar(vault_token, True)

            logger.info(f"Variables to update: {list(variables.keys())}")
            logger.info(f"(End) - view Variables to update: {variables}")

            # Update workspace variables
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

            final_status = "plan_completed"

            # Execute Terraform Apply (if requested)
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

            # Update final status
            update_deployment_status(
                account_name=account_name,
                status=final_status,
                session=db_session,
            )

            return {
                "account_name": account_name,
                "workspace_id": workspace_id,
                "status": "success",
                "final_status": final_status,
            }

        except Exception as e:
            logger.error(f"NO - Deployment failed for {account_name}: {str(e)}")

            update_deployment_status(
                account_name=account_name,
                status="deployment_failed",
                session=db_session,
                error_message=str(e),
            )

            # Raise l'exception pour marquer la tâche comme failed
            raise

    # =========================
    # STEP 6 - AGGREGATION
    # =========================
    @step(trigger_rule="all_done")
    def aggregate_deployment_results(
        deployment_results: list[dict],
        payload: AccountUpdatePayload = depends(payload_dependency),
    ) -> dict:
        """Aggregate deployment results from all accounts"""

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

        if results["total"] > 0:
            success_rate = (len(successful) / results["total"]) * 100
            logger.info(f"Success rate: {success_rate:.1f}%")

        if len(failed) == results["total"]:
            raise Exception("All deployments failed")

        return results

    # =========================
    # WORKFLOW
    # =========================
    
    # Phase 1: Validation
    valid_account_names = check_accounts_exist_and_validate()
    
    # Phase 2: Update database
    updated_accounts = update_accounts_database(valid_account_names)
    
    # Phase 3: Check Vault and get Schematics API key
    vault_data = check_vault_and_get_schematics_key(updated_accounts)
    
    # Phase 4: Prepare deployment data
    deployment_data = prepare_deployment_data(vault_data)
    
    # Phase 5: Parallel deployment
    deployment_results = get_workspace_and_update_account.expand_kwargs(
        deployment_data
    )
    
    # Phase 6: Aggregate results
    final_results = aggregate_deployment_results(deployment_results)

dag_parallel_update()