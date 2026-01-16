@product_action(
    action_id=Path(__file__).stem,
    payload=AccountUpdatePayload,
    tags=["account", "admin", "parallel", "update"],
    doc_md="DAG to parallel account update."
)
def dag_parallel_update() -> None:

    # STEP 1: CHECK ACCOUNTS EXIST
    @step
    def check_accounts_exist(
        payload: AccountUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:
        """Validate that all accounts exist in database"""
        missing_accounts: list[str] = []

        logger.info("Checking account existence in database")

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

    # STEP 2: UPDATE ACCOUNTS IN DATABASE
    @step
    def update_accounts_database(
        payload: AccountUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> list[dict]:
        """Update all accounts configuration in database"""
        updated_accounts = []

        for account_name in payload.account_names:
            try:
                account = update_account_config(
                    account_name=account_name,
                    updates=payload.updates,
                    session=db_session,
                )

                updated_accounts.append({
                    "account_name": account.name,
                    "account_number": account.number,
                    "account_type": account.type,
                    "environment_type": account.environment_type,
                    "code_bu": account.code_bu,
                })

                logger.info(f"Updated {account_name} in database")

            except Exception as e:
                logger.error(f"Error updating {account_name}: {str(e)}")
                update_deployment_status(
                    account_name=account_name,
                    status="update_failed",
                    session=db_session,
                    error_message=str(e),
                )

        if not updated_accounts:
            raise Exception("No accounts were successfully updated in database")

        logger.info(f"Updated {len(updated_accounts)} accounts in database")
        return updated_accounts

    # STEP 3: GET VAULT SECRETS
    @step(hide_output=True)
    def get_vault_secrets(
        payload: AccountUpdatePayload = depends(payload_dependency),
        vault: Vault = depends(vault_dependency),
    ) -> AccountSecrets:
        """Get orchestrator secrets from Vault"""

        logger.info("Retrieving orchestrator secrets from Vault...")

        gitlab_secret = vault.get_secret(
            f"orchestrator/{ENVIRONMENT}/products/account/gitlab"
        )

        tfe_secrets = vault.get_secret(
            f"orchestrator/{ENVIRONMENT}/tfe/hprod"
        )

        logger.info("Retrieved orchestrator secrets from Vault")

        return AccountSecrets(
            gitlab_token=gitlab_secret["token"],
            vault_token=vault.vault.token,
            tfe_secrets=tfe_secrets,
        )

    # STEP 4: CHECK ACCOUNT API KEYS IN VAULT
    @step(hide_output=True)
    def check_accounts_in_vault(
        updated_accounts: list[dict],
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
                    mount_point="ibmsid",
                )

                if api_key_secret is None or "api_key" not in api_key_secret:
                    missing_api_keys.append(f"{account_name} ({account_number})")

            except Exception as e:
                logger.error(f"Error accessing Vault for {account_name}: {str(e)}")
                missing_api_keys.append(f"{account_name} ({account_number})")

        if missing_api_keys:
            raise DeclineDemandException(
                f"The following accounts do not have API keys in Vault: {', '.join(missing_api_keys)}"
            )

        logger.info("All accounts have valid API keys in Vault")
        return True

    # STEP 5: GET BUHUB ACCOUNT DETAILS
    @step
    def get_buhub_account_details(
        updated_accounts: list[dict],
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> AccountDetails | None:
        """Retrieve BUHUB account details if needed"""

        logger.info("Checking if BUHUB account is needed...")

        wklapp_accounts = [
            acc for acc in updated_accounts
            if acc.get("account_type") == "WKLAPP"
        ]

        if not wklapp_accounts:
            logger.info("No WKLAPP accounts - BUHUB not needed")
            return None

        first_wklapp = wklapp_accounts[0]
        code_bu = first_wklapp.get("code_bu")

        logger.info(f"Looking for BUHUB account for code_bu: {code_bu}")

        from account.database.utils.account_services import get_account_by_type_and_bu

        buhub_account = get_account_by_type_and_bu(
            "BUHUB",
            code_bu,
            db_session,
        )

        if not buhub_account:
            raise Exception(f"BUHUB account not found for code_bu: {code_bu}")

        reader_connector = ReaderConnector()
        buhub_account_details = reader_connector.get_account_details(
            buhub_account.name
        )

        logger.info("BUHUB account details retrieved")
        return buhub_account_details
    # STEP 6: GET BUHUB / GLBHUB API KEY FOR SCHEMATICS
    @step(hide_output=True)
    def get_buhub_account_api_key(
        buhub_account_details: AccountDetails | None,
        vault: Vault = depends(vault_dependency),
    ) -> str:
        """
        Get API key for Schematics operations.
        Uses BUHUB account if available, otherwise falls back to GLBHUB.
        """

        logger.info("Retrieving API key for Schematics...")
        logger.info(f"BUHUB account details: {buhub_account_details}")

        if buhub_account_details is not None:
            api_key = vault.get_secret(
                f"{buhub_account_details['number']}/account-owner",
                mount_point="ibmsid",
            )["api_key"]

            logger.info("Using BUHUB account API key")
            return api_key

        logger.info("Using GLBHUB account (no BUHUB needed)")
        api_key = vault.get_secret(
            f"{GLBHUB_ACCOUNT_NUMBER}/account-owner",
            mount_point="ibmsid",
        )["api_key"]

        logger.info("Retrieved GLBHUB account API key")
        return api_key

    # STEP 7: GET SCHEMATICS WORKSPACE FOR ONE ACCOUNT
    def get_workspace_for_account(
        account_info: dict,
        tf: SchematicsBackend = depends(schematics_backend_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> db_models.Workspace:
        """Get Schematics workspace for a specific account"""

        workspace_name = (
            f"WS-ACCOUNT-DCLOUD-"
            f"{account_info['environment_type'].upper()}-"
            f"{account_info['account_type'].upper()}-"
            f"{account_info['account_name'].upper()}"
        )

        logger.info(f"Looking for workspace: {workspace_name}")

        workspace_in_db: db_models.Workspace = (
            db_session.query(db_models.Workspace)
            .filter(
                db_models.Workspace.name.contains(workspace_name),
                db_models.Workspace.type == "schematics",
            )
            .one_or_none()
        )

        if not workspace_in_db:
            raise ValueError(f"Workspace {workspace_name} not found in database")

        workspace = tf.workspaces.get_by_id(workspace_in_db.id)
        status = SchematicsWorkspaceStatus(workspace.status)

        logger.info(f"Workspace {workspace_name} status: {status.value}")
        return workspace_in_db

    # STEP 7B: GET WORKSPACES FOR ALL ACCOUNTS
    @step
    def get_workspaces_for_accounts(
        updated_accounts: list[dict],
        accounts_in_vault: bool,
        vault_secrets: AccountSecrets,
        tf: SchematicsBackend = depends(schematics_backend_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
    ) -> list[dict]:
        """Get Schematics workspaces for all accounts"""

        logger.info(f"Getting Schematics workspaces for {len(updated_accounts)} accounts...")

        account_workspaces = []
        failed_accounts = []

        for account_info in updated_accounts:
            try:
                workspace = get_workspace_for_account(
                    account_info=account_info,
                    tf=tf,
                    db_session=db_session,
                )

                account_with_workspace = {
                    **account_info,
                    "workspace_id": workspace.id,
                    "workspace_name": workspace.name,
                }

                account_workspaces.append(account_with_workspace)
                logger.info(f"Found workspace for {account_info['account_name']}")

            except Exception as e:
                logger.error(
                    f"Failed to get workspace for {account_info['account_name']}: {str(e)}"
                )
                failed_accounts.append(account_info["account_name"])

                update_deployment_status(
                    account_name=account_info["account_name"],
                    status="workspace_not_found",
                    session=db_session,
                    error_message=str(e),
                )

        if not account_workspaces:
            raise Exception(
                f"No workspaces found for any accounts. Failed: {', '.join(failed_accounts)}"
            )

        if failed_accounts:
            logger.warning(f"Some workspaces not found: {', '.join(failed_accounts)}")

        logger.info(f"Found {len(account_workspaces)} Schematics workspaces")
        return account_workspaces

    # STEP 8: DEPLOY SINGLE ACCOUNT
    def deploy_single_account(
        account_workspace: dict,
        vault_secrets: AccountSecrets,
        payload: AccountUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
        tf: SchematicsBackend = depends(schematics_backend_dependency),
    ) -> dict:
        """Deploy configuration updates for a single account"""

        account_name = account_workspace["account_name"]
        workspace_id = account_workspace["workspace_id"]

        try:
            logger.info(f"Starting deployment for account: {account_name}")

            account = get_account_by_name(account_name, db_session)

            variables = {}

            if payload.updates.iam_version:
                variables["iam_version"] = account.iam_version

            if payload.updates.subnet_count is not None:
                variables["subnet_count"] = str(account.subnet_count)

            if payload.updates.enable_monitoring is not None:
                variables["enable_monitoring"] = str(
                    account.enable_monitoring
                ).lower()

            if payload.updates.tags:
                variables["tags"] = ",".join(account.tags)

            variables["vault_token"] = TerraformVar(
                vault_secrets.vault_token, True
            )

            logger.info(f"Variables to update: {list(variables.keys())}")
            tf.workspaces.update_variables(workspace_id, variables)

            plan_job = tf.workspaces.plan(
                workspace_id=workspace_id,
                cooldown_policy=LinearCooldownPolicy(
                    delay=180, max_attempts=540
                ),
            )

            update_deployment_status(
                account_name=account_name,
                status="planning",
                session=db_session,
            )

            plan_result = tf.workspaces.wait_after_plan_or_apply(
                workspace_id=workspace_id,
                activity_id=plan_job.id,
            )

            if plan_result.status != "job_finished":
                raise Exception(
                    f"Terraform plan failed with status: {plan_result.status}"
                )

            if payload.execution_mode == "plan_and_apply":
                apply_job = tf.jobs.apply(workspace_id)
                apply_result = tf.jobs.wait_for_completion(
                    apply_job.id, timeout=1800
                )

                if apply_result.status != "job_finished":
                    raise Exception(
                        f"Terraform apply failed with status: {apply_result.status}"
                    )

                final_status = "deployed"
            else:
                final_status = "plan_completed"

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
            logger.error(f"Deployment failed for {account_name}: {str(e)}")

            update_deployment_status(
                account_name=account_name,
                status="deployment_failed",
                session=db_session,
                error_message=str(e),
            )

            return {
                "account_name": account_name,
                "workspace_id": workspace_id,
                "status": "failed",
                "error": str(e),
            }

    # STEP 9: DEPLOY ACCOUNTS IN PARALLEL
    @step
    def deploy_accounts_parallel(
        account_workspaces: list[dict],
        vault_secrets: AccountSecrets,
        payload: AccountUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
        tf: SchematicsBackend = depends(schematics_backend_dependency),
    ) -> dict:
        """Deploy configuration updates to all accounts in parallel"""

        total_accounts = len(account_workspaces)
        max_parallel = payload.max_parallel_executions

        logger.info("Starting parallel deployment")
        logger.info(f"Total accounts: {total_accounts}")
        logger.info(f"Max parallel: {max_parallel}")
        logger.info(f"Mode: {payload.execution_mode}")

        results = {
            "successful": [],
            "failed": [],
            "total": total_accounts,
            "execution_mode": payload.execution_mode,
        }

        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = {
                executor.submit(
                    deploy_single_account,
                    account_workspace,
                    vault_secrets,
                    payload,
                    db_session,
                    tf,
                ): account_workspace["account_name"]
                for account_workspace in account_workspaces
            }

            for future in as_completed(futures):
                account_name = futures[future]
                try:
                    result = future.result()
                    if result["status"] == "success":
                        results["successful"].append(result)
                        logger.info(
                            f"{account_name}: {result['final_status']}"
                        )
                    else:
                        results["failed"].append(result)
                        logger.error(
                            f"{account_name}: {result.get('error', 'Unknown error')}"
                        )
                except Exception as e:
                    results["failed"].append({
                        "account_name": account_name,
                        "status": "failed",
                        "error": str(e),
                    })
                    logger.error(
                        f"{account_name}: Unexpected error: {str(e)}"
                    )

        if len(results["failed"]) == total_accounts:
            raise Exception(
                f"All {total_accounts} account deployments failed. Check logs for details."
            )

        return results
    accounts_valid = check_accounts_exist()
    updated_accounts = update_accounts_database()
    vault_secrets = get_vault_secrets()
    accounts_in_vault = check_accounts_in_vault(updated_accounts)
    buhub_account_details = get_buhub_account_details(updated_accounts)
    schematics_api_key = get_buhub_account_api_key(buhub_account_details)

    account_workspaces = get_workspaces_for_accounts(
        updated_accounts=updated_accounts,
        accounts_in_vault=accounts_in_vault,
        vault_secrets=vault_secrets,
    )

    results = deploy_accounts_parallel(
        account_workspaces=account_workspaces,
        vault_secrets=vault_secrets,
    )

    accounts_valid > updated_accounts
    updated_accounts > vault_secrets
    vault_secrets > accounts_in_vault
    accounts_in_vault > buhub_account_details
    buhub_account_details > schematics_api_key
    schematics_api_key > account_workspaces
    account_workspaces > results

    return results
