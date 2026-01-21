from typing import TypedDict, List


class AccountInfo(TypedDict):
    account_name: str
    account_number: str
    account_type: str
    environment_type: str
    code_bu: str


class DeploymentResult(TypedDict):
    account_name: str
    workspace_id: str
    status: str
    final_status: str | None
    error: str | None

def mark_failure(
    *,
    account_name: str,
    status: str,
    session: SASession,
    error: Exception | str,
):
    logger.error(f"{status} - {account_name}: {error}")
    update_deployment_status(
        account_name=account_name,
        status=status,
        session=session,
        error_message=str(error),
    )


@product_action(
    action_id=Path(__file__).stem,
    payload=AccountUpdatePayload,
    tags=["account", "admin", "parallel", "update"],
    doc_md="Clean parallel account update DAG",
    max_active_tasks=4,
)
def dag_parallel_update_clean() -> None:

    accounts_valid = validate_accounts_group()
    updated_accounts = prepare_database_group()
    vault_secrets = get_vault_secrets()
    schematics_api_key = vault_and_secrets_group(updated_accounts)
    account_workspaces = workspace_lookup_group(updated_accounts, schematics_api_key)
    deployment_results = parallel_deployment_group(
        account_workspaces,
        vault_secrets,
        schematics_api_key,
    )

    (
        accounts_valid
        >> updated_accounts
        >> vault_secrets
        >> schematics_api_key
        >> account_workspaces
        >> deployment_results
    )

    return deployment_results

@task_group(group_id="validation")
def validate_accounts_group():

    @step
    def validate_accounts_exist(
        payload: AccountUpdatePayload = depends(payload_dependency),
        db: SASession = depends(sqlalchemy_session_dependency),
    ) -> bool:

        missing = [
            name
            for name in payload.account_names
            if not get_account_by_name(name, db)
        ]

        if missing:
            raise DeclineDemandException(
                f"Accounts missing in DB: {', '.join(missing)}"
            )

        logger.info("All accounts exist in database")
        return True

    return validate_accounts_exist()

@task_group(group_id="database_preparation")
def prepare_database_group():

    @step
    def update_accounts(
        payload: AccountUpdatePayload = depends(payload_dependency),
        db: SASession = depends(sqlalchemy_session_dependency),
    ) -> List[AccountInfo]:

        results: List[AccountInfo] = []

        for name in payload.account_names:
            try:
                acc = update_account_config(
                    account_name=name,
                    updates=payload.updates,
                    session=db,
                )

                results.append(
                    {
                        "account_name": acc.name,
                        "account_number": acc.number,
                        "account_type": acc.type,
                        "environment_type": acc.environment_type,
                        "code_bu": acc.code_bu,
                    }
                )

            except Exception as e:
                mark_failure(
                    account_name=name,
                    status="update_failed",
                    session=db,
                    error=e,
                )

        if not results:
            raise Exception("No account updated successfully")

        return results

    return update_accounts()

@task_group(group_id="vault_and_secrets")
def vault_and_secrets_group(accounts: List[AccountInfo]):

    @step(hide_output=True)
    def validate_vault_keys(
        vault: Vault = depends(vault_dependency),
    ) -> None:

        missing = []

        for acc in accounts:
            secret = vault.get_secret(
                f"{acc['account_number']}/account-owner",
                mount_point="ibmsid",
            )

            if not secret or "api_key" not in secret:
                missing.append(acc["account_name"])

        if missing:
            raise DeclineDemandException(
                f"Missing API keys in Vault: {', '.join(missing)}"
            )

    @step(hide_output=True)
    def resolve_schematics_api_key(
        vault: Vault = depends(vault_dependency),
        db: SASession = depends(sqlalchemy_session_dependency),
    ) -> str:

        wklapp = next(
            (a for a in accounts if a["account_type"] == "WKLAPP"),
            None,
        )

        if wklapp:
            buhub = get_account_by_type_and_bu(
                "BUHUB", wklapp["code_bu"], db
            )
            if not buhub:
                raise Exception("BUHUB account not found")

            secret = vault.get_secret(
                f"{buhub.number}/account-owner",
                mount_point="ibmsid",
            )
            return secret["api_key"]

        # fallback GLBHUB
        return vault.get_secret(
            f"{HUB_ACCOUNT_NUMBER}/account-owner",
            mount_point="ibmsid",
        )["api_key"]

    validate_vault_keys()
    return resolve_schematics_api_key()


@task_group(group_id="workspace_lookup")
def workspace_lookup_group(
    accounts: List[AccountInfo],
    schematics_api_key: str,
):

    @step
    def lookup_workspaces(
        tf: SchematicsBackend = depends(schematics_backend_dependency),
        db: SASession = depends(sqlalchemy_session_dependency),
    ) -> List[dict]:

        results = []

        for acc in accounts:
            try:
                ws = get_workspace_for_account(acc, tf, db)
                results.append(
                    {
                        **acc,
                        "workspace_id": ws.id,
                        "workspace_name": ws.name,
                    }
                )
            except Exception as e:
                mark_failure(
                    account_name=acc["account_name"],
                    status="workspace_not_found",
                    session=db,
                    error=e,
                )

        if not results:
            raise Exception("No workspace found for any account")

        return results

    return lookup_workspaces()


@task_group(group_id="parallel_deployment")
def parallel_deployment_group(
    account_workspaces: List[dict],
    vault_secrets,
    schematics_api_key: str,
):

    @step
    def deploy_single_account(
        account_workspace: dict,
        payload: AccountUpdatePayload = depends(payload_dependency),
        db: SASession = depends(sqlalchemy_session_dependency),
        tf: SchematicsBackend = depends(schematics_backend_dependency),
    ) -> DeploymentResult:
        ...
        # (même logique que ton code actuel, non modifiée)
        ...

    @step
    def aggregate_results(
        results: List[DeploymentResult],
        payload: AccountUpdatePayload = depends(payload_dependency),
    ) -> dict:
        success = [r for r in results if r["status"] == "success"]
        failed = [r for r in results if r["status"] == "failed"]

        if not success:
            raise Exception("All deployments failed")

        return {
            "successful": success,
            "failed": failed,
            "total": len(results),
            "execution_mode": payload.execution_mode,
        }

    deployments = deploy_single_account.expand(
        account_workspace=account_workspaces
    )

    return aggregate_results(deployments)

