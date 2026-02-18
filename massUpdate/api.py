@step(hide_output=True)
def prepare_accounts_with_api_keys(
    db_session: SASession = depends(sqlalchemy_session_dependency),
    payload: AccountUpdatePayload = depends(payload_dependency),
    vault: Vault = depends(vault_dependency),
) -> dict:
    """
    Prepare accounts and retrieve Schematics API keys.

    - If account_type == "WKLAPP", use BUHUB account for the same code_bu
    - Otherwise fallback to GLBHUB
    - Validate API keys presence
    - Return aligned accounts + api keys
    """

    logger.info("🔎 Updating accounts...")
    updated_accounts = update_accounts_(payload, db_session)

    if not updated_accounts:
        raise DeclineDemandException("No accounts found after update.")

    from accountdatabase.utils.account_services import get_account_by_type_and_bu
    reader_connector = ReaderConnector()

    accounts_list = []
    api_keys = []
    accounts_without_api_keys = []

    logger.info("🔐 Retrieving API keys for Schematics...")

    for account in updated_accounts:
        account_name = account.get("name")
        account_type = account.get("account_type")
        code_bu = account.get("code_bu")

        logger.info(f"Processing account: {account_name} ({account_type})")

        api_key = None

        # 🔹 Case 1: WKLAPP → Need BUHUB account
        if account_type == "WKLAPP":
            logger.info(f"Looking for BUHUB account for code_bu={code_bu}")

            buhub_account = get_account_by_type_and_bu(
                "BUHUB",
                code_bu,
                db_session
            )

            if not buhub_account:
                raise Exception(
                    f"BUHUB account not found for code_bu={code_bu}"
                )

            buhub_details = reader_connector.get_account_details(
                buhub_account.name
            )

            api_key = vault.get_secret(
                f"{buhub_details['number']}/account-owner",
                mount_point="ibmsid"
            ).get("api_key")

            logger.info("Using BUHUB API key")

        # 🔹 Case 2: fallback to GLBHUB
        else:
            logger.info("Using GLBHUB API key")

            api_key = vault.get_secret(
                f"{HUB_ACCOUNT_NUMBER}/account-owner",
                mount_point="ibmsid"
            ).get("api_key")

        accounts_list.append(account_name)

        if api_key:
            api_keys.append(str(api_key))
        else:
            accounts_without_api_keys.append(account_name)
            api_keys.append(None)

    # 🔎 Log summary
    valid_count = len(accounts_list) - len(accounts_without_api_keys)

    if not valid_count:
        raise DeclineDemandException(
            "No accounts have valid API keys in Vault. Cannot proceed."
        )

    if accounts_without_api_keys:
        logger.warning(
            "The following accounts have no API key "
            "(will fail during deployment): "
            + ", ".join(accounts_without_api_keys)
        )

    logger.info(
        f"✔ Prepared {len(accounts_list)} accounts for deployment "
        f"({valid_count} with API keys, "
        f"{len(accounts_without_api_keys)} without)"
    )

    # 🔎 Alignment check
    logger.info(
        f"ALIGNMENT CHECK: accounts_list={len(accounts_list)}, "
        f"schematics_api_keys_list={len(api_keys)}"
    )

    return {
        "accounts_list": accounts_list,
        "schematics_api_keys_list": api_keys,
    }
