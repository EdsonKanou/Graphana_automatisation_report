@step(hide_output=True)
def get_accounts_api_keys(
    updated_accounts: List[dict],
    vault: Vault = depends(vault_dependency),
) -> List[str]:
    """
    Retrieve API keys for all accounts in a single step.
    - Try direct account_number first
    - Fallback using ReaderConnector if needed
    - Always return a list with same size as input
    - Accounts without API key are logged as warning
    """

    logger.info(f"🔐 Retrieving API keys for {len(updated_accounts)} accounts...")

    from account.reader_connector import ReaderConnector

    reader_connector = ReaderConnector()
    api_keys: List[str] = []
    accounts_without_api_keys: List[str] = []

    for account in updated_accounts:
        account_name = account.get("account_name")
        account_number = account.get("account_number")
        api_key = None

        # --- First attempt: direct Vault access
        try:
            if account_number:
                secret = vault.get_secret(
                    f"{account_number}/account-owner",
                    mount_point="ibmsid",
                )
                if secret and "api_key" in secret:
                    api_key = secret["api_key"]
        except Exception as e:
            logger.warning(
                f"Vault access failed for {account_name} (direct): {str(e)}"
            )

        # --- Second attempt: ReaderConnector fallback
        if not api_key:
            try:
                account_details = reader_connector.get_account_details(account_name)
                fallback_account_number = account_details.get("number")

                secret = vault.get_secret(
                    f"{fallback_account_number}/account-owner",
                    mount_point="ibmsid",
                )
                if secret and "api_key" in secret:
                    api_key = secret["api_key"]
            except Exception as e:
                logger.warning(
                    f"Vault access failed for {account_name} (reader fallback): {str(e)}"
                )

        # --- Final handling
        if api_key:
            api_keys.append(api_key)
        else:
            accounts_without_api_keys.append(account_name)
            api_keys.append("API key not found for this account")

    # --- Logging summary
    if accounts_without_api_keys:
        logger.warning(
            "⚠️ API key not found for the following accounts: "
            f"{', '.join(accounts_without_api_keys)}"
        )

    logger.info(f"✅ API key retrieval finished ({len(api_keys)} entries)")
    return api_keys
