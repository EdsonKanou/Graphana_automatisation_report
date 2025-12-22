from typing import List

class AccountExtractIamPayload(ProductActionPayload):
    account_names: List[str] = Field(
        ...,
        description="Specify the list of account names",
        examples=[["ac0021000259", "ac0021000260"]],
        min_items=1,
    )

    @field_validator("account_names")
    @classmethod
    def validate_account_names(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("account_names cannot be empty.")
        if any(not name for name in v):
            raise ValueError("account_names cannot contain empty values.")
        return v



def get_account_by_name(
    name: str,
    session: SASession,
) -> db_models.AccountExtractIAM | None:
    account = (
        session.query(db_models.AccountExtractIAM)
        .filter(db_models.AccountExtractIAM.account_name.contains(name))
        .first()
    )
    return account


def check_account_name(
    payload: AccountExtractIamPayload = depends(payload_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
    vault: Vault = depends(vault_dependency),
) -> bool:
    missing_accounts: list[str] = []

    for account_name in payload.account_names:
        account = get_account_by_name(account_name, db_session)
        if account is None:
            missing_accounts.append(account_name)

    if missing_accounts:
        raise DeclineDemandException(
            f"Accounts not present in vault: {', '.join(missing_accounts)}"
        )

    return True






def extract_iam_to_excel(
    directory_path: str,
    payload: AccountExtractIamPayload = depends(payload_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
    vault: Vault = depends(vault_dependency),
) -> None:
    from account.services.extract_iam_account import (
        DMZRC_VIRTUEL_ECOSYSTEM,
        PAASV4_VIRTUEL_ECOSYSTEM,
    )

    for account_name in payload.account_names:
        try:
            account = get_account_by_name(account_name, db_session)
            if account is None:
                # sécurité supplémentaire
                raise ValueError(f"Account {account_name} not found")

            if account.account_type == "PAASV4":
                apikey = vault.get_secret(
                    f"{account.account_number}/account-owner",
                    mount_point="ibmsid",
                    namespace=PAASV4_VIRTUEL_ECOSYSTEM,
                )["api_key"]
            else:
                apikey = vault.get_secret(
                    f"{account.account_number}/account-owner",
                    mount_point="ibmsid",
                    namespace=DMZRC_VIRTUEL_ECOSYSTEM,
                )["api_key"]

            extract_iam_to_excel_file(
                apikey,
                account.account_id,
                account.account_name,
                directory_path,
            )

            logger.info(
                f"TAM extracted to Excel for account: {account.account_number}"
            )

        except Exception as e:
            logger.error(
                f"Error extracting IAM for account {account_name}: {e}"
            )
            raise



def zip_generated_directory(
    payload: AccountExtractIamPayload = depends(payload_dependency),
) -> str:
    try:
        from account.services.extract_iam_account import TMP_DIRECTORY_PATH

        directory_path = generate_directory_path(
            payload.subscription_id,
            TMP_DIRECTORY_PATH,
        )

        zip_name = (
            f"extract-iam-"
            f"{datetime.now().strftime('%d-%m-%Y')}-"
            f"{payload.subscription_id}.zip"
        )

        zip_path = zip_directory(
            directory_path,
            TMP_DIRECTORY_PATH,
            zip_name,
        )

        logger.info(f"Directory zipped at: {zip_path}")
        return zip_path

    except Exception as e:
        logger.error(f"Error zipping directory '{directory_path}': {e}")
        raise




from typing import List
from pydantic import Field, field_validator

class AccountExtractIamPayload(ProductActionPayload):
    account_names: List[str] = Field(
        ...,
        description="Specify the list of account names",
        examples=[["ac0021000259"]],
        min_items=1,
    )

    @field_validator("account_names")
    @classmethod
    def validate_account_names(cls, v: List[str]) -> List[str]:
        if not v:
            raise ValueError("account_names cannot be empty.")

        for name in v:
            if not isinstance(name, str) or not name.strip():
                raise ValueError(
                    "account_names cannot contain empty or blank values."
                )

        return v





@field_validator("account_names")
@classmethod
def validate_account_names(cls, v: List[str]) -> List[str]:
    cleaned = []

    for name in v:
        if not name or not name.strip():
            raise ValueError("account_names cannot contain empty values.")
        cleaned.append(name.strip().lower())

    return list(dict.fromkeys(cleaned))  # supprime doublons
