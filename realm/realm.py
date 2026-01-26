get_buhub_account_number_by_name(realm.realm_name)

updated_realms = []

for realm in realm_list:
    updated_realms.append({
        "realm_name": realm.name,
        "subscription_id": realm.subscription_id,
        "code_bu": realm.code_bu,
        "environment_type": realm.env_type,
        "buhub_account_number": realm.buhub_account_number
    })


from collections import defaultdict

realms_by_buhub = defaultdict(list)

for realm in updated_realms:
    realms_by_buhub[realm["buhub_account_number"]].append(realm)


@step(hide_output=True)
def get_schematics_api_keys(
    realms_by_buhub: dict,
    vault: Vault = depends(vault_dependency),
) -> dict:
    api_keys = {}

    for buhub_number in realms_by_buhub.keys():
        api_keys[buhub_number] = vault.get_secret(
            f"{buhub_number}/account-owner",
            mount_point="ibmsid",
        )["api_key"]

    return api_keys

for buhub_number, realms in realms_by_buhub.items():
    api_key = api_keys[buhub_number]

    for realm in realms:
        tf.run(
            realm=realm,
            api_key=api_key
        )


def get_buhub_account_number_by_name(...)
