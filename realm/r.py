def build_updated_realms(realm_list) -> list[dict]:
    updated_realms = []

    try:
        for realm in realm_list:
            updated_realms.append({
                "realm_name": realm.name,
                "subscription_id": realm.subscription_id,
                "code_bu": realm.code_bu,
                "environment_type": realm.env_type,
                "buhub_account_number": realm.buhub_account_number,
            })
    except Exception as e:
        logger.error(f"Database query failed: {str(e)}")
        raise

    return updated_realms

from collections import defaultdict

@step(hide_output=True)
def get_schematics_api_keys(
    updated_realms: list[dict],
    vault: Vault = depends(vault_dependency),
) -> dict[str, str]:
    """
    Returns:
        {
          buhub_account_number: schematics_api_key
        }
    """
    realms_by_buhub = defaultdict(list)
    api_keys: dict[str, str] = {}

    for realm in updated_realms:
        realms_by_buhub[realm["buhub_account_number"]].append(realm)

    for buhub_number in realms_by_buhub.keys():
        api_keys[buhub_number] = vault.get_secret(
            f"{buhub_number}/account-owner",
            mount_point="ibmsid",
        )["api_key"]

        logger.info(f"🔑 Schematics API key loaded for BUHUB {buhub_number}")

    return api_keys


def build_schematics_backend(api_key: str) -> SchematicsBackend:
    return SchematicsBackend(api_key=api_key)

def get_workspace_for_realm(
    realm_info: dict,
    tf: SchematicsBackend,
    db_session: SASession,
) -> db_models.Workspace:
    """
    Retrieve Schematics workspace for a specific realm
    """
    workspace_in_db = get_subscription_by_name(
        realm_info["realm_name"],
        db_session,
    )

    if not workspace_in_db:
        raise ValueError(
            f"Workspace not found in database for realm {realm_info['realm_name']}"
        )

    # Verify workspace status in Schematics
    workspace = tf.workspaces.get_by_id(workspace_in_db.id)
    status = SchematicsWorkspaceStatus(workspace.status)

    logger.info(
        f"✅ Workspace {workspace_in_db.name} status: {status.value}"
    )

    if status.value not in ["ACTIVE", "INACTIVE"]:
        raise ValueError(
            f"Workspace {workspace_in_db.name} has invalid status: {status.value}"
        )

    return workspace_in_db

@step
def get_workspaces_for_realms(
    updated_realms: list[dict],
    schematics_api_keys: dict[str, str],
    db_session: SASession = depends(sqlalchemy_session_dependency),
) -> list[dict]:
    """
    Returns:
        List of realms enriched with workspace info
    """
    realm_workspaces: list[dict] = []
    failed_realms: list[str] = []

    for realm_info in updated_realms:
        realm_name = realm_info["realm_name"]
        buhub_number = realm_info["buhub_account_number"]

        try:
            api_key = schematics_api_keys[buhub_number]
            tf = build_schematics_backend(api_key)

            workspace = get_workspace_for_realm(
                realm_info=realm_info,
                tf=tf,
                db_session=db_session,
            )

            realm_with_workspace = {
                **realm_info,
                "workspace_id": workspace.id,
                "workspace_name": workspace.name,
            }

            realm_workspaces.append(realm_with_workspace)

            logger.info(f"✅ Workspace found for realm {realm_name}")

        except Exception as e:
            logger.error(
                f"❌ Failed to get workspace for realm {realm_name}: {str(e)}"
            )
            failed_realms.append(realm_name)

    if not realm_workspaces:
        raise Exception(
            f"No workspaces found. Failed realms: {', '.join(failed_realms)}"
        )

    if failed_realms:
        logger.warning(
            f"⚠️ Some workspaces not found: {', '.join(failed_realms)}"
        )

    logger.info(
        f"🎉 Found {len(realm_workspaces)} Schematics workspaces"
    )

    return realm_workspaces
