@product_action(
    action_id=Path(__file__).stem,
    payload=RealmUpdatePayload,
    tags=[PRODUCT_NAME, "admin", "parallel", "update"],
    doc="DAG to admin parallel realm update.",
    max_active_tasks=4,
)
def dag_parallel_update() -> None:
    
    @step
    def detect_extraction_mode(
        payload: RealmUpdatePayload = depends(payload_dependency)
    ) -> dict:
        """Détecte le mode d'extraction."""
        mode = "by_names" if payload.realm_names else "by_filters"
        logger.info(f"Branching decision: {mode.upper()}")
        return {"mode": mode}

    @step(hide_output=True)
    def get_realm(
        mode_info: dict,
        payload: RealmUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> list[dict]:
        """Récupère les realms selon le mode."""
        # ... ton code existant ...
        return update_realms

    @step
    def group_realms_by_buhub(updated_realms: list[dict]) -> list[dict]:
        """Regroupe les realms par buhub_account_number."""
        grouped = defaultdict(list)
        
        for realm in updated_realms:
            buhub_number = realm["buhub_account_number"]
            grouped[buhub_number].append(realm)
        
        result = [
            {
                "buhub_account_number": buhub,
                "realms": realms
            }
            for buhub, realms in grouped.items()
        ]
        
        logger.info(f"Grouped {len(updated_realms)} realms into {len(result)} buhub groups")
        return result

    @step(hide_output=True)
    def get_schematics_api_keys(
        buhub_groups: list[dict],
        vault: Vault = depends(vault_dependency),
    ) -> dict[str, str]:
        """Récupère les API keys pour chaque buhub_number unique."""
        api_keys = {}
        unique_buhubs = {group["buhub_account_number"] for group in buhub_groups}
        
        logger.info(f"Loading API keys for {len(unique_buhubs)} unique buhub accounts")
        
        for buhub_number in unique_buhubs:
            api_keys[buhub_number] = vault.get_secret(
                f"{buhub_number}/sid-account-realm",
                mount_point="tbmsid"
            )["api_key"]
            logger.info(f"✓ API key loaded for BUHUB {buhub_number}")
        
        return api_keys

    # ========== Helper step avec dependency ==========
    
    @step
    def _get_workspace_for_buhub(
        realms: list[dict],
        buhub_number: str,
        schematics_api_key: str,  # ← IMPORTANT : avant le dependency
        tf: SchematicsBackend = depends(schematics_backend_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> dict:
        """
        Get workspace for a buhub avec dependency injection.
        
        CRITICAL: schematics_api_key MUST be before tf dependency
        so the dependency resolver can use it.
        """
        logger.info(f"Processing BUHUB {buhub_number} with {len(realms)} realms")
        
        first_realm = realms[0]
        
        try:
            workspace_in_db = get_souscription_by_name(
                first_realm['realm_name'], 
                db_session
            )
            
            if not workspace_in_db:
                raise ValueError(
                    f"Workspace not found in DB for {first_realm['realm_name']}"
                )
            
            # Utiliser le backend injecté
            workspace = tf.workspaces.get_by_id(workspace_in_db.id)
            status = SchematicsWorkspaceStatus(workspace.status)
            
            logger.info(f"✓ Workspace {workspace_in_db.name} status: {status.value}")
            
            if status.value not in ["ACTIVE", "INACTIVE"]:
                raise ValueError(f"Invalid status: {status.value}")
            
            return {
                "buhub_account_number": buhub_number,
                "workspace_id": workspace_in_db.id,
                "workspace_name": workspace_in_db.name,
                "workspace_status": status.value,
                "realms": realms,
                "realm_count": len(realms)
            }
            
        except Exception as e:
            logger.error(f"✗ Failed BUHUB {buhub_number}: {str(e)}")
            raise

    # ========== Task wrapper pour dynamic mapping ==========
    
    @task
    def process_buhub_workspace(
        buhub_group: dict,
        api_keys: dict[str, str],
    ) -> dict:
        """
        Task wrapper pour traiter un buhub avec dynamic task mapping.
        Appelle le step _get_workspace_for_buhub qui utilise les dependencies.
        """
        buhub_number = buhub_group["buhub_account_number"]
        realms = buhub_group["realms"]
        
        # Récupérer l'API key
        api_key = api_keys.get(buhub_number)
        if not api_key:
            raise ValueError(f"No API key found for buhub {buhub_number}")
        
        # ✅ Appeler le step avec l'API key
        result = _get_workspace_for_buhub(
            realms=realms,
            buhub_number=buhub_number,
            schematics_api_key=api_key  # ← Passé au step
        )
        
        return result

    # ========== DAG Flow ==========
    
    mode_info = detect_extraction_mode()
    updated_realms = get_realm(mode_info=mode_info)
    buhub_groups = group_realms_by_buhub(updated_realms)
    api_keys = get_schematics_api_keys(buhub_groups)
    
    # Dynamic Task Mapping : une tâche par buhub
    workspace_results = process_buhub_workspace.partial(
        api_keys=api_keys
    ).expand(
        buhub_group=buhub_groups
    )
    
    # Dépendances
    mode_info >> updated_realms >> buhub_groups >> api_keys >> workspace_results


dag_parallel_update()










@task
def process_buhub_workspace(
    buhub_group: dict,
    api_keys: dict[str, str],
    db_session: SASession = depends(sqlalchemy_session_dependency)
) -> dict:
    """
    Traite un groupe de realms partageant le même buhub_number.
    """
    from airflow.operators.python import get_current_context
    
    buhub_number = buhub_group["buhub_account_number"]
    realms = buhub_group["realms"]
    
    logger.info(f"Processing BUHUB {buhub_number} with {len(realms)} realms")
    
    # Récupérer l'API key
    api_key = api_keys.get(buhub_number)
    if not api_key:
        raise ValueError(f"No API key found for buhub {buhub_number}")
    
    # ✅ Stocker l'API key dans le contexte Airflow pour que le dependency puisse la lire
    context = get_current_context()
    context['schematics_api_key'] = api_key
    
    # ✅ Appeler une fonction qui utilise le dependency
    result = _process_workspace_with_dependency(
        realms=realms,
        buhub_number=buhub_number,
        db_session=db_session
    )
    
    return result


@step
def _process_workspace_with_dependency(
    realms: list[dict],
    buhub_number: str,
    tf: SchematicsBackend = depends(schematics_backend_dependency),  # ← Lit l'API key du contexte
    db_session: SASession = depends(sqlalchemy_session_dependency)
) -> dict:
    """
    Fonction helper qui utilise le dependency.
    Le schematics_backend_dependency doit lire l'API key depuis le contexte Airflow.
    """
    first_realm = realms[0]
    
    try:
        workspace_in_db = get_souscription_by_name(
            first_realm['realm_name'], 
            db_session
        )
        
        if not workspace_in_db:
            raise ValueError(f"Workspace not found for {first_realm['realm_name']}")
        
        # ✅ Utiliser le backend
        workspace = tf.workspaces.get_by_id(workspace_in_db.id)
        status = SchematicsWorkspaceStatus(workspace.status)
        
        logger.info(f"✓ Workspace {workspace_in_db.name} status: {status.value}")
        
        if status.value not in ["ACTIVE", "INACTIVE"]:
            raise ValueError(f"Invalid status: {status.value}")
        
        return {
            "buhub_account_number": buhub_number,
            "workspace_id": workspace_in_db.id,
            "workspace_name": workspace_in_db.name,
            "workspace_status": status.value,
            "realms": realms,
            "realm_count": len(realms)
        }
        
    except Exception as e:
        logger.error(f"✗ Failed BUHUB {buhub_number}: {str(e)}")
        raise
    
    
    
    
@task
def process_buhub_workspace(
    buhub_group: dict,
    api_keys: dict[str, str],
) -> dict:
    """
    Traite un groupe de realms partageant le même buhub_number.
    """
    buhub_number = buhub_group["buhub_account_number"]
    realms = buhub_group["realms"]
    
    logger.info(f"Processing BUHUB {buhub_number} with {len(realms)} realms")
    
    # Récupérer l'API key
    api_key = api_keys.get(buhub_number)
    if not api_key:
        raise ValueError(f"No API key found for buhub {buhub_number}")
    
    # ✅ Appeler le step en passant l'API key
    result = _get_workspace_for_buhub(
        realms=realms,
        buhub_number=buhub_number,
        schematics_api_key=api_key  # ← Passer l'API key ici
    )
    
    return result


@step
def _get_workspace_for_buhub(
    realms: list[dict],
    buhub_number: str,
    schematics_api_key: str,  # ← L'API key DOIT être avant le dependency
    tf: SchematicsBackend = depends(schematics_backend_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency)
) -> dict:
    """
    Get workspace for a buhub.
    
    IMPORTANT: schematics_api_key doit être passé AVANT tf pour que
    le dependency puisse l'utiliser lors de l'injection.
    """
    logger.info(f"Using API key for BUHUB {buhub_number}")
    
    first_realm = realms[0]
    
    try:
        workspace_in_db = get_souscription_by_name(
            first_realm['realm_name'], 
            db_session
        )
        
        if not workspace_in_db:
            raise ValueError(f"Workspace not found for {first_realm['realm_name']}")
        
        # ✅ Utiliser le backend injecté
        workspace = tf.workspaces.get_by_id(workspace_in_db.id)
        status = SchematicsWorkspaceStatus(workspace.status)
        
        logger.info(f"✓ Workspace {workspace_in_db.name} status: {status.value}")
        
        if status.value not in ["ACTIVE", "INACTIVE"]:
            raise ValueError(f"Invalid status: {status.value}")
        
        return {
            "buhub_account_number": buhub_number,
            "workspace_id": workspace_in_db.id,
            "workspace_name": workspace_in_db.name,
            "workspace_status": status.value,
            "realms": realms,
            "realm_count": len(realms)
        }
        
    except Exception as e:
        logger.error(f"✗ Failed BUHUB {buhub_number}: {str(e)}")
        raise