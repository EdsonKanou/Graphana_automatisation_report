from airflow.decorators import task_group

@task
def prepare_buhub_task(buhub_group: dict, api_keys: dict) -> dict:
    """Prépare les données pour le TaskGroup."""
    buhub_number = buhub_group["buhub_account_number"]
    return {
        "buhub_number": buhub_number,
        "api_key": api_keys[buhub_number],
        "realms": buhub_group["realms"]
    }

@task_group
def process_buhub_group(buhub_config: dict):
    """TaskGroup qui traite un buhub."""
    
    @step
    def get_workspace(
        config: dict,
        schematics_api_key: str = buhub_config["api_key"],  # ← Injecté depuis le config
        tf: SchematicsBackend = depends(schematics_backend_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> dict:
        # ... logique de traitement ...
        pass
    
    return get_workspace(config=buhub_config)

# Flow
prepared_configs = prepare_buhub_task.partial(api_keys=api_keys).expand(
    buhub_group=buhub_groups
)

results = process_buhub_group.expand(buhub_config=prepared_configs)






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
        mode = "by_names" if payload.realm_names else "by_filters"
        logger.info(f"Branching decision: {mode.upper()}")
        return {"mode": mode}

    @step(hide_output=True)
    def get_realm(
        mode_info: dict,
        payload: RealmUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> list[dict]:
        # ... ton code existant ...
        return update_realms

    @step
    def group_realms_by_buhub(updated_realms: list[dict]) -> list[dict]:
        grouped = defaultdict(list)
        for realm in updated_realms:
            buhub_number = realm["buhub_account_number"]
            grouped[buhub_number].append(realm)
        
        result = [
            {"buhub_account_number": buhub, "realms": realms}
            for buhub, realms in grouped.items()
        ]
        
        logger.info(f"Grouped into {len(result)} buhub groups")
        return result

    @step(hide_output=True)
    def get_schematics_api_keys(
        buhub_groups: list[dict],
        vault: Vault = depends(vault_dependency),
    ) -> dict[str, str]:
        api_keys = {}
        unique_buhubs = {group["buhub_account_number"] for group in buhub_groups}
        
        for buhub_number in unique_buhubs:
            api_keys[buhub_number] = vault.get_secret(
                f"{buhub_number}/sid-account-realm",
                mount_point="tbmsid"
            )["api_key"]
            logger.info(f"✓ API key loaded for BUHUB {buhub_number}")
        
        return api_keys

    @step
    def process_all_buhub_workspaces(
        buhub_groups: list[dict],
        api_keys: dict[str, str],
        schematics_api_key: str,  # ← Paramètre requis par le dependency
        tf: SchematicsBackend = depends(schematics_backend_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> list[dict]:
        """
        Traite tous les buhub groups.
        
        IMPORTANT: On passe schematics_api_key mais on l'utilise pas directement,
        c'est juste pour que le dependency schematics_backend_dependency puisse le voir.
        On va recréer tf pour chaque buhub avec la bonne API key.
        """
        results = []
        failed_buhubs = []
        
        for buhub_group in buhub_groups:
            buhub_number = buhub_group["buhub_account_number"]
            realms = buhub_group["realms"]
            
            logger.info(f"Processing BUHUB {buhub_number} with {len(realms)} realms")
            
            try:
                # Récupérer l'API key spécifique pour ce buhub
                current_api_key = api_keys[buhub_number]
                
                # ⚠️ PROBLÈME : On ne peut pas réinstancier tf ici
                # Il faut appeler une sous-fonction
                result = _process_single_buhub(
                    realms=realms,
                    buhub_number=buhub_number,
                    schematics_api_key=current_api_key,
                    db_session=db_session
                )
                
                results.append(result)
                logger.info(f"✓ BUHUB {buhub_number} processed")
                
            except Exception as e:
                logger.error(f"✗ Failed BUHUB {buhub_number}: {str(e)}")
                failed_buhubs.append(buhub_number)
        
        if not results:
            raise Exception(f"All buhub failed: {failed_buhubs}")
        
        if failed_buhubs:
            logger.warning(f"Some buhub failed: {failed_buhubs}")
        
        logger.info(f"✓ Processed {len(results)}/{len(buhub_groups)} buhub groups")
        return results

    @step
    def _process_single_buhub(
        realms: list[dict],
        buhub_number: str,
        schematics_api_key: str,  # ← AVANT le dependency
        tf: SchematicsBackend = depends(schematics_backend_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> dict:
        """
        Process un seul buhub avec son API key spécifique.
        """
        logger.info(f"Processing BUHUB {buhub_number} with {len(realms)} realms")
        
        first_realm = realms[0]
        
        workspace_in_db = get_souscription_by_name(
            first_realm['realm_name'], 
            db_session
        )
        
        if not workspace_in_db:
            raise ValueError(f"Workspace not found for {first_realm['realm_name']}")
        
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

    # ========== DAG Flow ==========
    
    mode_info = detect_extraction_mode()
    updated_realms = get_realm(mode_info=mode_info)
    buhub_groups = group_realms_by_buhub(updated_realms)
    api_keys = get_schematics_api_keys(buhub_groups)
    
    # ⚠️ On doit passer UNE api_key pour satisfaire le dependency
    # On prend la première, mais elle sera remplacée dans la boucle
    first_api_key = list(api_keys.values())[0]
    
    workspace_results = process_all_buhub_workspaces(
        buhub_groups=buhub_groups,
        api_keys=api_keys,
        schematics_api_key=first_api_key  # ← Requis par le dependency
    )
    
    mode_info >> updated_realms >> buhub_groups >> api_keys >> workspace_results


dag_parallel_update()














##################


@task
def process_buhub_workspace(
    buhub_group: dict,
    api_keys: dict[str, str],
) -> dict:
    """
    Task pour traiter un buhub en parallèle.
    """
    from bp2i_airflow_library.dependencies.solver import solve_dependencies
    from bp2i_terraform.backends.schematics import SchematicsBackend
    
    buhub_number = buhub_group["buhub_account_number"]
    realms = buhub_group["realms"]
    
    logger.info(f"Processing BUHUB {buhub_number} with {len(realms)} realms")
    
    # Récupérer l'API key
    api_key = api_keys.get(buhub_number)
    if not api_key:
        raise ValueError(f"No API key found for buhub {buhub_number}")
    
    # ✅ Résoudre les dependencies manuellement
    # Injecter l'API key dans le contexte pour le dependency
    dependencies_context = {
        'schematics_api_key': api_key
    }
    
    # Résoudre le schematics_backend_dependency avec l'API key
    tf = solve_dependencies(
        schematics_backend_dependency,
        context=dependencies_context
    )
    
    # Résoudre sqlalchemy_session_dependency
    db_session = solve_dependencies(sqlalchemy_session_dependency)
    
    first_realm = realms[0]
    
    try:
        workspace_in_db = get_souscription_by_name(
            first_realm['realm_name'], 
            db_session
        )
        
        if not workspace_in_db:
            raise ValueError(f"Workspace not found for {first_realm['realm_name']}")
        
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
    finally:
        # Fermer la session DB
        if db_session:
            db_session.close()


# Flow avec dynamic mapping
workspace_results = process_buhub_workspace.partial(
    api_keys=api_keys
).expand(
    buhub_group=buhub_groups
)