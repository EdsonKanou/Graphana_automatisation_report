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
        logger.info(f"Extraction mode: {mode.upper()}")
        return {"mode": mode}

    @step(hide_output=True)
    def get_realms(
        mode_info: dict,
        payload: RealmUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> list[dict]:
        """Récupère les realms."""
        # ... ton code existant ...
        return realms_list

    @step
    def group_realms_by_buhub(realms_list: list[dict]) -> list[dict]:
        """Regroupe par buhub."""
        from collections import defaultdict
        
        grouped = defaultdict(list)
        for realm in realms_list:
            buhub_number = realm["buhub_account_number"]
            grouped[buhub_number].append(realm)
        
        buhub_groups = []
        for buhub_number, realms in grouped.items():
            buhub_groups.append({
                "buhub_account_number": buhub_number,
                "realm_count": len(realms),
                "first_realm_name": realms[0]["realm_name"],
                "realms": realms
            })
        
        logger.info(f"✓ Grouped into {len(buhub_groups)} buhub accounts")
        return buhub_groups

    @step
    def get_and_validate_workspace(
        buhub_group: dict,
        tf_backend: SchematicsBackend = depends(schematics_backend_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> dict:
        """Valide le workspace pour un buhub."""
        buhub_number = buhub_group["buhub_account_number"]
        first_realm_name = buhub_group["first_realm_name"]
        realms = buhub_group["realms"]
        
        logger.info(f"Validating workspace for BUHUB {buhub_number}")
        
        workspace_in_db = get_souscription_by_name(first_realm_name, db_session)
        
        if not workspace_in_db:
            raise ValueError(f"Workspace not found for {first_realm_name}")
        
        workspace_exists = tf_backend.check_workspace_exists(workspace_in_db.id)
        if not workspace_exists:
            raise ValueError(f"Workspace {workspace_in_db.name} does not exist")
        
        workspace = tf_backend.workspaces.get_by_id(workspace_in_db.id)
        status = SchematicsWorkspaceStatus(workspace.status)
        
        if status.value not in ["ACTIVE", "INACTIVE"]:
            raise ValueError(f"Invalid status: {status.value}")
        
        logger.info(f"✓ Workspace {workspace_in_db.name} validated")
        
        return {
            "buhub_account_number": buhub_number,
            "workspace_id": workspace_in_db.id,
            "workspace_name": workspace_in_db.name,
            "workspace_status": status.value,
            "realms": realms,
            "realm_count": len(realms)
        }

    @step
    def generate_terraform_plan(
        workspace_details: dict,
        tf_backend: SchematicsBackend = depends(schematics_backend_dependency),
    ) -> dict:
        """Génère le plan Terraform."""
        workspace_id = workspace_details["workspace_id"]
        workspace_name = workspace_details["workspace_name"]
        
        logger.info(f"Generating plan for {workspace_name}")
        
        # Générer le plan
        workspace_status = tf_backend.plan_workspace(
            workspace_id=workspace_id,
            terraform_variables=None
        )
        
        logger.info(f"✓ Plan generated, status: {workspace_status}")
        
        return {
            **workspace_details,
            "plan_status": workspace_status,
            "plan_generated": True
        }

    # ========== DAG Flow ==========
    
    mode_info = detect_extraction_mode()
    realms_list = get_realms(mode_info=mode_info)
    buhub_groups = group_realms_by_buhub(realms_list)
    
    # Validation (parallèle)
    validated_workspaces = []
    for buhub_group in buhub_groups:
        validated = get_and_validate_workspace(buhub_group=buhub_group)
        validated_workspaces.append(validated)
    
    # Plan generation (parallèle)
    plan_results = []
    for workspace_details in validated_workspaces:
        plan_result = generate_terraform_plan(workspace_details=workspace_details)
        plan_results.append(plan_result)
    
    # Dépendances
    mode_info >> realms_list >> buhub_groups >> validated_workspaces >> plan_results


dag_parallel_update()