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
        """Récupère les realms selon le mode."""
        mode = mode_info["mode"]
        realms_list: list[dict] = []
        
        if mode == "by_names":
            missing_realms: list[str] = []
            
            for realm_name in payload.realm_names:
                realm = get_realm_by_name(realm_name, db_session)
                if realm is None:
                    missing_realms.append(realm_name)
                else:
                    realms_list.append({
                        "realm_name": realm.name,
                        "subscription_id": realm.subscription_id,
                        "code_bu": realm.code_bu,
                        "environment_type": realm.env_type,
                        "buhub_account_number": realm.buhub_account_number
                    })
            
            if missing_realms:
                logger.warning(f"Missing realms: {missing_realms}")
            if not realms_list:
                raise Exception("No realms found")
        
        else:  # by_filters
            code_bu_values = payload.get_code_bu_filter_values()
            environment_values = payload.get_environment_filter_values()
            
            try:
                query = db_session.query(Realm)
                if payload.realm_code_bu != "ALL":
                    query = query.filter(Realm.code_bu.in_(code_bu_values))
                query = query.filter(Realm.env_type.in_(environment_values))
                
                realm_objects: list[Realm] = query.all()
                
                if not realm_objects:
                    raise ValueError("No realms found matching criteria")
                
                for realm in realm_objects:
                    realms_list.append({
                        "realm_name": realm.name,
                        "subscription_id": realm.subscription_id,
                        "code_bu": realm.code_bu,
                        "environment_type": realm.env_type,
                        "buhub_account_number": realm.buhub_account_number
                    })
                
            except Exception as e:
                logger.error(f"Database query failed: {str(e)}")
                raise
        
        logger.info(f"✓ Retrieved {len(realms_list)} realms")
        return realms_list

    @step
    def process_all_buhub_workspaces(
        realms_list: list[dict],
        vault: Vault = depends(vault_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency),
        airflow_context: AirflowContext = depends(airflow_context_dependency),
    ) -> list[dict]:
        """
        Traite TOUS les buhub en séquentiel (un après l'autre).
        
        Pas de parallélisation, mais ÇA MARCHE.
        Si tu as 3 buhub, ils seront traités l'un après l'autre.
        """
        from collections import defaultdict
        
        # Grouper par buhub
        grouped = defaultdict(list)
        for realm in realms_list:
            buhub_number = realm["buhub_account_number"]
            grouped[buhub_number].append(realm)
        
        logger.info(f"Processing {len(grouped)} unique buhub accounts")
        
        results = []
        failed_buhubs = []
        
        # Traiter chaque buhub UN PAR UN
        for buhub_number, realms in grouped.items():
            logger.info(f"Processing BUHUB {buhub_number} ({len(realms)} realms)")
            
            try:
                # === Créer le tf_backend pour ce buhub ===
                schematics_api_key = get_schematics_api_key(vault, buhub_number)
                
                tf_schematics = schematics_backend_dependency(
                    schematics_api_key,
                    airflow_context=airflow_context
                )
                
                tf_backend = SchematicsTerraformGateway(tf_schematics, vault)
                
                logger.info(f"✓ tf_backend initialized for BUHUB {buhub_number}")
                
                # === Récupérer le workspace ===
                workspace_in_db = get_souscription_by_buhub(buhub_number, db_session)
                
                if not workspace_in_db:
                    raise ValueError(f"Workspace not found for buhub {buhub_number}")
                
                workspace_id = workspace_in_db.id
                workspace_name = workspace_in_db.name
                
                logger.info(f"✓ Found workspace: {workspace_name}")
                
                # === Vérifier existence dans Schematics ===
                workspace_exists = tf_backend.check_workspace_exists(workspace_id)
                if not workspace_exists:
                    raise ValueError(f"Workspace {workspace_name} does not exist in Schematics")
                
                logger.info(f"✓ Workspace exists in Schematics")
                
                # === Récupérer et valider le statut ===
                workspace = tf_backend.get_workspace_by_name(workspace_name)
                status = tf_backend.get_workspace_status(workspace)
                
                logger.info(f"✓ Workspace status: {status}")
                
                if status not in ("ACTIVE", "INACTIVE"):
                    raise ValueError(f"Invalid status: {status}")
                
                # === Générer le plan Terraform ===
                logger.info(f"Generating Terraform plan for workspace {workspace_name}...")
                
                workspace_status = tf_backend.plan_workspace(
                    workspace_id=workspace_id,
                    terraform_variables=None,
                )
                
                logger.info(f"✓ Plan generated, status: {workspace_status}")
                
                # === Résultat ===
                results.append({
                    "buhub_account_number": buhub_number,
                    "workspace_id": workspace_id,
                    "workspace_name": workspace_name,
                    "workspace_status": status,
                    "workspace_plan_status": workspace_status,
                    "realm_count": len(realms)
                })
                
                logger.info(f"✓ Successfully processed BUHUB {buhub_number}")