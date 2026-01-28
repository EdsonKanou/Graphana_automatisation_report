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
        mode = mode_info["mode"]
        update_realms: list[dict] = []
        
        if mode == "by_names":
            missing_realms: list[str] = []
            for realm_name in payload.realm_names:
                realm = get_realm_by_name(realm_name, db_session)
                if realm is None:
                    missing_realms.append(realm_name)
                else:
                    update_realms.append({
                        "realm_name": realm.name,
                        "subscription_id": realm.subscription_id,
                        "code_bu": realm.code_bu,
                        "environment_type": realm.env_type,
                        "buhub_account_number": realm.buhub_account_number
                    })
            
            if missing_realms:
                logger.warning(f"Missing realms: {missing_realms}")
            if not update_realms:
                raise Exception("No realms were successfully retrieved")
                
        else:  # by_filters
            code_bu_values = payload.get_code_bu_filter_values()
            environment_values = payload.get_environment_filter_values()
            
            logger.info(f"Applied filters: Code BU IN {code_bu_values}, Env IN {environment_values}")
            
            try:
                query = db_session.query(Realm)
                if payload.realm_code_bu != "ALL":
                    query = query.filter(Realm.code_bu.in_(code_bu_values))
                query = query.filter(Realm.env_type.in_(environment_values))
                
                realm_list: list[Realm] = query.all()
                
                if not realm_list:
                    error_msg = (
                        f"No realm found matching criteria: "
                        f"code_bu={payload.realm_code_bu}, "
                        f"environment={payload.realm_environment}"
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                
                logger.info(f"Found {len(realm_list)} realms matching criteria")
                
                for realm in realm_list:
                    update_realms.append({
                        "realm_name": realm.name,
                        "subscription_id": realm.subscription_id,
                        "code_bu": realm.code_bu,
                        "environment_type": realm.env_type,
                        "buhub_account_number": realm.buhub_account_number
                    })
                    
            except Exception as e:
                logger.error(f"Database query failed: {str(e)}")
                raise
        
        logger.info(f"OK - Retrieved {len(update_realms)} realms in total")
        return update_realms

    @step
    def group_realms_by_buhub(updated_realms: list[dict]) -> list[dict]:
        """
        Regroupe les realms par buhub_account_number.
        
        Returns:
            List[dict]: [
                {
                    "buhub_account_number": "2763704",
                    "realms": [
                        {"realm_name": "...", "subscription_id": "...", ...},
                        {"realm_name": "...", "subscription_id": "...", ...}
                    ]
                },
                ...
            ]
        """
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
        for group in result:
            logger.info(
                f"  - BUHUB {group['buhub_account_number']}: "
                f"{len(group['realms'])} realms"
            )
        
        return result

    @step(hide_output=True)
    def get_schematics_api_keys(
        buhub_groups: list[dict],
        vault: Vault = depends(vault_dependency),
    ) -> dict[str, str]:
        """
        Récupère les API keys pour chaque buhub_number unique.
        
        Returns:
            dict: {"2763704": "api_key_xxx", "2763705": "api_key_yyy"}
        """
        api_keys = {}
        unique_buHub = {group["buhub_account_number"] for group in buhub_groups}
        
        logger.info(f"Loading API keys for {len(unique_buhubs)} unique buhub accounts")
        
        for buhub_number in unique_buhubs:
            api_keys[buhub_number] = vault.get_secret(
                f"{buhub_number}/sid-account-realm",
                mount_point="tbmsid"
            )["api_key"]
            logger.info(f"✓ Schematics API key loaded for BUHUB {buhub_number}")
        
        return api_keys

    @task
    def process_buhub_workspace(
        buhub_group: dict,
        api_keys: dict[str, str],
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> dict:
        """
        Traite un groupe de realms partageant le même buhub_number.
        Récupère le workspace Schematics une seule fois pour tout le groupe.
        
        Args:
            buhub_group: {"buhub_account_number": "xxx", "realms": [...]}
            api_keys: {"xxx": "api_key_xxx", ...}
            
        Returns:
            dict: {
                "buhub_account_number": "xxx",
                "workspace_id": "ws_xxx",
                "workspace_name": "workspace_name",
                "realms": [...]  # Liste des realms de ce buhub
            }
        """
        buhub_number = buhub_group["buhub_account_number"]
        realms = buhub_group["realms"]
        
        logger.info(f"Processing BUHUB {buhub_number} with {len(realms)} realms")
        
        # Récupérer l'API key pour ce buhub
        api_key = api_keys.get(buhub_number)
        if not api_key:
            raise ValueError(f"No API key found for buhub {buhub_number}")
        
        # Initialiser SchematicsBackend avec la clé spécifique
        tf = SchematicsBackend(api_key=api_key)
        
        # Prendre le premier realm pour récupérer le workspace
        # (tous les realms du même buhub partagent le même workspace)
        first_realm = realms[0]
        
        try:
            # Récupérer le workspace depuis la DB
            workspace_in_db = get_souscription_by_name(
                first_realm['realm_name'], 
                db_session
            )
            
            if not workspace_in_db:
                raise ValueError(
                    f"Workspace not found in database for realm {first_realm['realm_name']}"
                )
            
            # Vérifier le statut dans Schematics
            workspace = tf.workspaces.get_by_id(workspace_in_db.id)
            status = SchematicsWorkspaceStatus(workspace.status)
            
            logger.info(
                f"✓ Workspace {workspace_in_db.name} status: {status.value}"
            )
            
            if status.value not in ["ACTIVE", "INACTIVE"]:
                raise ValueError(
                    f"Workspace {workspace_in_db.name} has invalid status: {status.value}"
                )
            
            result = {
                "buhub_account_number": buhub_number,
                "workspace_id": workspace_in_db.id,
                "workspace_name": workspace_in_db.name,
                "workspace_status": status.value,
                "realms": realms,
                "realm_count": len(realms)
            }
            
            logger.info(
                f"✓ Successfully processed BUHUB {buhub_number}: "
                f"workspace={workspace_in_db.name}, {len(realms)} realms"
            )
            
            return result
            
        except Exception as e:
            logger.error(
                f"✗ Failed to process BUHUB {buhub_number}: {str(e)}"
            )
            raise

    # ========== DAG Flow ==========
    
    mode_info = detect_extraction_mode()
    updated_realms = get_realm(mode_info=mode_info)
    buhub_groups = group_realms_by_buhub(updated_realms)
    api_keys = get_schematics_api_keys(buhub_groups)
    
    # Dynamic Task Mapping : une tâche par buhub_group
    # S'exécutent en parallèle (jusqu'à max_active_tasks=4)
    workspace_results = process_buhub_workspace.partial(
        api_keys=api_keys
    ).expand(
        buhub_group=buhub_groups
    )
    
    # Dépendances
    mode_info >> updated_realms >> buhub_groups >> api_keys >> workspace_results


dag_parallel_update()