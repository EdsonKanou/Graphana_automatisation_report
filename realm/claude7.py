from airflow.decorators import task_group

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
            
            logger.info(f"Filters: Code BU={code_bu_values}, Env={environment_values}")
            
            try:
                query = db_session.query(Realm)
                if payload.realm_code_bu != "ALL":
                    query = query.filter(Realm.code_bu.in_(code_bu_values))
                query = query.filter(Realm.env_type.in_(environment_values))
                
                realm_objects: list[Realm] = query.all()
                
                if not realm_objects:
                    raise ValueError(f"No realms found matching criteria")
                
                for realm in realm_objects:
                    realms_list.append({
                        "realm_name": realm.name,
                        "subscription_id": realm.subscription_id,
                        "code_bu": realm.code_bu,
                        "environment_type": realm.env_type,
                        "buhub_account_number": realm.buhub_account_number
                    })
                
                logger.info(f"Found {len(realm_objects)} realms")
                
            except Exception as e:
                logger.error(f"Database query failed: {str(e)}")
                raise
        
        logger.info(f"✓ Retrieved {len(realms_list)} realms")
        return realms_list

    @step
    def group_realms_by_buhub(realms_list: list[dict]) -> list[dict]:
        """
        Regroupe les realms par buhub_account_number.
        """
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
        for group in buhub_groups:
            logger.info(f"  - BUHUB {group['buhub_account_number']}: {group['realm_count']} realms")
        
        return buhub_groups

    # ========== TaskGroup pour traiter chaque buhub en parallèle ==========
    
    @task_group(group_id="process_buhub_workspaces")
    def process_all_buhubs(buhub_groups_data: list[dict]):
        """
        TaskGroup qui crée une tâche par buhub.
        Chaque tâche valide le workspace et génère le plan Terraform.
        """
        
        @step
        def validate_and_plan_buhub_workspace(
            buhub_group: dict,
            buhub_account_number: str,  # ✅ Nécessaire pour tf_backend_adapter_dependency
            tf_backend: SchematicsTerraformGateway = depends(tf_backend_adapter_dependency),
            db_session: SASession = depends(sqlalchemy_session_dependency)
        ) -> dict:
            """
            Valide le workspace et génère le plan Terraform pour un buhub.
            
            Args:
                buhub_group: Données du groupe buhub
                buhub_account_number: Numéro buhub (pour le dependency)
                tf_backend: Backend Terraform (injecté via dependency)
                db_session: Session DB
            """
            first_realm_name = buhub_group["first_realm_name"]
            realms = buhub_group["realms"]
            
            logger.info(
                f"Processing BUHUB {buhub_account_number} "
                f"(reference: {first_realm_name}, {len(realms)} realms)"
            )
            
            try:
                # === ÉTAPE 1 : Récupérer workspace depuis DB ===
                workspace_in_db = get_souscription_by_name(first_realm_name, db_session)
                
                if not workspace_in_db:
                    raise ValueError(
                        f"Workspace not found in DB for realm {first_realm_name} "
                        f"(BUHUB {buhub_account_number})"
                    )
                
                logger.info(f"✓ Found workspace in DB: {workspace_in_db.name}")
                
                # === ÉTAPE 2 : Vérifier existence dans Schematics ===
                workspace_exists = tf_backend.check_workspace_exists(workspace_in_db.id)
                
                if not workspace_exists:
                    raise ValueError(
                        f"Workspace {workspace_in_db.name} does not exist in Schematics"
                    )
                
                logger.info(f"✓ Workspace exists in Schematics")
                
                # === ÉTAPE 3 : Récupérer et valider le statut ===
                workspace = tf_backend.workspaces.get_by_id(workspace_in_db.id)
                status = SchematicsWorkspaceStatus(workspace.status)
                
                logger.info(f"✓ Workspace status: {status.value}")
                
                if status.value not in ["ACTIVE", "INACTIVE"]:
                    raise ValueError(f"Invalid workspace status: {status.value}")
                
                # === ÉTAPE 4 : Générer le plan Terraform ===
                logger.info(f"Generating Terraform plan for workspace {workspace_in_db.name}")
                
                terraform_variables = None  # Ou construire selon besoin
                
                workspace_status = tf_backend.plan_workspace(
                    workspace_id=workspace_in_db.id,
                    terraform_variables=terraform_variables
                )
                
                logger.info(f"✓ Plan generated successfully, status: {workspace_status}")
                
                # === RÉSULTAT ===
                result = {
                    "buhub_account_number": buhub_account_number,
                    "workspace_id": workspace_in_db.id,
                    "workspace_name": workspace_in_db.name,
                    "workspace_status": status.value,
                    "plan_status": workspace_status,
                    "realms": realms,
                    "realm_count": len(realms),
                    "reference_realm_name": first_realm_name
                }
                
                logger.info(
                    f"✓ Successfully processed BUHUB {buhub_account_number}: "
                    f"workspace={workspace_in_db.name}, {len(realms)} realms"
                )
                
                return result
                
            except Exception as e:
                logger.error(
                    f"✗ Failed to process BUHUB {buhub_account_number}: {str(e)}"
                )
                raise
        
        # === Créer une tâche par buhub (PARALLÉLISATION) ===
        results = []
        for group in buhub_groups_data:
            buhub_number = group["buhub_account_number"]
            
            # ✅ Passer buhub_account_number explicitement pour le dependency
            result = validate_and_plan_buhub_workspace(
                buhub_group=group,
                buhub_account_number=buhub_number
            )
            results.append(result)
        
        return results

    # ========== DAG Flow ==========
    
    mode_info = detect_extraction_mode()
    realms_list = get_realms(mode_info=mode_info)
    buhub_groups = group_realms_by_buhub(realms_list)
    
    # ✅ Traiter tous les buhub en parallèle via TaskGroup
    workspace_results = process_all_buhubs(buhub_groups_data=buhub_groups)
    
    # Dépendances
    mode_info >> realms_list >> buhub_groups >> workspace_results


dag_parallel_update()