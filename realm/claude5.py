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
        """Détecte le mode d'extraction (by_names ou by_filters)."""
        mode = "by_names" if payload.realm_names else "by_filters"
        logger.info(f"Extraction mode: {mode.upper()}")
        return {"mode": mode}

    @step(hide_output=True)
    def get_realms(
        mode_info: dict,
        payload: RealmUpdatePayload = depends(payload_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> list[dict]:
        """
        Récupère les realms selon le mode d'extraction.
        
        Returns:
            List[dict]: Liste des realms avec leurs infos
        """
        mode = mode_info["mode"]
        realms_list: list[dict] = []
        
        if mode == "by_names":
            # Mode : extraction par noms explicites
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
                raise Exception("No realms were found in database")
        
        else:  # by_filters
            # Mode : extraction par filtres
            code_bu_values = payload.get_code_bu_filter_values()
            environment_values = payload.get_environment_filter_values()
            
            logger.info(
                f"Filters applied - Code BU: {code_bu_values}, "
                f"Environment: {environment_values}"
            )
            
            try:
                query = db_session.query(Realm)
                
                if payload.realm_code_bu != "ALL":
                    query = query.filter(Realm.code_bu.in_(code_bu_values))
                
                query = query.filter(Realm.env_type.in_(environment_values))
                
                realm_objects: list[Realm] = query.all()
                
                if not realm_objects:
                    raise ValueError(
                        f"No realms found matching criteria: "
                        f"code_bu={payload.realm_code_bu}, "
                        f"environment={payload.realm_environment}"
                    )
                
                for realm in realm_objects:
                    realms_list.append({
                        "realm_name": realm.name,
                        "subscription_id": realm.subscription_id,
                        "code_bu": realm.code_bu,
                        "environment_type": realm.env_type,
                        "buhub_account_number": realm.buhub_account_number
                    })
                
                logger.info(f"Found {len(realm_objects)} realms matching filters")
                
            except Exception as e:
                logger.error(f"Database query failed: {str(e)}")
                raise
        
        logger.info(f"✓ Retrieved {len(realms_list)} realms for processing")
        return realms_list

    @step
    def group_realms_by_buhub(realms_list: list[dict]) -> list[dict]:
        """
        Regroupe les realms par buhub_account_number.
        Chaque buhub correspond à un workspace Schematics unique.
        
        Returns:
            List[dict]: [
                {
                    "buhub_account_number": "2763704",
                    "realm_count": 3,
                    "first_realm_name": "r10021000770",  # Pour lookup workspace
                    "realms": [...]
                },
                ...
            ]
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
                "first_realm_name": realms[0]["realm_name"],  # Premier realm comme référence
                "realms": realms
            })
        
        logger.info(
            f"✓ Grouped {len(realms_list)} realms into "
            f"{len(buhub_groups)} unique buhub accounts"
        )
        
        for group in buhub_groups:
            logger.info(
                f"  - BUHUB {group['buhub_account_number']}: "
                f"{group['realm_count']} realms (ref: {group['first_realm_name']})"
            )
        
        return buhub_groups

    @step
    def get_and_validate_workspace(
        buhub_group: dict,
        tf_backend: SchematicsBackend = depends(schematics_backend_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> dict:
        """
        Récupère et valide le workspace Schematics pour un buhub.
        
        Cette fonction est appelée EN PARALLÈLE pour chaque buhub unique.
        Le tf_backend_dependency gère automatiquement l'API key via le buhub_account_number.
        
        Args:
            buhub_group: {
                "buhub_account_number": "xxx",
                "first_realm_name": "yyy",
                "realms": [...]
            }
        
        Returns:
            dict: Workspace details avec les realms associés
        """
        buhub_number = buhub_group["buhub_account_number"]
        first_realm_name = buhub_group["first_realm_name"]
        realms = buhub_group["realms"]
        
        logger.info(
            f"Processing BUHUB {buhub_number} "
            f"(reference realm: {first_realm_name}, {len(realms)} realms total)"
        )
        
        try:
            # Récupérer le workspace depuis la DB
            # Tous les realms d'un même buhub partagent le même workspace
            workspace_in_db = get_souscription_by_name(
                first_realm_name, 
                db_session
            )
            
            if not workspace_in_db:
                raise ValueError(
                    f"Workspace not found in database for realm {first_realm_name} "
                    f"(BUHUB {buhub_number})"
                )
            
            logger.info(
                f"✓ Found workspace in DB: {workspace_in_db.name} "
                f"(id: {workspace_in_db.id})"
            )
            
            # Vérifier que le workspace existe toujours dans Schematics
            workspace_exists = tf_backend.check_workspace_exists(workspace_in_db.id)
            
            if not workspace_exists:
                raise ValueError(
                    f"Workspace {workspace_in_db.name} (id: {workspace_in_db.id}) "
                    f"does not exist in Schematics backend"
                )
            
            logger.info(f"✓ Workspace exists in Schematics backend")
            
            # Récupérer les détails complets du workspace
            workspace = tf_backend.workspaces.get_by_id(workspace_in_db.id)
            status = SchematicsWorkspaceStatus(workspace.status)
            
            logger.info(f"✓ Workspace status: {status.value}")
            
            # Valider le statut
            if status.value not in ["ACTIVE", "INACTIVE"]:
                raise ValueError(
                    f"Workspace {workspace_in_db.name} has invalid status: {status.value}. "
                    f"Expected ACTIVE or INACTIVE."
                )
            
            result = {
                "buhub_account_number": buhub_number,
                "workspace_id": workspace_in_db.id,
                "workspace_name": workspace_in_db.name,
                "workspace_status": status.value,
                "reference_realm_name": first_realm_name,
                "realms": realms,
                "realm_count": len(realms)
            }
            
            logger.info(
                f"✓ Successfully validated BUHUB {buhub_number}: "
                f"workspace={workspace_in_db.name}, "
                f"status={status.value}, "
                f"{len(realms)} realms"
            )
            
            return result
            
        except Exception as e:
            logger.error(
                f"✗ Failed to process BUHUB {buhub_number} "
                f"(realm: {first_realm_name}): {str(e)}"
            )
            raise

    # ========== DAG Flow avec parallélisation ==========
    
    mode_info = detect_extraction_mode()
    realms_list = get_realms(mode_info=mode_info)
    buhub_groups = group_realms_by_buhub(realms_list)
    
    # ✅ PARALLÉLISATION : Une tâche par buhub
    # Le tf_backend_dependency gère automatiquement l'API key pour chaque buhub
    # Airflow exécutera jusqu'à max_active_tasks=4 en parallèle
    workspace_results = []
    for buhub_group in buhub_groups:
        result = get_and_validate_workspace(buhub_group=buhub_group)
        workspace_results.append(result)
    
    # Dépendances
    mode_info >> realms_list >> buhub_groups >> workspace_results


dag_parallel_update()
```

---

## 🎯 Changements majeurs vs version précédente

### ✅ **Supprimé (plus nécessaire)** :
- ❌ `get_schematics_api_keys()` → géré par `schematics_backend_dependency`
- ❌ `prepare_buhub_accounts_with_api_keys()` → idem
- ❌ `enrich_buhub_groups_with_api_keys()` → idem
- ❌ Gestion manuelle de `vault` et des API keys
- ❌ Passage explicite de `schematics_api_key` en paramètre

### ✅ **Ajouté/Amélioré** :
- ✅ `check_workspace_exists()` → validation que le workspace existe dans Schematics
- ✅ Pattern plus propre avec dependency injection complète
- ✅ Meilleure gestion d'erreurs avec logs détaillés
- ✅ Structure plus simple : 3 steps au lieu de 5+

---

## 📊 Flow du DAG
```
detect_extraction_mode()
        ↓
get_realms()  [8 realms]
        ↓
group_realms_by_buhub()  [3 buhub groups]
        ↓
get_and_validate_workspace()  ← PARALLÈLE (max 4 en même temps)
   ↓        ↓        ↓
 BUHUB1   BUHUB2   BUHUB3
   ↓        ↓        ↓
workspace_results  [3 workspaces validés]