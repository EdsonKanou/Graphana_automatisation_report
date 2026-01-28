from airflow.decorators import task_group

@task_group(group_id="process_all_buhubs")
def process_all_buhubs_parallel(
    buhub_groups: list[dict],
    api_keys: dict[str, str]
):
    """
    TaskGroup qui traite tous les buhub en parallèle.
    """
    
    @step
    def process_buhub_workspace(
        buhub_group: dict,
        schematics_api_key: str,
        tf: SchematicsBackend = depends(schematics_backend_dependency),
        db_session: SASession = depends(sqlalchemy_session_dependency)
    ) -> dict:
        """Process un buhub."""
        buhub_number = buhub_group["buhub_account_number"]
        realms = buhub_group["realms"]
        
        logger.info(f"Processing BUHUB {buhub_number}")
        
        first_realm = realms[0]
        
        workspace_in_db = get_souscription_by_name(
            first_realm['realm_name'], 
            db_session
        )
        
        if not workspace_in_db:
            raise ValueError(f"Workspace not found")
        
        workspace = tf.workspaces.get_by_id(workspace_in_db.id)
        status = SchematicsWorkspaceStatus(workspace.status)
        
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
    
    # ✅ Créer une tâche par buhub
    results = []
    for group in buhub_groups:
        buhub_number = group["buhub_account_number"]
        api_key = api_keys[buhub_number]
        
        result = process_buhub_workspace(
            buhub_group=group,
            schematics_api_key=api_key
        )
        results.append(result)
    
    return results


# Flow
workspace_results = process_all_buhubs_parallel(
    buhub_groups=buhub_groups,
    api_keys=api_keys
)