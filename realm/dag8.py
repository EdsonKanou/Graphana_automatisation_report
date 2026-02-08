def tf_backend_adapter_dependency(
    buhub_group: dict = None,  # ← Accepte le groupe complet
    buhub_account_number: str = None,  # ← Ou le numéro seul
    payload: BaseModel = depends(payload_dependency),
    db_session: SASession = depends(sqlalchemy_session_dependency),
    tf_ramen: RamenBackend = depends(smart_ramen_backend_dependency),
    airflow_context: AirflowContext = depends(airflow_context_dependency),
    vault: Vault = depends(vault_dependency),
) -> SchematicsTerraformGateway | RamenTerraformGateway:
    """
    Dependency qui accepte soit buhub_group, soit buhub_account_number.
    Permet la flexibilité pour différents cas d'usage.
    """
    
    # ✅ Extraire buhub_account_number depuis buhub_group si nécessaire
    if buhub_account_number is None and buhub_group is not None:
        buhub_account_number = buhub_group["buhub_account_number"]
    
    # Validation
    if buhub_account_number is None:
        raise ValueError(
            "Either buhub_group or buhub_account_number must be provided "
            "to tf_backend_adapter_dependency"
        )
    
    logger.info(f"Initializing tf_backend for BUHUB {buhub_account_number}")
    
    try:
        # Récupérer l'API key Schematics pour ce buhub
        schematics_api_key = get_schematics_api_key(vault, buhub_account_number)
        
        # Créer le backend Schematics avec l'API key
        tf_schematics = schematics_backend_dependency(
            schematics_api_key,
            airflow_context=airflow_context
        )
        
        # Créer et retourner le gateway
        tf_backend = SchematicsTerraformGateway(tf_schematics, vault)
        
        logger.info(f"✓ tf_backend initialized for BUHUB {buhub_account_number}")
        
        return tf_backend
        
    except Exception as e:
        logger.error(f"✗ Failed to initialize tf_backend for BUHUB {buhub_account_number}: {str(e)}")
        raise