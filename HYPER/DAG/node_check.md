Voici les étapes complètes pour implémenter la vérification (check) que les nodes à ajouter ne sont pas déjà actifs (associés à un autre nodepool).

## 1. Création de la branche

```bash
git checkout main
git pull origin main
git checkout -b feature/add-node-active-check
```

## 2. Valeur du commit

```
feat: add active node check before adding nodes to nodepool

- Prevent adding nodes that are already active (associated to another nodepool)
- Add validation logic in update action "add" flow
- Check node status via agent subscription before proceeding
```

## 3. Code à implémenter

### Dans le fichier de logique principale (ex: `node_pool_handler.py` ou `update_handler.py`) :

```python
# Add this method to your handler class
def _check_nodes_not_active(self, nodepools: list, owner_bu: str) -> list:
    """
    Check that none of the nodes to be added are already active.
    
    Args:
        nodepools: List of nodepools with their nodes to add
        owner_bu: Owner business unit for the cluster
    
    Returns:
        List of declined reasons if any node is active
    """
    declined_reasons = []
    
    for nodepool in nodepools:
        nodepool_name = nodepool.get("nodepool name")
        nodes_to_add = nodepool.get("nodes", [])
        
        for node_name in nodes_to_add:
            try:
                # Step 1: Get the agent for this node name
                agent = self._get_agent_by_nodename(node_name, owner_bu)
                
                if agent is None:
                    declined_reasons.append(
                        f"Node '{node_name}' not found in nodepool '{nodepool_name}' - agent does not exist"
                    )
                    continue
                
                # Step 2: Check if there's an active Node linked to this agent's sub_id
                active_node = self._get_subscription_by_agent_subid(
                    agent.in_db.subid, 
                    raise_if_not_exist=False
                )
                
                # Step 3: If active node exists, node is already used
                if active_node is not None:
                    declined_reasons.append(
                        f"Active Node '{node_name}' found in nodepool '{nodepool_name}', "
                        f"with agent sub id {agent.in_db.subid} - Node already belongs to another nodepool"
                    )
                    
            except Exception as exc:
                declined_reasons.append(
                    f"Error checking node '{node_name}' in nodepool '{nodepool_name}': {str(exc)}"
                )
    
    return declined_reasons

def _get_agent_by_nodename(self, nodename: str, owner_bu: str):
    """Get agent instance by node name and owner BU."""
    try:
        agent = self.db_session.query(Agent).filter(
            Agent.owner.bu == owner_bu,
            Agent.name == nodename,
            Agent.mat_cluster_member != "yes",
            Agent.active == True  # We want to find active agents
        ).first()
        return agent
    except Exception as exc:
        raise Exception(f"Failed to get agent for nodename {nodename}: {str(exc)}")

def _get_subscription_by_agent_subid(self, agent_sub_id, raise_if_not_exist=False):
    """Get active Node subscription by agent sub id."""
    try:
        subscription = self.db_session.query(Node).filter(
            Node.agent_sub_id == agent_sub_id,
            Node.active == True
        ).first()
        return subscription
    except Exception as exc:
        if raise_if_not_exist:
            raise Exception(f"No Node instance found for agent sub id {agent_sub_id}: {str(exc)}")
        return None
```

### Dans la fonction principale d'update (ex: `process_update_action`):

```python
def process_update_action(self, pavload: dict) -> dict:
    """Process the update action from pavload."""
    
    update_action = pavload.get("update_action")  # Note: your JSON shows "update action"
    # Handle the space in key name if needed
    if not update_action:
        update_action = pavload.get("update action")
    
    # For "add" action, perform pre-check
    if update_action == "add":
        nodepools = pavload.get("nodepoels", [])  # Note: typo in your JSON "nodepoels"
        if not nodepools:
            nodepools = pavload.get("nodepools", [])
        
        owner_bu = self._get_owner_bu_from_cluster(pavload.get("nat cluster name"))
        
        # Perform the active node check
        declined_reasons = self._check_nodes_not_active(nodepools, owner_bu)
        
        # If any node is active, decline the entire operation
        if declined_reasons:
            return {
                "status": "declined",
                "reasons": declined_reasons,
                "message": "Cannot add nodes: some nodes are already active and belong to another nodepool"
            }
        
        # If check passes, proceed with the add operation
        return self._execute_add_nodes(pavload, nodepools)
    
    # Handle other update actions (remove, modify, etc.)
    elif update_action == "remove":
        return self._execute_remove_nodes(pavload)
    
    # ... other actions

def _get_owner_bu_from_cluster(self, cluster_name: str) -> str:
    """Extract owner BU from cluster details."""
    realm_details = realm_service.get_realm_details()
    # Assuming the owner.bu comes from realm details
    return realm_details.code.bu

def _execute_add_nodes(self, pavload: dict, nodepools: list) -> dict:
    """Execute the actual node addition after validation passes."""
    # Your existing add logic here
    results = []
    
    for nodepool in nodepools:
        nodepool_name = nodepool.get("nodepool name")
        nodes_to_add = nodepool.get("nodes", [])
        replicas = nodepool.get("replica", 0)
        
        # Logic to add nodes to the nodepool
        # ...
        results.append({
            "nodepool": nodepool_name,
            "nodes_added": nodes_to_add,
            "status": "success"
        })
    
    return {
        "status": "success",
        "action": "add",
        "results": results
    }
```

## 4. Logique expliquée étape par étape

| Étape | Description | Fonction utilisée |
|-------|-------------|-------------------|
| 1 | Vérifier si `update_action` est "add" | `if update_action == "add"` |
| 2 | Récupérer les nodepools et leurs nodes | `pavload.get("nodepoels")` |
| 3 | Pour chaque node, récupérer l'agent associé | `_get_agent_by_nodename()` |
| 4 | Vérifier si un Node actif existe avec `agent_sub_id` | `_get_subscription_by_agent_subid()` |
| 5 | Si Node actif trouvé → ajouter raison de refus | `declined_reasons.append()` |
| 6 | Si des refus existent → décliner l'opération | Return error response |
| 7 | Sinon → exécuter l'ajout | `_execute_add_nodes()` |

## 5. Test et validation

```bash
# Run tests
pytest tests/test_node_pool_update.py -v

# Linting
flake8 --max-line-length=120

# Type checking (if using mypy)
mypy --ignore-missing-imports
```

## 6. Commit et push

```bash
git add .
git commit -m "feat: add active node check before adding nodes to nodepool

- Implement _check_nodes_not_active method
- Add _get_agent_by_nodename helper
- Add _get_subscription_by_agent_subid helper
- Prevent adding nodes that already have active subscription
- Return declined reasons with clear error messages"

git push origin feature/add-node-active-check
```

## 7. Créer une Pull Request

```bash
gh pr create --title "feat: add active node check for update add action" \
  --body "## Description
- Adds validation step before adding nodes to nodepool
- Checks if nodes are already active (associated to another nodepool)
- Returns clear error messages if nodes are active

## Testing
- [ ] Test with inactive nodes → success
- [ ] Test with active nodes → declined with reason
- [ ] Test with non-existent nodes → error message

## Related Issue
#XXXX"
```

Cette implémentation suit la logique de votre code existant (utilisation de `Agent` et `Node` avec `agent_sub_id`) et ajoute la vérification demandée avant toute opération d'ajout.s