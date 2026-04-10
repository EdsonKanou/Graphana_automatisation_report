Voici une façon claire et structurée d’ajouter ton **check “node actif”** dans ton update `add`, en restant cohérent avec ton code existant 👇

---

# ✅ 1. Logique globale à ajouter

Tu es dans le cas :

```python
if payload.update_action == "add":
```

👉 Pour chaque `nodepool`
👉 Pour chaque `node` dans ce nodepool
➡️ Vérifier que le node **n’est PAS actif** (donc pas déjà associé)

---

# ✅ 2. Étapes techniques

### Étape 1 — Récupérer les nodes depuis le payload

```python
nodepools = payload.get("nodepools", [])
```

---

### Étape 2 — Boucler sur les nodepools et nodes

```python
declined_reasons = []

for nodepool in nodepools:
    nodes = nodepool.get("nodes", [])

    for node_name in nodes:
        try:
```

---

### Étape 3 — Récupérer l’agent via le nodename

👉 Tu utilises déjà :

```python
agent = AgentInstanceHelper(
    db_session
).get_subscription_by_nodename_in_owner_bu_out(
    nodename=node_name,
    owner_bu=realm_details.code.bu,
    raise_if_not_exist=True,
    active=True  # important ici
)
```

---

### Étape 4 — Vérifier si le node est déjà actif (lié à un nodepool)

👉 Avec ta méthode existante :

```python
node = NodeInstanceHelper(
    db_session
).get_subscription_by_agent_subid(
    agent_subid=agent.subid,
    raise_if_not_exist=False,
    active=True
)
```

---

### Étape 5 — Ajouter la condition de refus

```python
if node is not None:
    declined_reasons.append(
        f"Node {node_name} is already active and belongs to another nodepool (agent_subid={agent.subid})"
    )
```

---

### Étape 6 — Gestion des erreurs

```python
except Exception as exc:
    declined_reasons.append(
        f"Error while processing node {node_name}: {str(exc)}"
    )
```

---

# ✅ 3. Code final (version clean)

```python
if payload.update_action == "add":
    declined_reasons = []

    realm_details = realm_service.get_realm_details()

    for nodepool in payload.get("nodepools", []):
        for node_name in nodepool.get("nodes", []):
            try:
                agent = AgentInstanceHelper(
                    db_session
                ).get_subscription_by_nodename_in_owner_bu_out(
                    nodename=node_name,
                    owner_bu=realm_details.code.bu,
                    raise_if_not_exist=True,
                    active=True
                )

                node = NodeInstanceHelper(
                    db_session
                ).get_subscription_by_agent_subid(
                    agent_subid=agent.subid,
                    raise_if_not_exist=False,
                    active=True
                )

                if node is not None:
                    declined_reasons.append(
                        f"Node {node_name} is already active and belongs to another nodepool (agent_subid={agent.subid})"
                    )

            except Exception as exc:
                declined_reasons.append(
                    f"Error while processing node {node_name}: {str(exc)}"
                )
```

---

# ✅ 4. Workflow Git (propre projet)

### 🌿 Nom de branche

```
feature/add-node-active-check
```

---

### 🧾 Commit message

```
feat: add validation to prevent adding active nodes to nodepools

- check if node is already associated via agent_subid
- reuse existing helpers for consistency
- append declined reasons when node is active
```

---

### 🧪 (optionnel) commit test/mock

```
test: add mock validation for node active check (no db access)
```

---

# ✅ 5. Message Teams à ton manager (simple & efficace)

Hi [Manager Name],

I’m currently working on the nodepool update “add” logic, specifically adding a validation to prevent adding nodes that are already active (already attached to another nodepool).

Since I don’t have DB access yet, could you please provide when you have time:

* 2 nodepools with a couple of nodes each
* and 1 node (agent) that is inactive and can be added

This would help me validate the implementation. Thanks!

---

Si tu veux, je peux aussi t’aider à faire un **mock sans DB** pour tester localement (genre fake Agent/Node helpers).



Voici une version **courte, claire et rassurante** pour ton manager 👇

---

Salut [Prénom],

Je travaille actuellement sur l’update “add” des nodepools, avec un check pour éviter d’ajouter des nodes déjà actifs.

Comme je n’ai pas encore accès à la DB, quand tu auras un peu de temps, pourrais-tu me fournir :

* 2 nodepools avec quelques nodes
* et 1 node (agent) non actif que je peux utiliser pour test

De mon côté, je vais limiter le workload du DAG à cette étape de vérification uniquement, sans aucune écriture en base 👍

Merci !

---

# 🧪 Mock simple sans DB (pour tester localement)

L’idée 👉 simuler :

* les **agents**
* les **nodes actifs**
* le comportement de tes helpers

---

## ✅ 1. Fake data

```python
FAKE_AGENTS = {
    "s02vx9961320": {"subid": "A1", "active": True},
    "s02vx9961580": {"subid": "A2", "active": True},
    "s02vx9961581": {"subid": "A3", "active": True},
    "free-node-01": {"subid": "A4", "active": True},
}

# Nodes déjà associés à un nodepool (donc actifs côté Node)
FAKE_NODES = {
    "A1": {"active": True},
    "A2": {"active": True},
    # A3 volontairement libre
}
```

---

## ✅ 2. Mock des helpers

```python
class MockAgentInstanceHelper:
    def __init__(self, db_session=None):
        pass

    def get_subscription_by_nodename_in_owner_bu_out(
        self, nodename, owner_bu, raise_if_not_exist=True, active=True
    ):
        agent = FAKE_AGENTS.get(nodename)

        if agent is None and raise_if_not_exist:
            raise Exception(f"No agent found for nodename {nodename}")

        if active and not agent["active"]:
            raise Exception(f"Agent {nodename} is not active")

        return type("Agent", (), agent)  # objet simple


class MockNodeInstanceHelper:
    def __init__(self, db_session=None):
        pass

    def get_subscription_by_agent_subid(
        self, agent_subid, raise_if_not_exist=False, active=True
    ):
        node = FAKE_NODES.get(agent_subid)

        if node is None:
            return None

        if active and not node["active"]:
            return None

        return type("Node", (), node)
```

---

## ✅ 3. Test de ton check

```python
def validate_nodes(payload):
    declined_reasons = []

    for nodepool in payload.get("nodepools", []):
        for node_name in nodepool.get("nodes", []):
            try:
                agent = MockAgentInstanceHelper().get_subscription_by_nodename_in_owner_bu_out(
                    nodename=node_name,
                    owner_bu="TEST_BU",
                    raise_if_not_exist=True,
                    active=True
                )

                node = MockNodeInstanceHelper().get_subscription_by_agent_subid(
                    agent_subid=agent.subid,
                    raise_if_not_exist=False,
                    active=True
                )

                if node is not None:
                    declined_reasons.append(
                        f"Node {node_name} already active (agent_subid={agent.subid})"
                    )

            except Exception as exc:
                declined_reasons.append(str(exc))

    return declined_reasons
```

---

## ✅ 4. Exemple payload test

```python
payload = {
    "update_action": "add",
    "nodepools": [
        {
            "nodepool_name": "np-031",
            "nodes": ["s02vx9961320", "free-node-01"]
        }
    ]
}
```

---

## ✅ 5. Résultat attendu

```python
[
  "Node s02vx9961320 already active (agent_subid=A1)"
]
```

👉 `free-node-01` passe ✅
👉 `s02vx9961320` est refusé ❌

---

Si tu veux, prochaine étape je peux te faire :

* un **test unitaire pytest propre**
* ou une **intégration directe dans ton DAG airflow** 👍
