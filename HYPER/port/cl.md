Je vais t'apporter une réponse d'architecture complète et structurée.Voici une analyse complète de ton problème d'architecture.---

## 1. Design DB recommandé

**Table dédiée `ports` — c'est la bonne approche**, et non un simple champ sur `Cluster`. Voici pourquoi : un champ `cluster_api_port INT` sur `Cluster` ne te donne aucun moyen de savoir quels ports sont libres sans scanner tous les clusters. La table dédiée te donne un pool contrôlable, requêtable, verrouillable.

```python
# models/port.py
from sqlalchemy import Boolean, Column, Integer, ForeignKey
from sqlalchemy.dialects.postgresql import UUID
from hypershift.database.models.base import Base
from hypershift.database.models.mixins import EditableTimestampRangeMixin

class Port(Base, EditableTimestampRangeMixin):
    __tablename__ = "port"

    sub_id    = Column(UUID(as_uuid=True), primary_key=True)
    port_number = Column(Integer, nullable=False, unique=True)  # contrainte UNIQUE
    is_used   = Column(Boolean, nullable=False, default=False)
    cluster_id = Column(UUID(as_uuid=True), ForeignKey("cluster.sub_id"), nullable=True)
```

Sur `Cluster`, tu ajoutes juste une FK :

```python
# dans cluster.py
cluster_api_port_id = Column(UUID(as_uuid=True), ForeignKey("port.sub_id"), nullable=True)
rs_port = relationship("Port", backref="cluster", uselist=False)
```

---

## 2. Range de ports

Pour Hypershift, le range standard Kubernetes NodePort est **30000–32767**, ce qui donne 2768 ports disponibles — largement suffisant. Tu pré-peuples la table au déploiement via une migration Alembic :

```python
# alembic/versions/xxxx_seed_ports.py
import uuid
from alembic import op
import sqlalchemy as sa

PORT_RANGE_START = 30000
PORT_RANGE_END   = 32767

def upgrade():
    op.bulk_insert(
        sa.table("port",
            sa.column("sub_id"),
            sa.column("port_number"),
            sa.column("is_used"),
        ),
        [
            {"sub_id": str(uuid.uuid4()), "port_number": p, "is_used": False}
            for p in range(PORT_RANGE_START, PORT_RANGE_END + 1)
        ]
    )
```

Avantage : **visibilité immédiate** en prod sur le taux d'occupation via un simple `SELECT COUNT(*) WHERE is_used = false`.

---

## 3. Stratégie d'attribution — séquentielle avec `SKIP LOCKED`

**Séquentielle `ORDER BY port_number`** — c'est la meilleure option :

- Pas de fragmentation : les ports se remplissent du bas, les libérations créent des trous réutilisés en priorité
- Déterministe et auditable
- Compatible avec `SELECT FOR UPDATE SKIP LOCKED` (la clé anti race condition)

```python
# helpers/port_instance_helper.py
from sqlalchemy.orm import Session as SASession
from hypershift.database.models.port import Port

class PortInstanceHelper:
    def __init__(self, db_session: SASession) -> None:
        self.db_session = db_session

    def acquire_free_port(self) -> Port:
        """
        Sélectionne et verrouille atomiquement le premier port libre.
        SKIP LOCKED : si un autre worker a déjà verrouillé ce port,
        on passe au suivant — pas d'attente, pas de deadlock.
        """
        port = (
            self.db_session.query(Port)
            .filter(Port.is_used == False)
            .order_by(Port.port_number)
            .with_for_update(skip_locked=True)
            .first()
        )
        if port is None:
            raise Exception(
                "No free port available in the pool. "
                "Range 30000-32767 exhausted — contact infra team."
            )
        port.is_used = True
        return port

    def release_port(self, cluster_id: str) -> None:
        """Libère le port lors de la suppression du cluster."""
        port = (
            self.db_session.query(Port)
            .filter(Port.cluster_id == cluster_id)
            .first()
        )
        if port:
            port.is_used = False
            port.cluster_id = None
```

---

## 4. Concurrence — pourquoi `SKIP LOCKED` suffit

`SELECT FOR UPDATE` seul bloque toutes les transactions concurrentes sur la même row. `SKIP LOCKED` fait mieux : il **ignore les rows déjà verrouillées** et passe à la suivante libre. Résultat : deux workers simultanés obtiennent deux ports différents sans attente ni deadlock.

La contrainte `UNIQUE` sur `port_number` est ton **filet de sécurité** : même si la logique applicative avait un bug, la DB rejetterait une double attribution.

---

## 5. Intégration dans le flow existant

En suivant le pattern `OcpVersionInstanceHelper` que tu as partagé, l'intégration dans le flow de création devient naturelle :

```python
# hypershift/cluster/v1/create.py  (extrait)

port_instance: Port = PortInstanceHelper(db_session).acquire_free_port()

cluster = Cluster(
    name=cluster_name,
    # ... autres champs ...
    cluster_api_port_id=port_instance.sub_id,
)
db_session.add(cluster)

# Lie le port au cluster
port_instance.cluster_id = cluster.sub_id

db_session.commit()  # atomique : cluster + port en une transaction
```

---

## Récapitulatif des décisions

| Sujet | Choix | Raison |
|---|---|---|
| Stockage | Table `port` dédiée | Pool auditable, requêtable |
| Pré-génération | Oui, via migration Alembic | Visibilité immédiate du stock |
| Range | 30000–32767 | Standard Kubernetes NodePort |
| Ordre | Séquentiel `ORDER BY` | Pas de fragmentation inutile |
| Concurrence | `WITH FOR UPDATE SKIP LOCKED` | Pas de deadlock, pas de retry |
| Sécurité secondaire | `UNIQUE` sur `port_number` | Filet DB niveau bas |
| Pattern | `PortInstanceHelper` | Cohérent avec le reste du projet |