# Mise en place d'un environnement de développement local avec Docker, PostgreSQL et Airflow

Je vais t'expliquer tout ça étape par étape, comme si tu démarrais de zéro. On va construire quelque chose de propre et professionnel.

---

## Vue d'ensemble de ce qu'on va faire

```
Ton projet existant
        │
        ▼
┌─────────────────────────────────────┐
│  Environnement local (Docker)       │
│                                     │
│  ┌─────────────┐  ┌──────────────┐  │
│  │ PostgreSQL  │  │   Airflow    │  │
│  │  (ta DB     │  │  (tes DAGs)  │  │
│  │   locale)   │  │              │  │
│  └─────────────┘  └──────────────┘  │
└─────────────────────────────────────┘
        │
        ▼
  Bascule facile prod ↔ local
  via variable d'environnement
```

---

## ÉTAPE 1 — Prérequis : installe Docker

### 1.1 Installer Docker Desktop
Va sur [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop) et installe la version pour ton OS.

Vérifie que ça marche :
```bash
docker --version
docker compose version
```
Tu dois voir quelque chose comme `Docker version 24.x.x`.

---

## ÉTAPE 2 — Structure de ton projet

Voici comment organiser les nouveaux fichiers dans ton projet existant. Tu ne touches à **rien d'existant** pour l'instant.

```
mon_projet/
│
├── 📁 local_dev/                    ← tout ce qu'on va créer
│   ├── docker-compose.yml           ← démarre PostgreSQL + Airflow
│   ├── .env.local                   ← tes variables d'environnement locales
│   ├── init_db/
│   │   └── 01_schema.sql            ← structure de ta DB (tables, etc.)
│   ├── pgdump/
│   │   └── sample_data.sql          ← données de test anonymisées
│   └── payloads/
│       └── payload_exemple.json     ← tes payloads de test
│
├── 📁 ton_code_existant/            ← tu ne touches pas à ça
│   ├── database/
│   ├── helpers/
│   └── dags/
│
├── pyproject.toml                   ← Poetry (on va l'adapter)
└── .env                             ← variables d'env (déjà existant ou à créer)
```

---

## ÉTAPE 3 — Créer le fichier `docker-compose.yml`

C'est le fichier central. Il dit à Docker : "lance ces services pour moi".

Crée le fichier `local_dev/docker-compose.yml` :

```yaml
# local_dev/docker-compose.yml

version: '3.8'

# Les "services" sont les conteneurs Docker qui vont tourner
services:

  # ── SERVICE 1 : PostgreSQL ──────────────────────────────────────
  postgres:
    image: postgres:15          # utilise l'image officielle PostgreSQL v15
    container_name: mon_projet_postgres
    restart: unless-stopped     # redémarre automatiquement si crash

    environment:                # variables d'environnement DANS le conteneur
      POSTGRES_USER: dev_user
      POSTGRES_PASSWORD: dev_password
      POSTGRES_DB: mon_projet_local

    ports:
      - "5433:5432"             # 5433 sur ta machine → 5432 dans Docker
                                # (on évite 5432 au cas où tu as un postgres local)

    volumes:
      # Persiste les données même si tu arrêtes Docker
      - postgres_data:/var/lib/postgresql/data
      # Scripts SQL exécutés automatiquement au 1er démarrage
      - ./init_db:/docker-entrypoint-initdb.d

    healthcheck:                # vérifie que postgres est prêt
      test: ["CMD-SHELL", "pg_isready -U dev_user -d mon_projet_local"]
      interval: 10s
      timeout: 5s
      retries: 5

  # ── SERVICE 2 : Airflow (optionnel dans un premier temps) ───────
  # On le configure plus bas dans l'étape Airflow

# Les "volumes" sont des espaces de stockage persistants
volumes:
  postgres_data:
```

### Pourquoi le port 5433 et pas 5432 ?
Si tu as déjà PostgreSQL installé sur ta machine (hors Docker), il utilise le port 5432. On utilise 5433 pour éviter le conflit. Ton code se connectera sur `localhost:5433`.

---

## ÉTAPE 4 — Créer le fichier `.env.local`

Ce fichier contient tes variables locales. **Ne le commite jamais sur Git** (ajoute-le au `.gitignore`).

```bash
# local_dev/.env.local

# ── Base de données locale ──────────────────────────────
DB_HOST=localhost
DB_PORT=5433
DB_NAME=mon_projet_local
DB_USER=dev_user
DB_PASSWORD=dev_password

# URL complète pour SQLAlchemy
DATABASE_URL=postgresql://dev_user:dev_password@localhost:5433/mon_projet_local

# ── Environnement ───────────────────────────────────────
ENV=local    # production | local
```

Et dans ton `.gitignore` à la racine du projet :
```
# Environnements locaux
local_dev/.env.local
.env.local
*.env.local
```

---

## ÉTAPE 5 — Adapter ton code Python pour basculer prod ↔ local

C'est la partie la plus importante. L'idée : ton code lit la `DATABASE_URL` depuis les variables d'environnement. Il ne sait pas (et ne doit pas savoir) si c'est la prod ou le local.

### 5.1 Créer un fichier de configuration central

Crée `config.py` (ou adapte le tien s'il existe) :

```python
# config.py
import os
from dotenv import load_dotenv

# Charge le bon fichier .env selon l'environnement
env = os.getenv("ENV", "local")

if env == "local":
    # Cherche local_dev/.env.local
    load_dotenv("local_dev/.env.local")
else:
    # En prod, les variables sont déjà injectées par ton système
    load_dotenv()

# ── URL de la base de données ───────────────────────────────────────
DATABASE_URL = os.environ["DATABASE_URL"]
# Le ["DATABASE_URL"] (avec crochets) plante si la variable n'existe pas
# C'est voulu : mieux vaut une erreur claire qu'une connexion silencieuse à la mauvaise DB

# ── Autres configs utiles ───────────────────────────────────────────
ENV = os.getenv("ENV", "local")
IS_LOCAL = ENV == "local"
```

### 5.2 Adapter ta session SQLAlchemy

Tu as dit que tu importes `from sqlalchemy.orm import Session as SASession`. Voici comment créer une session qui utilise automatiquement la bonne DB :

```python
# database/session.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session as SASession
from config import DATABASE_URL

# L'engine est la "connexion" à la DB
engine = create_engine(
    DATABASE_URL,
    # pool_pre_ping vérifie que la connexion est vivante avant de l'utiliser
    pool_pre_ping=True,
    # En local, on veut voir les requêtes SQL dans les logs
    echo=IS_LOCAL,  # True en local, False en prod
)

# SessionLocal est une "fabrique" de sessions
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)

def get_db() -> SASession:
    """
    Utilisation typique :
        with get_db() as db:
            db.query(...)
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
```

### 5.3 Dans tes fichiers existants

Tu n'as **pas besoin de modifier** tes helpers existants si tu changes uniquement la `DATABASE_URL`. La seule chose à changer, c'est comment l'engine/session est créé. Si tes fichiers font :

```python
# AVANT (dans tes fichiers existants)
from sqlalchemy.orm import Session as SASession
# ... et utilisent une URL hardcodée ou une variable globale
```

Tu changes juste la source de l'URL :
```python
# APRÈS
from database.session import SessionLocal, engine  # importe depuis ton nouveau fichier
```

---

## ÉTAPE 6 — Configurer Poetry

```bash
# Ajoute les dépendances nécessaires
poetry add python-dotenv psycopg2-binary sqlalchemy

# Si tu n'as pas encore alembic pour les migrations
poetry add alembic
```

Ton `pyproject.toml` aura ces lignes dans `[tool.poetry.dependencies]` :
```toml
[tool.poetry.dependencies]
python = "^3.10"
sqlalchemy = "^2.0"
psycopg2-binary = "^2.9"    # driver PostgreSQL pour Python
python-dotenv = "^1.0"
alembic = "^1.13"            # migrations
```

---

## ÉTAPE 7 — Créer la structure de la DB locale

### 7.1 Script SQL d'initialisation

Ce script sera exécuté automatiquement par Docker au **premier démarrage** de PostgreSQL.

Crée `local_dev/init_db/01_schema.sql` :

```sql
-- local_dev/init_db/01_schema.sql
-- Ce fichier recrée la structure de tes tables (sans les données sensibles)

-- Exemple : adapte avec TES vraies tables
CREATE TABLE IF NOT EXISTS utilisateurs (
    id          SERIAL PRIMARY KEY,
    nom         VARCHAR(100) NOT NULL,
    email       VARCHAR(150) UNIQUE NOT NULL,
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    id              SERIAL PRIMARY KEY,
    utilisateur_id  INTEGER REFERENCES utilisateurs(id),
    montant         DECIMAL(15, 2) NOT NULL,
    statut          VARCHAR(50) DEFAULT 'pending',
    created_at      TIMESTAMP DEFAULT NOW()
);

-- Ajoute ici toutes tes autres tables...
```

**Comment obtenir la structure de ta DB de prod ?** Demande à ton DBA (sans les données, juste le schéma) :
```bash
# Commande que le DBA peut lancer sur la prod (structure seule, sans données)
pg_dump --schema-only --no-owner nom_db_prod > schema_prod.sql
```

### 7.2 Données de test anonymisées

Crée `local_dev/pgdump/sample_data.sql` :

```sql
-- local_dev/pgdump/sample_data.sql
-- Données FICTIVES pour tester — jamais de vraies données clients !

INSERT INTO utilisateurs (nom, email) VALUES
    ('Test User 1', 'test1@exemple-fictif.local'),
    ('Test User 2', 'test2@exemple-fictif.local'),
    ('Test User 3', 'test3@exemple-fictif.local');

INSERT INTO transactions (utilisateur_id, montant, statut) VALUES
    (1, 100.00, 'completed'),
    (1, 250.50, 'pending'),
    (2, 75.00, 'completed');
```

⚠️ **Règle d'or en environnement bancaire** : ne mets **jamais** de vraies données clients dans ces fichiers, même anonymisées partiellement. Utilise uniquement des données entièrement fictives.

---

## ÉTAPE 8 — Lancer Docker pour la première fois

```bash
# Place-toi dans le dossier local_dev
cd local_dev

# Lance PostgreSQL en arrière-plan (-d = detached)
docker compose up -d postgres

# Vérifie que ça tourne
docker compose ps
```

Tu dois voir quelque chose comme :
```
NAME                     STATUS          PORTS
mon_projet_postgres      running         0.0.0.0:5433->5432/tcp
```

### Vérifier la connexion

```bash
# Connecte-toi à la DB depuis ton terminal
docker exec -it mon_projet_postgres psql -U dev_user -d mon_projet_local

# Dans psql, liste les tables
\dt

# Quitte
\q
```

### Charger les données de test

```bash
# Charge le fichier de données de test
docker exec -i mon_projet_postgres psql -U dev_user -d mon_projet_local \
  < pgdump/sample_data.sql
```

---

## ÉTAPE 9 — Migrations avec Alembic (gestion de la structure DB)

Alembic te permet de faire évoluer ta DB proprement, comme un "Git pour ta base de données".

```bash
# Initialise Alembic à la racine du projet
cd ..  # remonte à la racine
poetry run alembic init alembic
```

Cela crée :
```
alembic/
├── versions/       ← tes fichiers de migration
├── env.py          ← configuration d'Alembic
└── script.py.mako
alembic.ini         ← config principale
```

### Configurer `alembic/env.py`

```python
# alembic/env.py
# Trouve la ligne "config.set_main_option" et remplace par :

from config import DATABASE_URL  # ton fichier config.py
config.set_main_option("sqlalchemy.url", DATABASE_URL)
```

### Utiliser Alembic

```bash
# Crée une nouvelle migration (après avoir modifié un modèle)
poetry run alembic revision --autogenerate -m "ajout colonne statut"

# Applique les migrations
poetry run alembic upgrade head

# Revenir en arrière
poetry run alembic downgrade -1
```

---

## ÉTAPE 10 — Airflow en local avec Docker

Ajoute Airflow à ton `docker-compose.yml` :

```yaml
# Ajoute ces services dans local_dev/docker-compose.yml

  # ── Airflow Init (s'exécute une seule fois pour initialiser) ─────
  airflow-init:
    image: apache/airflow:2.8.0
    container_name: airflow_init
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://dev_user:dev_password@postgres:5432/mon_projet_local
      AIRFLOW__CORE__FERNET_KEY: ''
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    command: >
      bash -c "airflow db init &&
               airflow users create
                 --username admin
                 --password admin
                 --firstname Admin
                 --lastname User
                 --role Admin
                 --email admin@local.dev"
    volumes:
      - ../dags:/opt/airflow/dags    # ← pointe vers TES dags existants

  # ── Airflow Webserver ────────────────────────────────────────────
  airflow-webserver:
    image: apache/airflow:2.8.0
    container_name: airflow_webserver
    depends_on:
      - airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://dev_user:dev_password@postgres:5432/mon_projet_local
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      # Ta DB applicative (pour tes DAGs)
      DATABASE_URL: postgresql://dev_user:dev_password@postgres:5432/mon_projet_local
    ports:
      - "8080:8080"
    volumes:
      - ../dags:/opt/airflow/dags
      - airflow_logs:/opt/airflow/logs
    command: webserver

  # ── Airflow Scheduler ────────────────────────────────────────────
  airflow-scheduler:
    image: apache/airflow:2.8.0
    container_name: airflow_scheduler
    depends_on:
      - airflow-init
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://dev_user:dev_password@postgres:5432/mon_projet_local
      DATABASE_URL: postgresql://dev_user:dev_password@postgres:5432/mon_projet_local
    volumes:
      - ../dags:/opt/airflow/dags
      - airflow_logs:/opt/airflow/logs
    command: scheduler

# Ajoute ce volume dans la section volumes:
# volumes:
#   postgres_data:
#   airflow_logs:     ← ajoute cette ligne
```

### Lancer Airflow

```bash
cd local_dev

# Lance tout (postgres + airflow)
docker compose up -d

# Attends ~30 secondes, puis ouvre http://localhost:8080
# Login : admin / admin
```

### Tester un DAG avec un payload

Crée `local_dev/payloads/payload_exemple.json` :
```json
{
  "dag_id": "mon_dag",
  "conf": {
    "utilisateur_id": 1,
    "montant": 500.00,
    "date_traitement": "2024-01-15"
  }
}
```

Déclenche le DAG via l'API Airflow :
```bash
curl -X POST "http://localhost:8080/api/v1/dags/mon_dag/dagRuns" \
  -H "Content-Type: application/json" \
  -u admin:admin \
  -d @local_dev/payloads/payload_exemple.json
```

---

## ÉTAPE 11 — Commandes du quotidien

```bash
# ── Démarrer l'environnement ────────────────────────────
cd local_dev
docker compose up -d

# ── Arrêter (garde les données) ─────────────────────────
docker compose stop

# ── Arrêter ET supprimer les données ────────────────────
docker compose down -v   # -v supprime les volumes = repart de zéro

# ── Voir les logs ────────────────────────────────────────
docker compose logs postgres
docker compose logs airflow-webserver

# ── Recharger les données de test ───────────────────────
docker exec -i mon_projet_postgres psql -U dev_user -d mon_projet_local \
  < pgdump/sample_data.sql
```

---

## Erreurs courantes à éviter

| Erreur | Cause | Solution |
|--------|-------|----------|
| `Connection refused` sur port 5432 | Tu utilises 5433 localement | Vérifie que ton `.env.local` a bien `DB_PORT=5433` |
| `password authentication failed` | Mauvais user/password | Relis ton `docker-compose.yml` et `.env.local` |
| Les tables n'existent pas | Script SQL non exécuté | Docker exécute `init_db/` seulement au 1er démarrage. Fais `docker compose down -v` puis `up` |
| Airflow ne voit pas tes DAGs | Mauvais chemin de volume | Vérifie le chemin `../dags:/opt/airflow/dags` |
| `MODULE_NOT_FOUND` dans un DAG | Dépendances manquantes dans le container | Crée un `Dockerfile` Airflow custom avec tes packages |

---

## Récapitulatif de l'ordre à suivre

1. **Installe Docker** Desktop
2. **Crée la structure** `local_dev/` avec les fichiers ci-dessus
3. **Lance** `docker compose up -d postgres`
4. **Adapte** `config.py` et `database/session.py`
5. **Installe** les dépendances avec `poetry add ...`
6. **Teste** la connexion Python → PostgreSQL local
7. **Ajoute Airflow** quand PostgreSQL fonctionne bien
8. **Lance** tes DAGs avec des payloads de test

Si tu bloques à une étape précise, partage l'erreur et on la résoudra ensemble !






################# SUITE ##############################



Bonne nouvelle : **tu n'as pas besoin du script d'initialisation SQL** ! Tu as déjà tout ce qu'il faut avec tes CSV et ton `sql-command.sql`. On va juste adapter l'approche.

---

## Ce que tu as déjà et comment l'exploiter

```
pgdump/
├── sql-command.sql          ← commandes COPY (charge les CSV)
├── env_specific_data.csv
├── mgt_cluster.csv
├── ocp_version.csv
├── oidc.csv
├── svm.csv
└── cluster.csv
```

Le problème actuel : ton `sql-command.sql` utilise des chemins **internes à un serveur** (`/var/lib/postgresql/...`). On va adapter ça pour Docker.

---

## Le plan en 3 étapes

### ÉTAPE 1 — Comprendre pourquoi les CSV suffisent

Tes fichiers CSV **contiennent déjà** la structure implicite (les colonnes sont dans l'en-tête). La commande `COPY` de PostgreSQL peut créer les données **mais pas les tables**. Donc tu as deux options :

**Option A** — SQLAlchemy crée les tables automatiquement via tes modèles (le plus propre si tu as des modèles définis)

**Option B** — PostgreSQL crée les tables lui-même via un dump complet

Dis-moi : est-ce que dans ton projet tu as des fichiers avec des classes Python comme ça ?

```python
class MgtCluster(Base):
    __tablename__ = "mgt_cluster"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    ...
```

Si oui → **Option A**, SQLAlchemy s'en charge.
Si non → **Option B**, on fait autrement.

---

### ÉTAPE 2 — Adapter ton `sql-command.sql` pour Docker

Le seul changement à faire : les chemins des CSV. Dans Docker, on va monter tes CSV dans `/docker-entrypoint-initdb.d/csv/`, donc les chemins changent.

Crée un nouveau fichier `local_dev/init_db/02_load_data.sql` (copie de ton sql-command.sql avec les chemins corrigés) :

```sql
-- local_dev/init_db/02_load_data.sql

COPY env_specific_data("created_at", "created_by", "deleted_at", "deleted_by", 
    "updated_at", "updated_by", "id", "name", "quay_url_1", "quay_url_2", 
    "quay_url_3", "active")
FROM '/docker-entrypoint-initdb.d/csv/env_specific_data.csv'  -- ← chemin Docker
DELIMITER ','
CSV HEADER;

COPY mgt_cluster("created_at", "created_by", "deleted_at", "deleted_by", 
    "updated_at", "updated_by", "id", "name", "api_url", "provisionning", 
    "active", "discovery_url", "env_specific_data__id")
FROM '/docker-entrypoint-initdb.d/csv/mgt_cluster.csv'
DELIMITER ','
CSV HEADER;

COPY ocp_version("created_at", "created_by", "deleted_at", "deleted_by", 
    "updated_at", "updated_by", "id", "name", "version", "release_date", 
    "eoss_date", "eoes_date", "eol_date", "active", "url")
FROM '/docker-entrypoint-initdb.d/csv/ocp_version.csv'
DELIMITER ','
CSV HEADER;

COPY oidc("created_at", "created_by", "deleted_at", "deleted_by", 
    "updated_at", "updated_by", "id", "sub_id", "name", "client_id", 
    "client_secret", "issuer", "active")
FROM '/docker-entrypoint-initdb.d/csv/oidc.csv'
DELIMITER ','
CSV HEADER;

COPY svm("created_at", "created_by", "deleted_at", "deleted_by", 
    "updated_at", "updated_by", "id", "sub_id", "name", "svm_name", 
    "address_mgt", "address_data", "username", "password", 
    "prefix", "quota", "active")
FROM '/docker-entrypoint-initdb.d/csv/svm.csv'
DELIMITER ','
CSV HEADER;

COPY cluster("created_at", "created_by", "deleted_at", "deleted_by", 
    "updated_at", "updated_by", "sub_id", "name", "apcode", "realm", 
    "environment_type", "code_bu", "pod_cidr", "pod_prefix", 
    "svc_cidr", "mandatory_upgrade", "active", "oidc__id", 
    "mgt_cluster__id", "ocp_version__id", "svm__id")
FROM '/docker-entrypoint-initdb.d/csv/cluster.csv'
DELIMITER ','
CSV HEADER;
```

---

### ÉTAPE 3 — Adapter le `docker-compose.yml` pour monter tes CSV

```yaml
services:
  postgres:
    image: postgres:15
    container_name: mon_projet_postgres
    environment:
      POSTGRES_USER: dev_user
      POSTGRES_PASSWORD: dev_password
      POSTGRES_DB: mon_projet_local
    ports:
      - "5433:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      # Montage du dossier init (scripts SQL exécutés au 1er démarrage)
      - ./init_db:/docker-entrypoint-initdb.d
      # Montage de tes CSV dans un sous-dossier accessible
      - ../pgdump:/docker-entrypoint-initdb.d/csv   # ← tes CSV sont ici

volumes:
  postgres_data:
```

---

## Mais les tables doivent exister avant le COPY !

C'est le point clé. Le `COPY` charge des données, mais les tables doivent déjà exister. Voici la solution la plus simple selon ta situation :

### Si tu as les modèles SQLAlchemy → script Python

```python
# local_dev/create_tables.py
import sys
sys.path.insert(0, '..')   # pour accéder à ton projet

from database.session import engine
from tes_modeles import Base   # adapte l'import

print("Création des tables...")
Base.metadata.create_all(engine)
print("Tables créées !")
```

```bash
cd local_dev
poetry run python create_tables.py
# puis charge les CSV
docker exec -i mon_projet_postgres psql -U dev_user -d mon_projet_local \
  < init_db/02_load_data.sql
```

### Si tu n'as pas les modèles → demande un dump de structure à ton DBA

```bash
# Commande que le DBA lance côté prod (sans données, juste la structure)
pg_dump --schema-only --no-owner --no-acl nom_db_prod > local_dev/init_db/01_schema.sql
```

Ce fichier `01_schema.sql` sera exécuté automatiquement par Docker avant ton `02_load_data.sql` (ils sont exécutés dans l'ordre alphabétique/numérique).

---

## Pour savoir quelle option te convient

Partage-moi juste :
1. Est-ce que tu as des fichiers Python avec des classes `Base` / `Column` / `__tablename__` dans ton projet ?
2. As-tu accès à quelqu'un qui peut faire un `pg_dump --schema-only` sur la prod ?

Avec ça je te donne la procédure exacte adaptée à ta situation.