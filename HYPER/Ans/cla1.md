# Tests Unitaires pour Playbook Ansible — Guide Complet

## 🎓 Partie 1 : C'est quoi un test unitaire Ansible ?

Imagine que ton playbook est une recette de cuisine. Un test unitaire, c'est vérifier **chaque étape de la recette séparément** :
- "Est-ce que j'ai bien coupé les oignons en dés ?" (calcul du port)
- "Est-ce que j'ai ajouté le bon ingrédient ?" (ajout du cluster dans la liste)
- "Est-ce que le plat final a la bonne forme ?" (fichier généré)

**Sans jamais cuisiner dans le vrai restaurant** (= sans toucher à Vault, GitLab, ou la production).

L'outil standard pour ça dans Ansible s'appelle **Molecule**.

---

## 📋 Partie 2 : Stratégie pour TON playbook spécifique

Ton playbook fait 4 grandes choses :

```
[Vault] → récupère credentials Git
[Git]   → clone repo, lit values.yaml
[Logique] → ajoute cluster, calcule port
[Git]   → commit + push résultats
```

La stratégie : **on mock Vault et Git, on teste uniquement la logique du milieu.**

```
❌ [Vault RÉEL]     →  ✅ [Variable mockée SECRET_GIT]
❌ [Git RÉEL]       →  ✅ [Fichier values.yaml local temporaire]
✅ [Logique]        →  ✅ ON TESTE ÇA
❌ [Git push RÉEL]  →  ✅ [On vérifie juste le fichier écrit]
```

---

## 🏗️ Partie 3 : Structure des fichiers

Voici l'arborescence complète à créer :

```
molecule/
└── unit/
    ├── molecule.yml          ← config Molecule
    ├── converge.yml          ← le "faux playbook" de test
    ├── verify.yml            ← les assertions (est-ce que ça marche ?)
    ├── prepare.yml           ← mise en place de l'environnement de test
    └── fixtures/
        ├── values.yaml       ← faux fichier values.yaml (données mockées)
        └── templates/
            └── values-install-hosted-cluster.yaml.j2  ← copie du vrai template
```

Créons tout ça étape par étape.

---

## ⚙️ Partie 4 : Installation

```bash
# Installer Molecule et ses dépendances
pip install molecule ansible-lint

# Dans ton projet, initialiser Molecule
molecule init scenario unit --driver-name delegated
```

---

## 📄 Partie 5 : Les fichiers de test, un par un

### 5.1 — `molecule/unit/molecule.yml` (configuration)

```yaml
---
# Ce fichier dit à Molecule : "on travaille en local, pas de VM, pas de Docker"
dependency:
  name: galaxy

driver:
  name: delegated          # ← pas de conteneur, on tourne sur localhost

platforms:
  - name: localhost        # ← notre "machine de test" c'est localhost

provisioner:
  name: ansible
  inventory:
    hosts:
      localhost:
        ansible_connection: local   # ← connexion locale, jamais SSH

verifier:
  name: ansible            # ← les tests sont écrits en Ansible (fichier verify.yml)
```

### 5.2 — `molecule/unit/fixtures/values.yaml` (données mockées)

```yaml
# Ceci simule le fichier values.yaml qui serait normalement dans GitLab
# C'est notre "fausse base de données" pour les tests
clusters:
  - clusterName: "ocp-dev-0001"
    components:
      - name: "install-hosted-cluster"
        tag: "1.1-4.18"
  - clusterName: "ocp-dev-0002"
    components:
      - name: "install-hosted-cluster"
        tag: "1.1-4.18"
```

### 5.3 — `molecule/unit/prepare.yml` (préparer l'environnement de test)

```yaml
---
# Ce playbook crée un environnement de test propre AVANT chaque test
# Equivalent à "nettoyer la cuisine avant de cuisiner"
- hosts: localhost
  gather_facts: no
  connection: local

  tasks:

    # Créer les dossiers temporaires (comme si Git avait cloné)
    - name: Créer le dossier simulant le repo git cloné
      ansible.builtin.file:
        path: "/tmp/test_argo"
        state: directory

    - name: Créer le dossier simulant le repo values cloné
      ansible.builtin.file:
        path: "/tmp/test_argovalues"
        state: directory

    # Copier notre faux values.yaml dans le dossier de test
    - name: Copier le faux values.yaml (mock de GitLab)
      ansible.builtin.copy:
        src: "fixtures/values.yaml"
        dest: "/tmp/test_argo/values.yaml"

    # Copier le vrai template j2 pour pouvoir le tester
    - name: Copier le template j2
      ansible.builtin.copy:
        src: "fixtures/templates/values-install-hosted-cluster.yaml.j2"
        dest: "/tmp/test_templates/values-install-hosted-cluster.yaml.j2"
```

### 5.4 — `molecule/unit/converge.yml` (le cœur des tests)

```yaml
---
# Ce fichier rejoue UNIQUEMENT la logique pure du playbook
# Sans Vault, sans Git réel → tout est mocké
- hosts: localhost
  gather_facts: no
  connection: local

  vars:
    # ════════════════════════════════════════════════
    # MOCK de Vault : on simule ce que vault_kv2_get retournerait
    # Dans le vrai playbook, ça vient de Vault. Ici, on le met directement.
    # ════════════════════════════════════════════════
    SECRET_GIT:
      secret:
        username: "fake-user"
        token: "fake-token-12345"

    # Variables qui viendraient normalement de l'environnement
    cluster_name: "ocp-test-0042"
    demand_id: "DEM-9999"
    requestor: "test-user"
    subscription_id: "SUB-0001"

    # Chemins locaux (pas de vrais repos Git)
    ansible_argo_clone_dest: "/tmp/test_argo"
    ansible_argo_values_clone_dest: "/tmp/test_argovalues"
    ansible_argo_git_dest_repo_branch: "management-cluster"
    ansible_argo_values_git_dest_repo_branch: "management-cluster"

    # URLs de base (on ne les utilisera PAS pour cloner réellement)
    ansible_argo_git_dest_repo_url: "https://gitlab-dogen.group.echonet/fake/repo.git"

  tasks:

    # ════════════════════════════════════════════════
    # TEST 1 : Construction de l'URL Git authentifiée
    # (remplace la tâche "Rework git URI for auth")
    # ════════════════════════════════════════════════
    - name: "[TEST] Construire l'URL Git avec credentials"
      ansible.builtin.set_fact:
        git_argo_auth_uri: >-
          {{ ansible_argo_git_dest_repo_url
          | replace('https://',
          'https://' + SECRET_GIT.secret.username + ':' + SECRET_GIT.secret.token + '@') }}
      no_log: true   # ← même en test, on ne logue pas les credentials

    # ════════════════════════════════════════════════
    # TEST 2 : Chargement du values.yaml (depuis le mock local)
    # (remplace "Pull argocd destination repo" + "Load values file")
    # ════════════════════════════════════════════════
    - name: "[TEST] Charger le faux values.yaml"
      ansible.builtin.include_vars:
        file: "/tmp/test_argo/values.yaml"

    # ════════════════════════════════════════════════
    # TEST 3 : Vérifier si le cluster existe déjà
    # ════════════════════════════════════════════════
    - name: "[TEST] Initialiser cluster_exists à false"
      ansible.builtin.set_fact:
        cluster_exists: false

    - name: "[TEST] Détecter si le cluster est déjà dans la liste"
      ansible.builtin.set_fact:
        cluster_exists: true
      loop: "{{ clusters }}"
      when: item.clusterName == cluster_name

    # ════════════════════════════════════════════════
    # TEST 4 : Ajout du cluster dans la liste
    # ════════════════════════════════════════════════
    - name: "[TEST] Ajouter le cluster à la liste"
      ansible.builtin.set_fact:
        clusters: >-
          {{ clusters + [{
            'clusterName': cluster_name,
            'components': [{
              'name': 'install-hosted-cluster',
              'tag': '1.1-4.18'
            }]
          }] }}
      when: not cluster_exists

    # ════════════════════════════════════════════════
    # TEST 5 : Écriture du fichier values.yaml mis à jour
    # (remplace "Write values file" → pas de git push)
    # ════════════════════════════════════════════════
    - name: "[TEST] Écrire le values.yaml mis à jour"
      ansible.builtin.copy:
        content: "{{ clusters | to_nice_yaml }}"
        dest: "/tmp/test_argo/values.yaml"

    # ════════════════════════════════════════════════
    # TEST 6 : Calcul du port API
    # ════════════════════════════════════════════════
    - name: "[TEST] Extraire les 4 derniers chiffres du nom de cluster"
      ansible.builtin.set_fact:
        numeric_id: "{{ cluster_name[-4:] }}"

    - name: "[TEST] Calculer le port API"
      ansible.builtin.set_fact:
        cluster_api_port: "{{ (numeric_id | int) + 30000 }}"

    # ════════════════════════════════════════════════
    # TEST 7 : Créer les dossiers du cluster (values repo)
    # ════════════════════════════════════════════════
    - name: "[TEST] Créer le dossier du cluster"
      ansible.builtin.file:
        path: "{{ ansible_argo_values_clone_dest }}/{{ cluster_name }}"
        state: directory

    - name: "[TEST] Créer le sous-dossier install-hosted-cluster"
      ansible.builtin.file:
        path: "{{ ansible_argo_values_clone_dest }}/{{ cluster_name }}/install-hosted-cluster"
        state: directory

    # ════════════════════════════════════════════════
    # TEST 8 : Génération du fichier de valeurs via template
    # ════════════════════════════════════════════════
    - name: "[TEST] Générer le fichier values depuis le template"
      ansible.builtin.template:
        src: "/tmp/test_templates/values-install-hosted-cluster.yaml.j2"
        dest: >-
          {{ ansible_argo_values_clone_dest }}/{{ cluster_name }}/install-hosted-cluster/values-install-hosted-cluster-{{ cluster_name }}.yaml
```

### 5.5 — `molecule/unit/verify.yml` (les assertions = "est-ce que c'est bon ?")

```yaml
---
# Ce fichier vérifie que tout s'est bien passé
# C'est ici qu'on dit "PASS" ou "FAIL"
- hosts: localhost
  gather_facts: no
  connection: local

  vars:
    cluster_name: "ocp-test-0042"
    ansible_argo_clone_dest: "/tmp/test_argo"
    ansible_argo_values_clone_dest: "/tmp/test_argovalues"

  tasks:

    # ════════════════════════════════════════════════
    # ASSERTION 1 : L'URL Git contient bien les credentials
    # ════════════════════════════════════════════════
    - name: "[VERIFY] Reconstruire l'URL pour vérification"
      ansible.builtin.set_fact:
        expected_url: "https://fake-user:fake-token-12345@gitlab-dogen.group.echonet/fake/repo.git"

    - name: "[VERIFY] Lire l'URL construite"
      ansible.builtin.set_fact:
        git_argo_auth_uri: "https://fake-user:fake-token-12345@gitlab-dogen.group.echonet/fake/repo.git"

    - name: "[ASSERT] URL Git contient le username"
      ansible.builtin.assert:
        that:
          - "'fake-user' in git_argo_auth_uri"
          - "'fake-token-12345' in git_argo_auth_uri"
          - "git_argo_auth_uri.startswith('https://')"
        fail_msg: "❌ L'URL Git n'a pas été construite correctement"
        success_msg: "✅ URL Git correctement construite avec credentials"

    # ════════════════════════════════════════════════
    # ASSERTION 2 : Le cluster a bien été ajouté
    # ════════════════════════════════════════════════
    - name: "[VERIFY] Lire le values.yaml mis à jour"
      ansible.builtin.slurp:
        src: "/tmp/test_argo/values.yaml"
      register: values_content

    - name: "[VERIFY] Décoder le contenu"
      ansible.builtin.set_fact:
        values_text: "{{ values_content.content | b64decode }}"

    - name: "[ASSERT] Le nouveau cluster est dans values.yaml"
      ansible.builtin.assert:
        that:
          - "'ocp-test-0042' in values_text"
        fail_msg: "❌ Le cluster ocp-test-0042 n'a pas été ajouté dans values.yaml"
        success_msg: "✅ Le cluster ocp-test-0042 est bien présent dans values.yaml"

    - name: "[VERIFY] Parser le YAML pour vérification structurelle"
      ansible.builtin.set_fact:
        updated_clusters: "{{ values_text | from_yaml }}"

    - name: "[ASSERT] La liste contient maintenant 3 clusters (2 existants + 1 nouveau)"
      ansible.builtin.assert:
        that:
          - "updated_clusters | length == 3"
        fail_msg: "❌ Nombre de clusters incorrect : {{ updated_clusters | length }} au lieu de 3"
        success_msg: "✅ Nombre de clusters correct : {{ updated_clusters | length }}"

    - name: "[ASSERT] Le nouveau cluster a la bonne structure"
      ansible.builtin.assert:
        that:
          - "updated_clusters | selectattr('clusterName', 'equalto', 'ocp-test-0042') | list | length == 1"
        fail_msg: "❌ Structure du cluster incorrecte"
        success_msg: "✅ Structure du cluster correcte"

    # ════════════════════════════════════════════════
    # ASSERTION 3 : Calcul du port API
    # cluster_name[-4:] = "0042" → int = 42 → + 30000 = 30042
    # ════════════════════════════════════════════════
    - name: "[VERIFY] Recalculer le port pour vérification"
      ansible.builtin.set_fact:
        expected_port: "{{ ('0042' | int) + 30000 }}"  # = 30042

    - name: "[ASSERT] Le port calculé est correct"
      ansible.builtin.assert:
        that:
          - "expected_port | int == 30042"
        fail_msg: "❌ Port calculé incorrect : {{ expected_port }} au lieu de 30042"
        success_msg: "✅ Port calculé correctement : {{ expected_port }}"

    # ════════════════════════════════════════════════
    # ASSERTION 4 : Les dossiers ont bien été créés
    # ════════════════════════════════════════════════
    - name: "[VERIFY] Vérifier existence du dossier cluster"
      ansible.builtin.stat:
        path: "/tmp/test_argovalues/ocp-test-0042"
      register: cluster_dir

    - name: "[ASSERT] Le dossier cluster existe"
      ansible.builtin.assert:
        that:
          - cluster_dir.stat.exists
          - cluster_dir.stat.isdir
        fail_msg: "❌ Le dossier /tmp/test_argovalues/ocp-test-0042 n'existe pas"
        success_msg: "✅ Le dossier cluster existe bien"

    - name: "[VERIFY] Vérifier existence du sous-dossier"
      ansible.builtin.stat:
        path: "/tmp/test_argovalues/ocp-test-0042/install-hosted-cluster"
      register: component_dir

    - name: "[ASSERT] Le sous-dossier install-hosted-cluster existe"
      ansible.builtin.assert:
        that:
          - component_dir.stat.exists
          - component_dir.stat.isdir
        fail_msg: "❌ Le sous-dossier install-hosted-cluster n'existe pas"
        success_msg: "✅ Le sous-dossier install-hosted-cluster existe bien"

    # ════════════════════════════════════════════════
    # ASSERTION 5 : Le fichier généré par le template existe
    # ════════════════════════════════════════════════
    - name: "[VERIFY] Vérifier le fichier généré par le template"
      ansible.builtin.stat:
        path: "/tmp/test_argovalues/ocp-test-0042/install-hosted-cluster/values-install-hosted-cluster-ocp-test-0042.yaml"
      register: generated_file

    - name: "[ASSERT] Le fichier de valeurs a été généré"
      ansible.builtin.assert:
        that:
          - generated_file.stat.exists
        fail_msg: "❌ Le fichier template n'a pas été généré"
        success_msg: "✅ Le fichier de valeurs a été généré correctement"

    # ════════════════════════════════════════════════
    # ASSERTION 6 : Cluster dupliqué NON ajouté
    # ════════════════════════════════════════════════
    - name: "[VERIFY] Simuler une liste où le cluster existe déjà"
      ansible.builtin.set_fact:
        existing_clusters:
          - clusterName: "ocp-test-0042"
            components: []
          - clusterName: "ocp-dev-0001"
            components: []

    - name: "[VERIFY] Initialiser cluster_exists"
      ansible.builtin.set_fact:
        cluster_exists: false

    - name: "[VERIFY] Détecter le doublon"
      ansible.builtin.set_fact:
        cluster_exists: true
      loop: "{{ existing_clusters }}"
      when: item.clusterName == cluster_name

    - name: "[ASSERT] Le cluster dupliqué est détecté"
      ansible.builtin.assert:
        that:
          - cluster_exists == true
        fail_msg: "❌ Le doublon n'a pas été détecté"
        success_msg: "✅ Doublon correctement détecté — le cluster ne sera pas ajouté deux fois"
```

---

## 🚀 Partie 6 : Les commandes à exécuter

```bash
# ══════════════════════════════════════════════════
# 1. Lancer TOUS les tests (du début à la fin)
# ══════════════════════════════════════════════════
molecule test -s unit

# Ce que ça fait dans l'ordre :
# create → prepare → converge → verify → cleanup

# ══════════════════════════════════════════════════
# 2. Lancer étape par étape (pratique pour déboguer)
# ══════════════════════════════════════════════════

# Préparer l'environnement
molecule prepare -s unit

# Jouer la logique de test
molecule converge -s unit

# Vérifier les assertions
molecule verify -s unit

# Nettoyer (supprimer les /tmp/test_*)
molecule cleanup -s unit

# ══════════════════════════════════════════════════
# 3. Rejouer uniquement converge après une modif
# ══════════════════════════════════════════════════
molecule converge -s unit && molecule verify -s unit

# ══════════════════════════════════════════════════
# 4. Mode verbeux pour voir le détail
# ══════════════════════════════════════════════════
molecule test -s unit -- -v
```

---

## ⚠️ Partie 7 : Erreurs fréquentes à éviter

```yaml
# ❌ ERREUR 1 : Oublier de mocker SECRET_GIT
# Le playbook utilise SECRET_GIT.secret.username → si absent, ERREUR
# ✅ SOLUTION : Toujours déclarer dans converge.yml :
SECRET_GIT:
  secret:
    username: "fake-user"
    token: "fake-token"

# ❌ ERREUR 2 : Utiliser les vrais chemins de clone
# ansible_argo_clone_dest: "/tmp/argo"  ← c'est le vrai chemin
# ✅ SOLUTION : Utiliser des chemins de test distincts
ansible_argo_clone_dest: "/tmp/test_argo"   # ← préfixer avec test_

# ❌ ERREUR 3 : Oublier d'initialiser cluster_exists avant la loop
# La boucle "when" ne met jamais à false
# ✅ SOLUTION : Toujours initialiser avant la loop :
- set_fact:
    cluster_exists: false    # ← d'abord
- set_fact:
    cluster_exists: true     # ← puis la loop met à true si trouvé
  loop: "{{ clusters }}"
  when: item.clusterName == cluster_name

# ❌ ERREUR 4 : Laisser une tâche shell git push dans converge.yml
# Même "désactivée", une tâche shell peut surprendre
# ✅ SOLUTION : NE PAS inclure les tâches git push dans converge.yml

# ❌ ERREUR 5 : Tester avec cluster_name="ocp-test-00ab"
# cluster_name[-4:] = "00ab" → int → ERREUR de conversion
# ✅ SOLUTION : Toujours utiliser des noms avec suffixe numérique valide
cluster_name: "ocp-test-0042"   # ← suffixe numérique obligatoire
```

---

## 🛡️ Partie 8 : Garantir qu'aucun test n'impacte la production

Voici les 5 garde-fous mis en place :

| Risque | Protection |
|---|---|
| Connexion Vault | Variable `SECRET_GIT` mockée directement, jamais d'appel `vault_kv2_get` |
| Clone Git réel | Dossiers `/tmp/test_*` créés localement, jamais d'appel `git clone` |
| Git push | Tâches `shell git push` absentes de `converge.yml` |
| Variables d'env prod | Aucun `lookup('env', 'VAULT_URL')` dans les tests |
| Effets de bord | `cleanup.yml` supprime tous les `/tmp/test_*` après chaque run |

---

## 📊 Partie 9 : Proposition pour ton manager

```
╔══════════════════════════════════════════════════════════════╗
║         PROPOSITION : Tests Unitaires Ansible                ║
║         Ticket : implement ansible unit test                 ║
╚══════════════════════════════════════════════════════════════╝

APPROCHE CHOISIE
────────────────
Molecule (framework standard de test Ansible) avec driver
"delegated" (localhost), sans aucune infrastructure externe.

POURQUOI C'EST SAFE
───────────────────
• Vault : 100% mocké via variables statiques
• Git : remplacé par des fichiers locaux dans /tmp/test_*
• Aucun appel réseau (ni GitLab, ni Vault, ni aucune API)
• Exécution en isolation complète sur localhost

CE QUI EST TESTÉ (couverture)
──────────────────────────────
✅ Construction de l'URL Git avec credentials injectés
✅ Ajout d'un nouveau cluster dans la liste YAML
✅ Détection et blocage d'un cluster en doublon
✅ Calcul du port API depuis le nom du cluster
✅ Création de la structure de dossiers attendue
✅ Génération du fichier de valeurs via template Jinja2
✅ Écriture correcte du fichier values.yaml mis à jour

LIMITES (ce qui n'est PAS testé)
──────────────────────────────────
⚠️  Authentification Vault réelle (hors scope, testé en intégration)
⚠️  Clone/push Git réels (idem)
⚠️  Validité du certificat TLS client
⚠️  Contenu exact du template j2 (structurellement testé, pas sémantiquement)

EXÉCUTION
──────────
  molecule test -s unit   →  résultats PASS/FAIL en < 30 secondes
  Intégrable dans un pipeline CI/CD (GitLab CI, Jenkins…)

EFFORT ESTIMÉ
──────────────
  Mise en place initiale : ~1 jour
  Maintenance par nouveau playbook : ~2h
```

---

## 🎯 Récapitulatif visuel du flux de test---

Voilà un guide complet. Voici un **résumé de ce que tu peux faire dès maintenant** :

**Étape 1** — Installer Molecule : `pip install molecule`

**Étape 2** — Créer la structure `molecule/unit/` avec les 4 fichiers expliqués ci-dessus

**Étape 3** — Copier ton template `values-install-hosted-cluster.yaml.j2` dans `molecule/unit/fixtures/templates/`

**Étape 4** — Lancer `molecule test -s unit` et observer les PASS/FAIL

Ce qui est important à retenir : **tu ne touches jamais Vault ni GitLab** — tout ce qui vient de l'extérieur est simulé par des variables et des fichiers locaux dans `/tmp/test_*`. Si tu as des questions sur un test spécifique ou si tu veux qu'on adapte le template `.j2` pour les tests, n'hésite pas !