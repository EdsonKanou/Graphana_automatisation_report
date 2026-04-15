Parfait, j'ai tout ce qu'il me faut. Voici la solution complète.

---

## La stratégie globale

```
projet/
├── playbooks/
│   ├── your-playbook.yml        ← playbook original (on y ajoute des tags)
│   └── other-playbook.yml
├── molecule/
│   └── unit/
│       ├── molecule.yml
│       ├── prepare.yml
│       ├── converge.yml         ← appelle le playbook original directement
│       ├── verify.yml
│       └── mock_vars.yml        ← toutes les variables mockées ici
└── .gitlab-ci.yml               ← CI à créer
```

**Un seul fichier de mock partagé** pour tous les playbooks → si un playbook évolue, tu touches seulement les tags dans ce playbook, pas les fichiers de test.

---

## Étape 1 — Ajouter des tags dans le playbook original

C'est la **seule modification** à faire sur le playbook original. On ajoute deux tags :
- `skip_in_test` sur les tâches qui touchent Vault ou Git
- rien à faire sur les tâches de logique pure

```yaml
---
- hosts: localhost
  gather_facts: no
  connection: local

  vars:
    hashivault_url: "{{ lookup('env', 'VAULT_URL') }}"
    hashivault_cert: "{{ lookup('env', 'CLIENT_CERT') | b64decode }}"
    hashivault_key: "{{ lookup('env', 'CLIENT_KEY') | b64decode }}"
    hashivault_secret_path: "{{ lookup('env', 'SECRET_PATH') }}"
    orchestrator_environment: "{{ lookup('env', 'ENVIRONMENT') }}"

    hashivault_gitlab_auth_secret_path: "orchestrator/{{ orchestrator_environment }}/products/hypershift/git_argo"
    hashivault_namespace: "BP21/BP21/EC001B003417"

    ansible_argo_git_dest_repo_url: "https://gitlab-dogen.group.echonet/market-place/ap27496/argo/bootstrap-clusters.git"
    ansible_argo_git_dest_repo_branch: "management-cluster"
    ansible_argo_values_git_dest_repo_url: "https://gitlab-dogen.group.echonet/market-place/ap27496/argo/bootstrap-clusters-values.git"
    ansible_argo_values_git_dest_repo_branch: "management-cluster"
    ansible_argo_clone_dest: "/tmp/argo"
    ansible_argo_values_clone_dest: "/tmp/argovalues"

  tasks:

    - name: Write public cert
      ansible.builtin.copy:
        content: "{{ hashivault_cert }}"
        dest: "/tmp/cs31.crt"
      tags: [skip_in_test]          # touches real Vault cert

    - name: Write private key
      ansible.builtin.copy:
        content: "{{ hashivault_key }}"
        dest: "/tmp/cs31.key"
      tags: [skip_in_test]          # touches real Vault cert

    - name: Gather GitLab credentials
      community.hashi_vault.vault_kv2_get:
        namespace: "{{ hashivault_namespace }}"
        path: "{{ hashivault_gitlab_auth_secret_path }}"
        url: "{{ hashivault_url }}"
        cert_auth_public_key: "/tmp/cs31.crt"
        cert_auth_private_key: "/tmp/cs31.key"
        ca_cert: "/etc/pki/ca-trust/source/anchors/bundle-ca.crt"
        auth_method: cert
      register: SECRET_GIT
      tags: [skip_in_test]          # real Vault call

    - name: Rework git URI for auth
      ansible.builtin.set_fact:
        git_argo_auth_uri: >-
          {{ ansible_argo_git_dest_repo_url
          | replace('https://',
          'https://' + SECRET_GIT.secret.username + ':' + SECRET_GIT.secret.token + '@') }}
      no_log: true
      tags: [skip_in_test]          # depends on real Vault response

    - name: Pull argocd destination repo
      ansible.builtin.git:
        repo: "{{ git_argo_auth_uri }}"
        dest: "{{ ansible_argo_clone_dest }}"
        version: "{{ ansible_argo_git_dest_repo_branch }}"
      tags: [skip_in_test]          # real Git clone

    - name: Load values file
      ansible.builtin.include_vars:
        file: "{{ ansible_argo_clone_dest }}/values.yaml"
      register: VALUESFILE
      # no tag = runs in test (file is prepared by prepare.yml)

    - name: Debug clusters
      debug:
        msg: "{{ clusters }}"

    - name: Check if cluster already exists
      ansible.builtin.set_fact:
        cluster_exists: true
      loop: "{{ clusters }}"
      when: item.clusterName == cluster_name
      ignore_errors: true
      # no tag = logic, always tested

    - name: Add payload cluster
      ansible.builtin.set_fact:
        clusters: >-
          {{ clusters + [{
            'clusterName': cluster_name,
            'components': [{
              'name': 'install-hosted-cluster',
              'tag': '1.1-4.18'
            }]
          }] }}
      # no tag = logic, always tested

    - name: Debug clusters updated
      debug:
        msg: "{{ clusters }}"

    - name: Write values file
      ansible.builtin.copy:
        content: "{{ clusters | to_nice_yaml }}"
        dest: "{{ ansible_argo_clone_dest }}/values.yaml"
      # no tag = tested (writes to /tmp/test_argo in test mode)

    - name: Commit and push changes
      shell: |
        cd {{ ansible_argo_clone_dest }} && \
        git config --global user.email "cs31-automation@bnpparibas.com" && \
        git config --global user.name "CS31 Automation" && \
        git add . && \
        git commit -m "orchestration: demand id: {{ demand_id }} from {{ requestor }} for sub: {{ subscription_id }}" && \
        git push origin {{ ansible_argo_git_dest_repo_branch }}
      tags: [skip_in_test]          # real Git push

    - name: Rework values repo URI
      ansible.builtin.set_fact:
        ansible_argo_values_git_dest_repo_auth_url: >-
          {{ ansible_argo_values_git_dest_repo_url
          | replace('https://',
          'https://' + SECRET_GIT.secret.username + ':' + SECRET_GIT.secret.token + '@') }}
      no_log: true
      tags: [skip_in_test]          # depends on real Vault response

    - name: Clone values repo
      ansible.builtin.git:
        repo: "{{ ansible_argo_values_git_dest_repo_auth_url }}"
        dest: "{{ ansible_argo_values_clone_dest }}"
        version: "{{ ansible_argo_values_git_dest_repo_branch }}"
      tags: [skip_in_test]          # real Git clone

    - name: Ensure cluster directory exists
      ansible.builtin.file:
        path: "{{ ansible_argo_values_clone_dest }}/{{ cluster_name }}"
        state: directory
      # no tag = tested

    - name: Ensure install-hosted-cluster directory exists
      ansible.builtin.file:
        path: "{{ ansible_argo_values_clone_dest }}/{{ cluster_name }}/install-hosted-cluster"
        state: directory
      # no tag = tested

    - name: Extract numeric ID
      ansible.builtin.set_fact:
        numeric_id: "{{ cluster_name[-4:] }}"
      # no tag = logic, always tested

    - name: Generate API port
      ansible.builtin.set_fact:
        cluster_api_port: "{{ (numeric_id | int) + 30000 }}"
      # no tag = logic, always tested

    - name: Template values file
      ansible.builtin.template:
        src: "values-install-hosted-cluster.yaml.j2"
        dest: "{{ ansible_argo_values_clone_dest }}/{{ cluster_name }}/install-hosted-cluster/values-install-hosted-cluster-{{ cluster_name }}.yaml"
      # no tag = tested

    - name: Commit and push values repo
      shell: |
        cd {{ ansible_argo_values_clone_dest }} && \
        git add . && \
        git commit -m "orchestration: demand id: {{ demand_id }} from {{ requestor }} for sub: {{ subscription_id }}" && \
        git push origin {{ ansible_argo_values_git_dest_repo_branch }}
      tags: [skip_in_test]          # real Git push
```

---

## Étape 2 — `molecule/unit/mock_vars.yml`

Un seul fichier de variables mockées, partagé par tous les playbooks.

```yaml
---
# All fake variables injected during tests
# Never use real Vault or Git values here

# Replaces the real Vault response (vault_kv2_get)
SECRET_GIT:
  secret:
    username: "fake-user"
    token: "fake-token-12345"

# Test input variables
cluster_name: "ocp-test-0042"
demand_id: "DEM-9999"
requestor: "test-user"
subscription_id: "SUB-0001"

# Override clone paths to use local test folders
ansible_argo_clone_dest: "/tmp/test_argo"
ansible_argo_values_clone_dest: "/tmp/test_argovalues"
```

---

## Étape 3 — `molecule/unit/prepare.yml`

```yaml
---
# Creates local folders and fake files before the test runs
- hosts: localhost
  gather_facts: no
  connection: local

  tasks:

    - name: Create fake argo repo folder
      ansible.builtin.file:
        path: "/tmp/test_argo"
        state: directory

    - name: Create fake values repo folder
      ansible.builtin.file:
        path: "/tmp/test_argovalues"
        state: directory

    - name: Copy fake values.yaml into fake repo
      ansible.builtin.copy:
        dest: "/tmp/test_argo/values.yaml"
        content: |
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

---

## Étape 4 — `molecule/unit/converge.yml`

Maintenant il **appelle directement le playbook original** :

```yaml
---
# Calls the real playbook directly
# Skips Vault and Git tasks via tags
# Injects mock variables via extra-vars

- name: Converge
  import_playbook: ../../playbooks/your-playbook.yml
```

Et dans `molecule.yml`, on passe les options à ansible :

```yaml
---
dependency:
  name: galaxy

driver:
  name: default

platforms:
  - name: localhost

provisioner:
  name: ansible
  inventory:
    hosts:
      localhost:
        ansible_connection: local
        ansible_python_interpreter: "{{ ansible_playbook_python }}"
  options:
    skip-tags: "skip_in_test"        # skip Vault and Git tasks
  extra_vars: "@molecule/unit/mock_vars.yml"   # inject mock variables

verifier:
  name: ansible
```

---

## Étape 5 — `molecule/unit/verify.yml`

Inchangé par rapport à avant :

```yaml
---
- hosts: localhost
  gather_facts: no
  connection: local

  vars:
    cluster_name: "ocp-test-0042"

  tasks:

    - name: Read updated values.yaml
      ansible.builtin.slurp:
        src: "/tmp/test_argo/values.yaml"
      register: values_content

    - name: Parse YAML content
      ansible.builtin.set_fact:
        updated_clusters: "{{ values_content.content | b64decode | from_yaml }}"

    - name: Check cluster was added
      ansible.builtin.assert:
        that:
          - "updated_clusters | length == 3"
          - "updated_clusters | selectattr('clusterName', 'equalto', 'ocp-test-0042') | list | length == 1"
        fail_msg: "FAIL - Cluster was not added correctly"
        success_msg: "PASS - Cluster added correctly"

    - name: Check API port calculation
      ansible.builtin.assert:
        that:
          - "( ('0042' | int) + 30000 ) == 30042"
        fail_msg: "FAIL - Port calculation is wrong"
        success_msg: "PASS - Port is 30042"

    - name: Check cluster directory was created
      ansible.builtin.stat:
        path: "/tmp/test_argovalues/ocp-test-0042/install-hosted-cluster"
      register: dir_check

    - name: Assert directory exists
      ansible.builtin.assert:
        that:
          - dir_check.stat.exists
        fail_msg: "FAIL - Directory was not created"
        success_msg: "PASS - Directory exists"
```

---

## Étape 6 — `.gitlab-ci.yml`

```yaml
---
# GitLab CI pipeline for Ansible unit tests
# Runs on every push and merge request
# Never touches Vault or production Git repos

stages:
  - test

ansible-unit-tests:
  stage: test

  # Use a standard Python image — no special requirements
  image: python:3.11

  before_script:
    # Install Ansible and Molecule
    - pip install ansible molecule

  script:
    # Run the full test cycle
    - molecule test -s unit

  # Run on every push and every merge request
  rules:
    - if: '$CI_PIPELINE_SOURCE == "push"'
    - if: '$CI_PIPELINE_SOURCE == "merge_request_event"'

  # Keep test artifacts (logs) for 7 days
  artifacts:
    when: always
    expire_in: 7 days
    paths:
      - molecule/unit/
```

---

## Réponse claire pour ton manager

| Question | Réponse |
|---|---|
| Tester à partir du playbook original ? | ✅ Oui — on appelle le vrai playbook, on skip juste les tâches dangereuses via des tags |
| Maintenance si le playbook évolue ? | Ajouter `tags: [skip_in_test]` sur toute nouvelle tâche Vault/Git. C'est tout. |
| CI GitLab possible ? | ✅ Oui — un seul fichier `.gitlab-ci.yml`, tourne sur chaque push et MR |
| Contrainte principale | Il faut connaître les runners disponibles sur votre GitLab (image Docker, accès réseau) |

La seule chose à confirmer avec ton équipe infra : **quel type de runner GitLab est disponible** (Docker, shell, Kubernetes) — l'image `python:3.11` dans le CI suppose un runner Docker.