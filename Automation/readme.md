# 📊 Grafana Reporter - Générateur de Rapports Automatisés

Outil Python pour extraire les données de plusieurs dashboards Grafana et générer des rapports HTML professionnels envoyés par email.

## 🎯 Fonctionnalités

✅ Extraction de données de plusieurs dashboards et panels Grafana  
✅ Analyse sur 3 périodes configurables (14 jours, 2 mois, 3 mois)  
✅ Génération d'emails HTML responsive et stylés  
✅ Indicateurs visuels (couleurs selon seuils)  
✅ Calcul automatique des tendances  
✅ Logging détaillé dans des fichiers  
✅ Envoi via SMTP (Outlook/Office365)  
✅ Configuration flexible via fichiers `.env`

---

## 📁 Structure du Projet

```
grafana-reporter/
├── .env                     # Configuration sensible (à créer)
├── config.py                # Configuration des dashboards/panels
├── grafana_client.py        # Client API Grafana
├── email_generator.py       # Génération HTML
├── email_sender.py          # Envoi SMTP
├── logger_config.py         # Configuration logging
├── main.py                  # Point d'entrée
├── requirements.txt         # Dépendances Python
├── README.md               # Cette documentation
├── logs/                    # Logs générés automatiquement
└── reports/                 # Rapports HTML sauvegardés
```

---

## 🚀 Installation

### 1. Prérequis

- Python 3.12+ installé
- Accès à l'API Grafana avec clé API
- Compte SMTP (Outlook/Office365)

### 2. Cloner/Télécharger le projet

```bash
# Créer le dossier du projet
mkdir grafana-reporter
cd grafana-reporter
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 4. Configurer le fichier `.env`

Créer un fichier `.env` à la racine avec vos identifiants :

```env
# Grafana Configuration
GRAFANA_URL=https://orchestrator-dashboard.group.echonet
GRAFANA_API_KEY=glsa_VotreCléAPIGrafana

# SMTP Configuration (Outlook/Office365)
SMTP_SERVER=smtp.office365.com
SMTP_PORT=587
SMTP_USERNAME=votre.email@entreprise.com
SMTP_PASSWORD=votre_mot_de_passe
SMTP_USE_TLS=True

# Email Configuration
EMAIL_FROM=votre.email@entreprise.com
EMAIL_TO=destinataire1@entreprise.com,destinataire2@entreprise.com
EMAIL_CC=copie@entreprise.com
EMAIL_SUBJECT=Rapport Grafana - Métriques de Performance
```

**⚠️ Important :** Ne JAMAIS commiter le fichier `.env` ! Ajoutez-le à `.gitignore`.

---

## ⚙️ Configuration des Dashboards

Éditez le fichier `config.py` pour définir vos dashboards et panels.

### Structure de configuration

```python
DASHBOARDS = [
    {
        "name": "Nom du Dashboard",
        "uid": "uid-dashboard",
        "panels": [
            {
                "name": "Nom du Panel",
                "panel_id": "1",
                "datasource": {
                    "type": "grafana-postgresql-datasource",
                    "uid": "datasource-uid"
                },
                "sql_query": """
                    SELECT votre_requete
                    WHERE date >= '{time_from}'
                    AND date <= '{time_to}'
                """,
                "format": "table",
                "aggregation": "last",
                "unit": "%",
                "thresholds": {"warning": 95, "critical": 90}
            }
        ]
    }
]
```

### Paramètres disponibles

| Paramètre | Description | Exemple |
|-----------|-------------|---------|
| `name` | Nom du dashboard/panel | `"Toolchain Performance"` |
| `uid` | UID Grafana du dashboard | `"cebe2faxirjeob"` |
| `panel_id` | ID du panel | `"1"` |
| `sql_query` | Requête SQL (avec placeholders) | Voir exemples |
| `unit` | Unité de mesure | `"%"`, `"s"`, `"ms"`, `""` |
| `aggregation` | Méthode d'agrégation | `"last"`, `"avg"`, `"max"`, `"min"` |
| `thresholds` | Seuils pour couleurs | `{"warning": 95, "critical": 90}` |

### Placeholders dans les requêtes SQL

Utilisez ces placeholders qui seront remplacés automatiquement :

- `{time_from}` : Date de début (format ISO 8601)
- `{time_to}` : Date de fin (format ISO 8601)

---

## 📊 Périodes d'analyse

Configurées dans `config.py` :

```python
TIME_PERIODS = [
    {"label": "14 derniers jours", "days": 14, "grafana_range": "14d"},
    {"label": "2 derniers mois", "days": 60, "grafana_range": "60d"},
    {"label": "3 derniers mois", "days": 90, "grafana_range": "90d"}
]
```

Modifiez selon vos besoins !

---

## 🏃 Utilisation

### Exécution manuelle

```bash
python main.py
```

### Résultats

1. **Logs** : Fichiers détaillés dans `logs/grafana_report_YYYYMMDD_HHMMSS.log`
2. **Rapport HTML** : Sauvegardé dans `reports/report_YYYYMMDD_HHMMSS.html`
3. **Email** : Envoyé aux destinataires configurés

### Exemple de sortie console

```
INFO - Logger initialisé - fichier: logs/grafana_report_20250114_103045.log
INFO - Client Grafana initialisé pour https://orchestrator-dashboard.group.echonet
INFO - 🚀 Début de l'interrogation de tous les dashboards
INFO - === Interrogation du dashboard 'Toolchain Performance' ===
INFO - ✓ Panel 'Demand Success Rate' (14d): 96.5
INFO - ✓ Panel 'Demand Success Rate' (60d): 95.2
INFO - ✓ Panel 'Demand Success Rate' (90d): 94.8
INFO - Dashboard 'Toolchain Performance' terminé
INFO - ✅ Interrogation terminée - 3 dashboard(s) traité(s)
INFO - Génération du HTML de l'email
INFO - Rapport HTML sauvegardé: reports/report_20250114_103045.html
INFO - ✅ Connexion SMTP réussie
INFO - ✅ Email envoyé avec succès à 2 destinataire(s)
```

---

## 🎨 Aperçu de l'Email

L'email généré contient :

- **En-tête stylé** avec gradient et date de génération
- **Sections par dashboard** avec bordure colorée
- **Cards par panel** avec :
  - Nom du panel avec icône 📊
  - 3 badges pour les 3 périodes
  - Valeurs colorées (vert/orange/rouge selon seuils)
  - Indicateur de tendance (↗ ↘ →)
- **Footer** avec informations de contact

### Système de couleurs

| Couleur | Signification (pour %) | Seuils par défaut |
|---------|------------------------|-------------------|
| 🟢 Vert | Bon (≥ warning) | ≥ 95% |
| 🟠 Orange | Attention (≥ critical) | ≥ 90% |
| 🔴 Rouge | Critique (< critical) | < 90% |
| ⚪ Gris | Donnée manquante | N/A |

---

## 🔧 Architecture Technique

### Modules

#### `grafana_client.py`
Client pour interroger l'API Grafana :
- Exécution de requêtes SQL via l'API
- Gestion des erreurs et timeouts
- Support multi-dashboards et multi-panels

#### `email_generator.py`
Génération du HTML :
- Templates HTML/CSS inline (compatible Outlook)
- Formatage intelligent des valeurs
- Calcul automatique des tendances
- Système de couleurs par seuils

#### `email_sender.py`
Envoi SMTP :
- Support TLS
- Gestion CC/BCC
- Conversion HTML → texte simple (fallback)
- Test de connexion

#### `logger_config.py`
Logging :
- Fichiers horodatés
- Rotation automatique
- Nettoyage des vieux logs (30 jours)

#### `config.py`
Configuration centralisée des dashboards, panels et périodes

#### `main.py`
Orchestration complète du workflow

---

## 🛡️ Gestion des Erreurs

### Logs détaillés

Tous les événements sont loggés :
- ✅ Succès : niveau `INFO`
- ⚠️ Avertissements : niveau `WARNING`
- ❌ Erreurs : niveau `ERROR`

### Comportement en cas d'erreur

- **Panel sans données** : Valeur `N/A` dans le rapport
- **Dashboard en erreur** : Continue avec les autres
- **Erreur SMTP** : Rapport sauvegardé localement
- **Erreur SQL** : Détails dans les logs

---

## 📅 Automatisation

### Avec Cron (Linux/Mac)

```bash
# Éditer crontab
crontab -e

# Exécution tous les lundis à 9h
0 9 * * 1 cd /chemin/vers/grafana-reporter && /usr/bin/python3 main.py

# Exécution quotidienne à 8h
0 8 * * * cd /chemin/vers/grafana-reporter && /usr/bin/python3 main.py
```

### Avec Task Scheduler (Windows)

1. Ouvrir le Planificateur de tâches
2. Créer une tâche de base
3. Déclencheur : Selon planning
4. Action : Démarrer un programme
   - Programme : `python.exe`
   - Arguments : `main.py`
   - Répertoire : `C:\chemin\vers\grafana-reporter`

---

## 🔐 Sécurité

### Bonnes pratiques

✅ **Clés API** : Utilisez des clés avec droits minimaux (lecture seule)  
✅ **Fichier .env** : Ne JAMAIS commiter (ajouter à `.gitignore`)  
✅ **Mots de passe** : Utilisez des mots de passe d'application (Outlook)  
✅ **SSL/TLS** : Toujours activé pour SMTP  
✅ **Logs** : Nettoyage automatique des vieux fichiers

### Exemple `.gitignore`

```
.env
logs/
reports/
__pycache__/
*.pyc
*.log
```

---

## 🐛 Dépannage

### Problème : "Impossible d'extraire la valeur"

**Cause** : Structure de réponse Grafana différente  
**Solution** : Vérifier la requête dans Grafana d'abord, ajuster le parsing dans `grafana_client.py`

### Problème : "Erreur d'authentification SMTP"

**Cause** : Identifiants incorrects ou MFA activé  
**Solution** : 
- Pour Outlook : Créer un "mot de passe d'application"
- Vérifier `SMTP_USERNAME` et `SMTP_PASSWORD`

### Problème : "Variables d'environnement manquantes"

**Cause** : Fichier `.env` absent ou mal configuré  
**Solution** : Créer/compléter le fichier `.env` à la racine

### Problème : Requête SQL échoue

**Cause** : Syntaxe SQL ou dates invalides  
**Solution** : Tester la requête directement dans Grafana, vérifier les placeholders

---

## 📈 Évolutions Futures

Idées d'améliorations possibles :

- [ ] Support d'autres datasources (Prometheus, InfluxDB)
- [ ] Graphiques intégrés dans l'email (images)
- [ ] Export PDF en plus du HTML
- [ ] Interface web pour configuration
- [ ] Alertes basées sur seuils
- [ ] Historique des rapports dans une base
- [ ] Support de templates d'email personnalisables
- [ ] API REST pour déclencher les rapports

---

## 📞 Support

Pour toute question ou problème :

1. Vérifier les logs dans `logs/`
2. Consulter cette documentation
3. Tester les composants individuellement
4. Contacter l'équipe infrastructure

---

## 📝 Licence

Projet interne - Tous droits réservés

---

**Dernière mise à jour** : 14 Janvier 2025  
**Version** : 1.0.0  
**Python** : 3.12.12