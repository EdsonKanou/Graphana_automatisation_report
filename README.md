# Grafana Email Exporter

🚀 **Solution complète d'export automatique de dashboards Grafana vers Email**

Automatise la récupération quotidienne des métriques depuis vos dashboards Grafana et les envoie par email sous forme de tableaux HTML stylés.

---

## 📋 Table des matières

1. [Fonctionnalités](#fonctionnalités)
2. [Prérequis](#prérequis)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Utilisation](#utilisation)
6. [Planification automatique](#planification-automatique)
7. [Dépannage](#dépannage)
8. [Personnalisation](#personnalisation)

---

## ✨ Fonctionnalités

- ✅ **Connexion automatique à Grafana via API**
- ✅ **Récupération des métriques de tous les panels**
- ✅ **Support des variables/filtres de dashboard**
- ✅ **Agrégation des données** (dernière valeur, moyenne, max, min, sum)
- ✅ **Génération de tableaux HTML stylés**
- ✅ **Statistiques récapitulatives automatiques**
- ✅ **Envoi d'emails HTML via SMTP**
- ✅ **Planification quotidienne automatique**
- ✅ **Logs détaillés pour le suivi**
- ✅ **Alertes en cas d'échec**

---

## 🔧 Prérequis

### Système
- **Python 3.8+** installé
- Accès à un serveur SMTP (Gmail, Outlook, serveur d'entreprise, etc.)
- Accès à une instance Grafana avec API activée

### Grafana
1. **Créer un API Key** dans Grafana:
   - Aller dans `Configuration` → `API Keys`
   - Cliquer sur `New API Key`
   - Nom: `Email Exporter`
   - Rôle: `Viewer` (suffisant pour lire les dashboards)
   - Copier la clé générée

2. **Identifier le UID du dashboard**:
   - Ouvrir votre dashboard dans Grafana
   - L'URL contient le UID: `https://grafana.com/d/DASHBOARD_UID/nom-dashboard`
   - Copier la partie `DASHBOARD_UID`

### Email (Gmail)
Si vous utilisez Gmail:
1. Activer l'authentification à 2 facteurs
2. Générer un "Mot de passe d'application":
   - Aller dans `Compte Google` → `Sécurité` → `Mots de passe des applications`
   - Créer un nouveau mot de passe pour "Autre (nom personnalisé)"
   - Utiliser ce mot de passe dans la configuration

---

## 📦 Installation

### 1. Cloner ou télécharger le projet

```bash
# Créer un dossier pour le projet
mkdir grafana-exporter
cd grafana-exporter
```

### 2. Créer un environnement virtuel (recommandé)

```bash
# Créer l'environnement virtuel
python -m venv venv

# Activer l'environnement
# Sur Linux/Mac:
source venv/bin/activate
# Sur Windows:
venv\Scripts\activate
```

### 3. Installer les dépendances

```bash
pip install -r requirements.txt
```

### 4. Structure des fichiers

Votre dossier doit contenir:
```
grafana-exporter/
├── .env                    # Configuration (à créer)
├── requirements.txt        # Dépendances Python
├── grafana_client.py       # Client API Grafana
├── data_formatter.py       # Formatage des données
├── email_sender.py         # Envoi d'emails
├── main.py                 # Script principal
├── scheduler.py            # Planificateur
├── grafana_export.log      # Logs (généré automatiquement)
└── scheduler.log           # Logs du scheduler (généré automatiquement)
```

---

## ⚙️ Configuration

### 1. Créer le fichier .env

Créer un fichier `.env` à la racine du projet avec le contenu suivant:

```bash
# Configuration Grafana
GRAFANA_URL=https://your-grafana-instance.com
GRAFANA_API_KEY=votre_api_key_grafana
DASHBOARD_UID=uid_de_votre_dashboard

# Configuration Email
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=votre.email@gmail.com
SMTP_PASSWORD=votre_mot_de_passe_application
EMAIL_FROM=votre.email@gmail.com
EMAIL_TO=destinataire1@company.com,destinataire2@company.com

# Configuration générale
TIMEZONE=Europe/Paris
SCHEDULE_TIME=08:00
ENABLE_WEEKLY_REPORT=false
```

### 2. Paramètres détaillés

| Paramètre | Description | Exemple |
|-----------|-------------|---------|
| `GRAFANA_URL` | URL de base de Grafana (sans slash final) | `https://grafana.company.com` |
| `GRAFANA_API_KEY` | Clé API Grafana (rôle Viewer minimum) | `eyJrIjoiT0tTc...` |
| `DASHBOARD_UID` | UID unique du dashboard | `abc123def` |
| `SMTP_SERVER` | Serveur SMTP | `smtp.gmail.com`, `smtp.office365.com` |
| `SMTP_PORT` | Port SMTP (587=TLS, 465=SSL) | `587` |
| `SMTP_USER` | Utilisateur SMTP | `user@company.com` |
| `SMTP_PASSWORD` | Mot de passe SMTP | `password123` |
| `EMAIL_FROM` | Adresse d'envoi | `monitoring@company.com` |
| `EMAIL_TO` | Destinataires (séparés par virgules) | `manager@company.com,team@company.com` |
| `SCHEDULE_TIME` | Heure d'envoi quotidien (HH:MM) | `08:00` |
| `ENABLE_WEEKLY_REPORT` | Active rapport hebdomadaire | `true` ou `false` |

### 3. Serveurs SMTP courants

**Gmail:**
```
SMTP_SERVER=smtp.gmail.com
SMTP_PORT=587
```

**Outlook/Office 365:**
```
SMTP_SERVER=smtp.office365.com
SMTP_PORT=587
```

**Yahoo:**
```
SMTP_SERVER=smtp.mail.yahoo.com
SMTP_PORT=587
```

---

## 🚀 Utilisation

### 1. Tester la configuration

Avant de lancer l'export complet, testez votre configuration:

```bash
python main.py test
```

Cette commande vérifie:
- ✓ La connexion à Grafana
- ✓ L'accès au dashboard
- ✓ La connexion SMTP
- ✓ L'envoi d'email de test

### 2. Générer un rapport sans l'envoyer

Pour prévisualiser le rapport HTML:

```bash
python main.py save rapport_test.html
```

Le fichier HTML sera créé et vous pourrez l'ouvrir dans un navigateur.

### 3. Exécuter l'export manuellement

Pour lancer un export complet avec envoi d'email:

```bash
python main.py
```

### 4. Options de ligne de commande

```bash
# Aide
python main.py help

# Test de configuration
python main.py test

# Sauvegarder dans un fichier
python main.py save [nom_fichier.html]

# Export et envoi normal
python main.py
```

---

## ⏰ Planification automatique

### 1. Lancer le planificateur

Pour que l'export s'exécute automatiquement chaque jour:

```bash
python scheduler.py
```

Le planificateur:
- ✅ Reste actif en arrière-plan
- ✅ Exécute l'export à l'heure définie dans `SCHEDULE_TIME`
- ✅ Envoie des alertes en cas d'échec
- ✅ Log toutes les opérations

### 2. Options du scheduler

```bash
# Lancer le planificateur en mode continu
python scheduler.py

# Exécuter immédiatement (sans attendre l'heure planifiée)
python scheduler.py now

# Tester la configuration
python scheduler.py test

# Aide
python scheduler.py help
```

### 3. Exécution en tant que service (Linux)

Pour que le scheduler démarre automatiquement au boot:

**Créer un service systemd:**

```bash
sudo nano /etc/systemd/system/grafana-exporter.service
```

**Contenu du fichier:**

```ini
[Unit]
Description=Grafana Email Exporter Scheduler
After=network.target

[Service]
Type=simple
User=votre_utilisateur
WorkingDirectory=/chemin/vers/grafana-exporter
Environment="PATH=/chemin/vers/grafana-exporter/venv/bin"
ExecStart=/chemin/vers/grafana-exporter/venv/bin/python scheduler.py
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
```

**Activer et démarrer le service:**

```bash
sudo systemctl daemon-reload
sudo systemctl enable grafana-exporter
sudo systemctl start grafana-exporter

# Vérifier le statut
sudo systemctl status grafana-exporter

# Voir les logs
sudo journalctl -u grafana-exporter -f
```

### 4. Exécution avec cron (Alternative)

Si vous préférez utiliser cron:

```bash
# Éditer la crontab
crontab -e

# Ajouter cette ligne (exécution quotidienne à 8h00)
0 8 * * * cd /chemin/vers/grafana-exporter && /chemin/vers/venv/bin/python main.py >> /chemin/vers/cron.log 2>&1
```

---

## 🔍 Dépannage

### Problème: "Erreur de connexion SMTP"

**Causes possibles:**
- Mot de passe incorrect
- Authentification à 2 facteurs non configurée (Gmail)
- Firewall bloquant le port SMTP

**Solutions:**
1. Vérifier que le mot de passe d'application est correct (Gmail)
2. Tester avec un autre port (587 ou 465)
3. Vérifier les règles firewall

### Problème: "Dashboard non trouvé"

**Causes possibles:**
- UID incorrect
- Clé API sans permissions suffisantes

**Solutions:**
1. Vérifier le UID dans l'URL du dashboard
2. Créer une nouvelle API Key avec rôle "Viewer" minimum

### Problème: "Aucune donnée récupérée"

**Causes possibles:**
- Panels vides ou sans données
- Période de temps incorrecte
- Datasource non supportée

**Solutions:**
1. Vérifier que le dashboard contient des données
2. Ajuster la période (`time_from`, `time_to`)
3. Vérifier les logs pour plus de détails

### Consulter les logs

```bash
# Logs de l'export
cat grafana_export.log

# Logs du scheduler
cat scheduler.log

# Suivre les logs en temps réel
tail -f grafana_export.log
```

---

## 🎨 Personnalisation

### Modifier la période de données

Dans `main.py`, fonction `export_and_send()`:

```python
# Dernières 24 heures
time_from='now-24h'

# Derniers 7 jours
time_from='now-7d'

# Dernier mois
time_from='now-30d'

# Dernière heure
time_from='now-1h'
```

### Changer la méthode d'agrégation

```python
# Dernière valeur (par défaut)
agg_method='last'

# Moyenne sur la période
agg_method='mean'

# Valeur maximum
agg_method='max'

# Valeur minimum
agg_method='min'

# Somme
agg_method='sum'
```

### Personnaliser l'email

Modifier le texte d'introduction:

```python
exporter.export_and_send(
    intro_text="""
    Bonjour,<br><br>
    Voici votre rapport personnalisé avec les métriques importantes.
    N'hésitez pas à me contacter pour toute question.
    """
)
```

### Modifier le style CSS

Éditer la méthode `create_full_html_report()` dans `data_formatter.py` pour changer:
- Les couleurs
- Les polices
- La mise en page
- Les styles de tableaux

### Ajouter des rapports hebdomadaires

Dans `.env`:
```bash
ENABLE_WEEKLY_REPORT=true
```

Le rapport hebdomadaire sera envoyé chaque lundi à 9h00.

---

## 📊 Structure des données

Le système récupère les données selon cette logique:

1. **Dashboard** → Contient plusieurs panels
2. **Panel** → Contient une ou plusieurs métriques
3. **Métrique** → Série de valeurs temporelles
4. **Agrégation** → Calcul d'une valeur unique par métrique
5. **Tableau** → Présentation formatée des résultats

---

## 🛡️ Sécurité

### Bonnes pratiques

1. **Ne jamais commiter le fichier .env** dans Git
   ```bash
   echo ".env" >> .gitignore
   ```

2. **Utiliser des API Keys avec permissions minimales**
   - Rôle "Viewer" suffit pour lire les dashboards

3. **Utiliser des mots de passe d'application** (Gmail)
   - Ne jamais utiliser le mot de passe principal

4. **Restreindre les permissions du fichier .env**
   ```bash
   chmod 600 .env
   ```

5. **Renouveler régulièrement les clés API**

---

## 📝 Support et contribution

- Pour signaler un bug, ouvrir une issue
- Pour proposer une amélioration, créer une pull request
- Documentation Grafana API: https://grafana.com/docs/grafana/latest/http_api/

---

## 📄 Licence

Ce projet est sous licence MIT. Vous êtes libre de l'utiliser, le modifier et le distribuer.

---

**Développé avec ❤️ pour simplifier le monitoring quotidien**