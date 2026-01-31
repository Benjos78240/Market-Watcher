HEAD

=======
# Market Watcher

Ce projet permet de surveiller, extraire et analyser des données financières (actions, crypto, indices) avec Python, FastAPI et une base Oracle. Il est conçu pour l’automatisation, l’analyse de portefeuille et l’alerte via Discord.

## Points forts
- Extraction automatisée de données (EODHD, Coinbase, Alpha Vantage…)
- API REST (FastAPI) pour exposer les données
- Gestion de portefeuilles et alertes personnalisées
- Scripts shell pour automatiser les tâches (cron, batch)
- Architecture modulaire (scripts, utils, system_mgt…)

## Structure du projet
- `scripts/` : Scripts principaux (voir scripts/README.md)
- `utils/` : Fonctions utilitaires (connexion Oracle, logs, Discord…)
- `system_mgt/` : Scripts de gestion système
- `run/` : Scripts shell d’automatisation
- `config/` : (ignoré par git) Clés API, secrets, configs sensibles
- `data/`, `logs/` : (ignorés) Données et logs

## Sécurité
- **Ne jamais versionner le dossier `config/` ni aucun secret !**
- Utiliser des fichiers `.example` pour montrer la structure des configs sans les vraies clés.

## Démarrage rapide
1. Cloner le repo
2. Créer un environnement virtuel Python et installer les dépendances (`pip install -r config/requirements.txt`)
3. Copier les fichiers de config `.example` vers `config/` et remplir avec vos clés
4. Lancer les scripts ou l’API selon vos besoins

## Screenshots (Grafana)

Surveillance système (API FastAPI + Grafana) :

![Dashboard Grafana Système](docs/grafana_dashboard_system.png)

Suivi de positions et indicateurs techniques ENR :

![Dashboard Grafana Positions Siemens Energy AG](docs/Suivi_de_Positions_ENR.png)

Suivi de positions et indicateurs techniques SOLANA-EURO :

![Dashboard Grafana Positions SOLANA-EURO](docs/Suivi_de_Positions_SOL_EUR.png)

Alerte Discord Signal Achat-Vente :

![Dashboard Alerte Discord](docs/Alerte_Discord_Captain_Crypto.png)

Alerte Discord Signal Suivi de Position :

![Dashboard Alerte Discord](docs/Alerte_Discord_Synthèse_Suivi.png)

## Valorisation pour recruteurs/clients

Ce projet démontre :
- Maîtrise de l’automatisation de bout en bout (extraction, analyse, alertes, monitoring)
- Développement d’API modernes (FastAPI)
- Intégration de bases de données complexes (Oracle)
- Mise en place de monitoring et visualisation avancée (Grafana)
- Sécurité et bonnes pratiques (aucun secret dans le code, .gitignore adapté)
- Capacité à interfacer avec des outils externes (Discord, webhooks)
- Code modulaire, prêt pour la production

> **Contactez-moi via GitHub ou les réseaux sociaux!**

## Valorisation technique
- Python avancé (pandas, FastAPI, dotenv, etc.)
- SQL, automatisation shell, cron
- Monitoring, alerting, reporting
- Documentation claire et exemples fournis

---

Pour toute question ou collaboration, n’hésitez pas à me contacter.
>>>>>>> afb792a (Initial public version (secure, no secrets, docs/screenshots added))
