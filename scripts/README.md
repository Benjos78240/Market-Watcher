# scripts/ README

Ce dossier contient les scripts principaux du projet **market-watcher**. Voici un aperçu des fichiers et de leur utilité :

## Scripts principaux

- **api_server.py** : Serveur FastAPI pour exposer des endpoints (API REST) pour la récupération de métriques système et autres données du projet.
- **compute_positions.py** : Calcule les positions courantes des utilisateurs à partir des transactions stockées en base Oracle.
- **crypto_watcher_.py** : Surveillance et alertes sur les actifs crypto, avec notifications Discord.
- **extraction_coinbase_.py** : Extraction des données de prix depuis l'API Coinbase et insertion en base.
- **extraction_coinbase_histo_.py** : Extraction historique des prix depuis Coinbase (batch).
- **extraction_eodhd_.py** : Extraction de données financières (actions, indices) via l'API EODHD.
- **extraction_eodhd_funda.py** : Extraction des données fondamentales (ratios, etc.) via EODHD.
- **extraction_eodhd_hist.py** : Extraction historique de prix via EODHD.
- **extraction_eodhd_indice.py** : Extraction de données pour les indices (ex : CAC40).
- **optimized_strategy_stock_watcher.py** : Optimisation de stratégies de suivi d'actions (backtest, skopt).
- **search_ticker_eodhd.py** : Recherche et validation de tickers sur EODHD.
- **show_watchlist.py** : Affiche la watchlist des utilisateurs depuis la base Oracle.
- **stock_watcher_.py** : Surveillance et alertes sur les actions/ETF, notifications Discord.
- **stock_watcher_pick.py** : Sélection d'actions (ex : CAC40) et analyse de portefeuille.
- **sync_watchlist_to_transactions.py** : Synchronise la watchlist utilisateur avec les transactions en base.
- **update_users.py** : Mise à jour des utilisateurs, tickers et données associées.
- **user_watchlist_alert.py** : Analyse la watchlist utilisateur et envoie des alertes Discord.

## Sous-dossiers

- **system_mgt/** : Scripts de gestion système (métriques, monitoring SSH, etc.).
- **utils/** : Fonctions utilitaires (connexion Oracle, logs, Discord, indicateurs, etc.).
- **old/** : Anciennes versions ou scripts de tests.
- **run/** : Scripts shell pour automatiser l'exécution des scripts Python (cron, batch, etc.).

## Scripts shell

- **run_extraction_alpha_vantage.sh** : Lance l'extraction via Alpha Vantage.
- **run_extraction_coinbase_histo.sh** : Extraction historique Coinbase.
- **python_crontab.sh** : Utilitaire pour logs d'environnement Python (pour cron).

Chaque script Python est généralement autonome et peut être lancé directement ou via un script shell associé dans `run/`.

> **Remarque :** Certains scripts nécessitent une connexion à une base Oracle et des variables d'environnement (voir `config/`).
