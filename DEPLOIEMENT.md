# Déploiement — refonte octobre 2026

Ce guide accompagne l'audit `docs/AUDIT-2026-10.md` (à la racine du dossier API). Il concerne deux dépôts :

- `foirfouille-api` (serveur Debian du magasin) : l'API, la sync SQL Server → PostgreSQL et frpc ;
- `gateway` (VPS Hostinger) : frps et la sync PostgreSQL Debian → PostgreSQL Hostinger. Voir `gateway/README.md`.

> **Ordre important.** frps exige désormais TLS et `auth.additionalScopes`. Il faut mettre à jour frpc (Debian) et frps (Hostinger) **en même temps**, avec le **nouveau token**. Sinon, le tunnel (donc api.ffnancy.fr) est coupé jusqu'à ce que les deux côtés soient alignés.

---

## 1. Préparer les secrets (avant tout déploiement)

| Secret | Où | Action |
|---|---|---|
| Token frp | `frpc.toml` (Debian) + `frps.toml` (Hostinger) | Le nouveau token se génère avec `openssl rand -hex 32` et doit être identique des deux côtés. L'ancien est faible et ne doit plus servir. |
| Mot de passe PG Hostinger `post` | `gateway/.env` (`DST_PASS`) | L'ancien mot de passe figure dans l'historique git : il faut le **changer** (voir gateway/README.md). |
| Login SQL Server | `.env.docker` (`DB_USER`/`DB_PASSWORD`) | La sync se connecte aujourd'hui avec **`sa`**. Il faut créer un login en lecture seule (script ci-dessous). |
| Clés d'API | `.env.docker` (`API_KEYS`) | **Obligatoire** (sans clé, l'API refuse de démarrer, sauf `ALLOW_NO_AUTH=true`). Générer une clé par application cliente avec `node -e "console.log(require('crypto').randomBytes(32).toString('base64url'))"`. |
| Dashboards sync | `.env.docker` + `gateway/.env` | `DASHBOARD_USER` / `DASHBOARD_PASSWORD` : sans eux, les dashboards renvoient 503. |
| Alertes (optionnel) | les deux `.env` | `ALERT_WEBHOOK_URL` : webhook Slack, Discord ou Mattermost. |

### Login SQL Server en lecture seule (à exécuter dans SSMS, par un administrateur)

```sql
USE [master];
CREATE LOGIN ff_reader WITH PASSWORD = 'REMPLACER_PAR_UN_MOT_DE_PASSE_FORT', CHECK_POLICY = ON;
GO
USE [foirfouille];
CREATE USER ff_reader FOR LOGIN ff_reader;
ALTER ROLE db_datareader ADD MEMBER ff_reader;
GO
```

Ensuite, dans `.env.docker` : `DB_USER=ff_reader` et `DB_PASSWORD=…`. SQL Server 2017 est en RTM-GDR (correctif de 2020) : appliquer le dernier Cumulative Update 2017.

---

## 2. Serveur Debian (`~/api`)

```bash
cd ~/api && git pull
```

Compléter `.env.docker`. Toutes les variables sont listées dans `.env.docker.example` :

```
API_KEYS=<clé1>,<clé2>
DASHBOARD_USER=admin
DASHBOARD_PASSWORD=<mot de passe fort>
ALERT_WEBHOOK_URL=            # optionnel
# API_BIND=127.0.0.1          # pour fermer le port 3000 au réseau local
```

Mettre `frpc.toml` à jour selon `frpc.toml.example` : nouveau token, `auth.additionalScopes`, `transport.tls.enable`.

```bash
docker compose --env-file .env.docker up -d --build
docker compose ps                         # api et sync « healthy »
curl -s localhost:3000/api/health         # {"status":"ok","db":"ok","auth":"enabled"}
```

### Première synchronisation

Au démarrage de chaque sync, le schéma est appliqué automatiquement (`postgres/migrations/*.sql` puis `init.sql`, montés dans le conteneur sync). Au premier passage :

- la migration 001 ajoute des colonnes, met une clé primaire sur `mvtreg` (les anciennes lignes, qui contenaient des doublons, sont supprimées puis rechargées) et force un rechargement complet de `articles`, `article_infosup` et `cdefou_reception` ;
- la sync détecte que les nouvelles colonnes de `mvtart` sont vides et **recharge 24 mois de mouvements, mois par mois** ;
- `prix_vente_historique` est chargé en entier (environ 1,5 M de lignes source).

Ce premier passage est long (de quelques dizaines de minutes à plus d'une heure). Il vaut mieux le lancer **hors des heures d'ouverture**, depuis le dashboard (`http://<debian>:3001`, bouton « Lancer sync ») ou en attendant le cron de 06:45. Les passages suivants sont incrémentaux : fenêtre glissante de 10 jours pour les mouvements.

Pour vérifier : le dashboard affiche une ligne par table. `_run` doit valoir `ok` et `_schema` doit indiquer `ok`.

---

## 3. VPS Hostinger (`~/gateway`)

Suivre `gateway/README.md` : `.env` (`DST_*`, `SRC_*`, `DASHBOARD_*`), `frps.toml` (même token que frpc), rotation du mot de passe `post`, puis `docker compose up -d --build`.

La sync Hostinger tourne à **07:30** (heure de Paris). Elle attend la fin de la sync Debian (ligne `_run` = `running`) et recopie automatiquement **toutes** les tables, nouvelles colonnes comprises. Il n'y a plus de schéma à maintenir à la main.

---

## 4. Applications clientes de l'API

- Ajouter l'en-tête `X-API-Key: <clé>` à chaque appel, sauf `/api/health` et `/api/articles/:id/photo`.
- Les changements visibles sont décrits dans `API_DOCUMENTATION.md` : nouvelle définition du CA, `dateFin` inclusive, filtres `site` exacts, `/api/commandes` relu depuis `commande_fou`/`cdefou_ligne`, format de `/api/health`.

---

## 5. Retour arrière

- **Code** : `git revert` ou `git checkout <commit précédent>`, puis `docker compose up -d --build`.
- **Base** : la migration 001 n'ajoute que des colonnes et des tables, elle ne supprime rien, sauf les anciennes lignes de `mvtreg` sans identifiant, que la sync recharge. L'ancien code ignore les nouvelles colonnes et tables.
