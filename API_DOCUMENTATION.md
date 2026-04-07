# Documentation API Foirfouille

**Base URL** : `https://api.ffnancy.fr`
**Format** : JSON
**Méthode** : GET uniquement (API lecture seule)
**Données** : Synchronisées depuis SQL Server chaque nuit à 6h45 (heure Paris)

---

## Santé

```
GET /api/health
```
```json
{ "status": "ok", "database": "foirfouille", "server": "postgres" }
```

---

## Articles

### Liste avec filtres
```
GET /api/articles
```

| Paramètre | Type   | Description                          | Défaut |
|-----------|--------|--------------------------------------|--------|
| `search`  | string | Recherche dans libellé               | —      |
| `codein`  | string | Code interne (partiel)               | —      |
| `ean`     | string | EAN / GTIN                           | —      |
| `codefou` | string | Code fournisseur (partiel)           | —      |
| `actif`   | `1`/`0`| `1` = actif, `0` = suspendu         | —      |
| `page`    | int    | Numéro de page                       | 1      |
| `limit`   | int    | Lignes par page (max 500)            | 50     |

**Réponse** :
```json
{
  "page": 1,
  "limit": 50,
  "articles": [
    {
      "no_id": 12345,
      "codein": "ABC123",
      "libelle1": "MON ARTICLE",
      "libelle2": "SOUS-LIBELLE",
      "lib_ticket": "MON ART",
      "tax_code": "A",
      "ach_code": "X",
      "utilisable": "O",
      "actif": "O",
      "suspendu": null,
      "suividatecreation": "2020-01-15T00:00:00.000Z",
      "suividatemodif": "2024-06-01T00:00:00.000Z",
      "prix_vente_mini": 15.00,
      "prix_vente_maxi": 25.00,
      "eco_ttc": 0.10,
      "on_web": "O",
      "interdit_remise": null,
      "nomphoto": "ABC123.jpg",
      "datedebvente": null,
      "datefinvente": null,
      "pa": 10.50,
      "gtin": "3760000000001",
      "codefou_principal": "FOU01",
      "nom_fou_principal": "MON FOURNISSEUR",
      "ref_fou_principale": "REF-FOU-001",
      "pcb_principal": 6
    }
  ]
}
```

---

### Détail d'un article
```
GET /api/articles/:id
```

**Paramètre** : `id` = `no_id` de l'article

**Réponse** :
```json
{
  "article": { "no_id": 12345, "codein": "...", "libelle1": "...", "pa": 10.50, "..." : "..." },
  "gtins": [
    { "gtin": "3760000000001", "preferentiel": 1 }
  ],
  "stock": [
    { "site": "001", "qte": 12, "prmp": 10.20, "valstock": 122.40, "pv": 18.50, "stockdispo": 10 }
  ],
  "prix": [
    { "site": "001", "pv": 18.50 }
  ],
  "fournisseurs": [
    {
      "codefou": "FOU01", "nom_fou": "MON FOURNISSEUR",
      "reference": "REF-001", "ean13": "3760000000001",
      "pcb": 6, "delai": 7, "preference": 1,
      "prixachat": 10.50, "remise_promotion": 5.00
    }
  ]
}
```

---

### Fiche complète (référentiel)
```
GET /api/articles/:id/referentiel
```

Retourne toutes les informations consolidées de l'article :

```json
{
  "article": {
    "no_id": 12345, "codein": "ABC123", "libelle1": "MON ARTICLE",
    "prix_vente_mini": 15.00, "prix_vente_maxi": 25.00,
    "eco_ht": 0.08, "eco_ttc": 0.10,
    "on_web": "O", "pv_conseille": 19.90,
    "nom_code": "RAY01", "nom_libelle": "RAYON MAISON", "nom_niveau": 1
  },
  "gtins": [ { "gtin": "3760000000001", "preferentiel": 1 } ],
  "gammes": [
    { "gamme_code": "ETE2024", "gamme_libelle": "ÉTÉ 2024", "saison_code": "S24", "saison_libelle": "SAISON 2024" }
  ],
  "stock": [
    { "site": "001", "qte": 12, "prmp": 10.20, "valstock": 122.40, "pv": 18.50, "stockdispo": 10 }
  ],
  "stock_remballe": [],
  "prix": {
    "achat": 10.50,
    "vente_par_site": [ { "site": "001", "pv": 18.50 } ]
  },
  "fournisseurs": [
    { "codefou": "FOU01", "nom_fou": "MON FOURNISSEUR", "ref_fou": "REF-001", "pcb": 6, "prixachat": 10.50 }
  ],
  "performance": {
    "derniere_entree": "2024-05-10T00:00:00.000Z",
    "derniere_vente": "2024-06-15T00:00:00.000Z",
    "qte_totale_vendue": 248,
    "ca_ttc_total": 4588.00,
    "marge_totale": 1020.50
  },
  "notes": []
}
```

---

### Analyse mensuelle d'un article (ventes + stock mois par mois)

```
GET /api/articles/:id/mensuel?dateDebut=&dateFin=&site=
```

Retourne en une seule requête, mois par mois et par site :
- **Ventes** du mois (quantité, CA, marge)
- **Stock fin de mois** (dernier QteStock enregistré dans le mois)
- **Réceptions** du mois

| Paramètre   | Type   | Description                | Défaut     |
|-------------|--------|----------------------------|------------|
| `dateDebut` | date   | Date début `YYYY-MM-DD`    | 2024-01-01 |
| `dateFin`   | date   | Date fin `YYYY-MM-DD`      | 2099-12-31 |
| `site`      | string | Filtrer par site           | tous       |

**Réponse** :

```json
{
  "artNoId": "12345",
  "dateDebut": "2025-01-01",
  "dateFin": "2025-12-31",
  "data": [
    {
      "mois": "2025-12",
      "site": "001",
      "stock_fin_mois": 28,
      "prmp_fin_mois": 10.20,
      "ventes": {
        "nb_passages": 18,
        "qte_vendue": 17,
        "ca_ht": 274.00,
        "ca_ttc": 315.10,
        "marge": 72.00,
        "taux_marge": 22.86
      },
      "receptions": {
        "nb_receptions": 1,
        "qte_recue": 24
      }
    },
    {
      "mois": "2025-11",
      "site": "001",
      "stock_fin_mois": 45,
      "prmp_fin_mois": 10.20,
      "ventes": {
        "nb_passages": 12,
        "qte_vendue": 10,
        "ca_ht": 161.00,
        "ca_ttc": 185.15,
        "marge": 42.00,
        "taux_marge": 22.69
      },
      "receptions": null
    }
  ]
}
```

> `stock_fin_mois` = QteStock du **dernier mouvement du mois** (vente, réception ou autre).
> Si un mois n'a aucun mouvement il n'apparaît pas dans la réponse.
> `ventes` ou `receptions` sont `null` si aucun mouvement de ce type dans le mois.

---

### Mouvements d'un article

```
GET /api/articles/:id/mouvements
```

| Paramètre   | Type   | Description                    | Défaut     |
|-------------|--------|--------------------------------|------------|
| `dateDebut` | date   | Date début `YYYY-MM-DD`        | 2000-01-01 |
| `dateFin`   | date   | Date fin `YYYY-MM-DD`          | 2099-12-31 |
| `site`      | string | Filtrer par site (partiel)     | —          |
| `page`      | int    | Numéro de page                 | 1          |
| `limit`     | int    | Lignes par page (max 1000)     | 100        |

**Réponse** :
```json
{
  "page": 1, "limit": 100,
  "mouvements": [
    {
      "datmvt": "2024-06-15T00:00:00.000Z",
      "site": "001",
      "libmvt": "VENTE",
      "genremvt": 3,
      "qtemvt": 2,
      "mntmvtttc": 37.00,
      "margemvt": 8.20,
      "qtestock": 10
    }
  ]
}
```

**Valeurs `GenreMvt`** : `1` = entrée stock, `3` = vente, `4` = avoir/annulation, `9` = autre vente

> Le CA TTC (`ca_ttc`) représente les **ventes brutes** (mouvements négatifs des genremvt 3, 4, 9). Le champ `retours_ttc` contient les retours séparément. CA net = `ca_ttc - retours_ttc`.

---

## Fournisseurs

### Liste des fournisseurs
```
GET /api/fournisseurs?search=
```

| Paramètre | Type   | Description               |
|-----------|--------|---------------------------|
| `search`  | string | Recherche sur code/nom    |

**Réponse** :
```json
{
  "count": 142,
  "fournisseurs": [
    {
      "codefou": "FOU01",
      "nom": "MON FOURNISSEUR SARL",
      "adresse": "1 RUE DE LA PAIX",
      "telephone": "0383000000",
      "email": "contact@fournisseur.fr",
      "nb_articles": 320,
      "nb_actifs": 285,
      "nb_suspendus": 35
    }
  ]
}
```

---

### Articles d'un fournisseur
```
GET /api/fournisseurs/:code/articles?page=&limit=
```

**Réponse** :
```json
{
  "code": "FOU01",
  "page": 1, "limit": 50,
  "articles": [
    {
      "no_id": 12345, "codein": "ABC123", "libelle1": "MON ARTICLE",
      "ref_fou": "REF-FOU-001", "ean13": "3760000000001",
      "pcb": 6, "delai": 7, "suspendu": null,
      "prixachat": 10.50, "remise_promotion": 5.00,
      "pa": 10.50, "pv_central": 18.50
    }
  ]
}
```

---

### Commandes en cours d'un fournisseur
```
GET /api/fournisseurs/:code/commandes
```

| Paramètre   | Type | Description             | Défaut     |
|-------------|------|-------------------------|------------|
| `dateDebut` | date | Date début `YYYY-MM-DD` | 2020-01-01 |
| `dateFin`   | date | Date fin `YYYY-MM-DD`   | 2099-12-31 |
| `page`      | int  | Numéro de page          | 1          |
| `limit`     | int  | Max 1000                | 100        |

---

## Stock

### Stock global
```
GET /api/stock?site=&page=&limit=
```

| Paramètre | Type   | Description              | Défaut |
|-----------|--------|--------------------------|--------|
| `site`    | string | Filtrer par site         | tous   |
| `page`    | int    | Numéro de page           | 1      |
| `limit`   | int    | Max 1000                 | 100    |

---

### Stock d'un article (tous sites)
```
GET /api/stock/article/:id
```

**Réponse** :
```json
{
  "artnoid": 12345,
  "stock": [
    {
      "site": "001",
      "qte": 12,
      "prmp": 10.20,
      "valstock": 122.40,
      "pv": 18.50,
      "stockdispo": 10,
      "stockmort": 0,
      "stockcolis": 2,
      "dernierevente": "2024-06-15T00:00:00.000Z",
      "dernierereception": "2024-05-10T00:00:00.000Z",
      "nbjoursdernierevente": 5,
      "interditachat": null,
      "codefou": "FOU01"
    }
  ]
}
```

---

### Stock d'un article à une date précise
```
GET /api/stock/article/:id/historique?date=&site=
```

Retourne le stock tel qu'il était à une date donnée, en utilisant le champ `QteStock`
enregistré dans chaque mouvement (valeur du stock au moment exact du mouvement).

| Paramètre | Type   | Description                        | Défaut      |
|-----------|--------|------------------------------------|-------------|
| `date`    | date   | Date cible `YYYY-MM-DD`            | aujourd'hui |
| `site`    | string | Filtrer par site (partiel)         | tous        |

**Réponse** :
```json
{
  "artNoId": "12345",
  "date": "2025-12-31",
  "stock": [
    {
      "site": "001",
      "qte": 28,
      "prmp": 10.20,
      "date_dernier_mouvement": "2025-12-29T00:00:00.000Z",
      "libelle_dernier_mouvement": "VENTE"
    }
  ]
}
```

> **Principe** : on prend le dernier mouvement enregistré avant ou à la date demandée.
> Si aucun mouvement n'existe avant cette date, le site ne figure pas dans la réponse.

---

### Stock + ventes d'un article sur une période
```
GET /api/stock/article/:id/periode?dateDebut=&dateFin=&site=
```

Endpoint principal pour analyser un article sur une période passée.
Retourne en une seule requête : stock de début, stock de fin, ventes et réceptions.

| Paramètre   | Type   | Description                        | Défaut           |
|-------------|--------|------------------------------------|------------------|
| `dateDebut` | date   | Date début `YYYY-MM-DD`            | 1er du mois      |
| `dateFin`   | date   | Date fin `YYYY-MM-DD`              | aujourd'hui      |
| `site`      | string | Filtrer par site (partiel)         | tous             |

**Réponse** :
```json
{
  "artNoId": "12345",
  "dateDebut": "2025-12-01",
  "dateFin": "2025-12-31",
  "data": [
    {
      "site": "001",
      "stock_debut": {
        "qte": 45,
        "prmp": 10.20,
        "au": "2025-11-30T00:00:00.000Z"
      },
      "stock_fin": {
        "qte": 28,
        "prmp": 10.20,
        "au": "2025-12-29T00:00:00.000Z"
      },
      "ventes": {
        "site": "001",
        "nb_passages": 18,
        "qte_vendue": 17,
        "ca_ht": 274.00,
        "ca_ttc": 315.10,
        "marge": 72.00,
        "taux_marge": 22.86,
        "premiere_vente": "2025-12-02T00:00:00.000Z",
        "derniere_vente": "2025-12-29T00:00:00.000Z"
      },
      "receptions": {
        "site": "001",
        "nb_receptions": 1,
        "qte_recue": 0
      }
    }
  ]
}
```

> **Lecture** : stock_debut = dernier mouvement **avant** dateDebut / stock_fin = dernier mouvement **à ou avant** dateFin.
> Les champs `ventes` et `receptions` sont `null` si aucun mouvement sur la période.

---

### Stock d'un site (tous articles)
```
GET /api/stock/site/:site?page=&limit=
```

---

### Valorisation du stock par site
```
GET /api/stock/valorisation
```

**Réponse** :
```json
{
  "valorisation": [
    {
      "site": "001",
      "nb_articles": 4200,
      "qte_totale": 85000,
      "val_stock": 420000.00,
      "val_pv_theorique": 890000.00,
      "prmp_moyen": 12.50
    }
  ]
}
```

---

## Commandes

### Commandes en cours
```
GET /api/commandes
```

| Paramètre   | Type   | Description              | Défaut     |
|-------------|--------|--------------------------|------------|
| `dateDebut` | date   | Date début `YYYY-MM-DD`  | 2020-01-01 |
| `dateFin`   | date   | Date fin `YYYY-MM-DD`    | 2099-12-31 |
| `codefou`   | string | Filtrer par fournisseur  | —          |
| `page`      | int    | Numéro de page           | 1          |
| `limit`     | int    | Max 1000                 | 100        |

**Réponse** :
```json
{
  "page": 1, "limit": 100,
  "commandes": [
    {
      "no_commande": 98765,
      "codefou": "FOU01",
      "codein": "ABC123",
      "libelle": "MON ARTICLE",
      "ref_fou": "REF-001",
      "qte_cde": 24,
      "prix_brut": 12.00,
      "remise": 5.00,
      "prix_net": 11.40,
      "montant": 273.60,
      "qte_acceptee": 24,
      "qte_annulee": 0,
      "qte_attente": 0,
      "qte_reliquat": 0,
      "date_livraison_cible": "2024-07-01T00:00:00.000Z",
      "date_commande": "2024-06-01T00:00:00.000Z"
    }
  ]
}
```

---

### Détail d'une commande
```
GET /api/commandes/:noCommande
```

**Réponse** :
```json
{
  "noCommande": "98765",
  "nbLignes": 15,
  "montantTotal": 4250.80,
  "lignes": [ { "..." : "..." } ]
}
```

---

### Liste des réceptions
```
GET /api/commandes/receptions/liste
```

| Paramètre   | Type | Description              | Défaut     |
|-------------|------|--------------------------|------------|
| `dateDebut` | date | Date début `YYYY-MM-DD`  | 2020-01-01 |
| `dateFin`   | date | Date fin `YYYY-MM-DD`    | 2099-12-31 |
| `page`      | int  | Numéro de page           | 1          |
| `limit`     | int  | Max 1000                 | 100        |

---

## Mouvements

### Types de mouvements disponibles
```
GET /api/mouvements/types
```

Retourne tous les codes `GenreMvt` et libellés présents dans la base, avec leur nombre d'occurrences.
**À appeler en premier** pour connaître les codes de régularisation propres à votre magasin.

```json
{
  "types": [
    { "genremvt": 1, "libmvt": "RECEPTION",      "nb_occurrences": "142580" },
    { "genremvt": 3, "libmvt": "VENTE",           "nb_occurrences": "2130137" },
    { "genremvt": 2, "libmvt": "RETOUR CLIENT",   "nb_occurrences": "8420" },
    { "genremvt": 5, "libmvt": "REGULARISATION",  "nb_occurrences": "3210" },
    { "genremvt": 6, "libmvt": "INVENTAIRE",      "nb_occurrences": "1850" }
  ]
}
```

> Les codes exacts dépendent de votre paramétrage Mercalys. Consultez cet endpoint pour connaître la liste réelle.

---

### Mouvements articles (tous types)
```
GET /api/mouvements/articles
```

| Paramètre   | Type   | Description                              | Défaut     |
|-------------|--------|------------------------------------------|------------|
| `dateDebut` | date   | Date début `YYYY-MM-DD`                  | 2024-01-01 |
| `dateFin`   | date   | Date fin `YYYY-MM-DD`                    | 2099-12-31 |
| `site`      | string | Filtrer par site                         | —          |
| `codefou`   | string | Filtrer par fournisseur                  | —          |
| `genremvt`  | int    | Filtrer par type (ex: `1`, `3`, `5`...)  | tous       |
| `page`      | int    | Numéro de page                           | 1          |
| `limit`     | int    | Max 1000                                 | 100        |

**Types principaux `GenreMvt`** :

| Code | Type              |
|------|-------------------|
| `1`  | Entrée stock      |
| `3`  | Vente             |
| autres | Régularisations (voir `/api/mouvements/types`) |

---

### Entrées en stock
```
GET /api/mouvements/entrees
```

Toutes les réceptions / entrées en stock (`GenreMvt = 1`).
Inclut la **date de création de l'article** pour retrouver la première entrée.

| Paramètre   | Type   | Description                    | Défaut     |
|-------------|--------|--------------------------------|------------|
| `dateDebut` | date   | Date début `YYYY-MM-DD`        | 2024-01-01 |
| `dateFin`   | date   | Date fin `YYYY-MM-DD`          | 2099-12-31 |
| `site`      | string | Filtrer par site               | —          |
| `artNoId`   | int    | Filtrer sur un article précis  | —          |
| `page`      | int    | Numéro de page                 | 1          |
| `limit`     | int    | Max 1000                       | 100        |

**Réponse** :
```json
{
  "page": 1, "limit": 100,
  "entrees": [
    {
      "date_entree": "2025-12-10T00:00:00.000Z",
      "site": "001",
      "artnoid": 12345,
      "codein": "ABC123",
      "libelle1": "MON ARTICLE",
      "date_creation_article": "2020-03-15T00:00:00.000Z",
      "libelle_mouvement": "RECEPTION",
      "qte_entree": 24,
      "prmp": 10.20,
      "stock_apres_entree": 36,
      "codefou": "FOU01"
    }
  ]
}
```

> Pour trouver la **première entrée d'un article** : filtrez par `artNoId` avec `dateDebut=2000-01-01` et triez par date croissante (la première ligne = première réception).

---

### Régularisations de stock
```
GET /api/mouvements/regularisations
```

Tous les mouvements **hors ventes et hors réceptions** : inventaires, corrections, démarques, transferts, retours, etc.

| Paramètre   | Type   | Description                    | Défaut     |
|-------------|--------|--------------------------------|------------|
| `dateDebut` | date   | Date début `YYYY-MM-DD`        | 2024-01-01 |
| `dateFin`   | date   | Date fin `YYYY-MM-DD`          | 2099-12-31 |
| `site`      | string | Filtrer par site               | —          |
| `artNoId`   | int    | Filtrer sur un article précis  | —          |
| `page`      | int    | Numéro de page                 | 1          |
| `limit`     | int    | Max 1000                       | 100        |

**Réponse** :
```json
{
  "page": 1, "limit": 100,
  "regularisations": [
    {
      "date_mouvement": "2025-12-05T00:00:00.000Z",
      "site": "001",
      "artnoid": 12345,
      "codein": "ABC123",
      "libelle1": "MON ARTICLE",
      "genremvt": 6,
      "libelle_mouvement": "INVENTAIRE",
      "qte": -3,
      "valeur": -30.60,
      "stock_apres": 42,
      "prmp": 10.20
    }
  ]
}
```

> Une `qte` négative = sortie de stock (correction à la baisse, démarque…).
> Une `qte` positive = entrée hors réception (correction à la hausse, retour…).

---

### Synthèse par jour et site
```
GET /api/mouvements/synthese?dateDebut=&dateFin=&site=
```

**Réponse** :
```json
{
  "synthese": [
    {
      "jour": "2024-06-15",
      "site": "001",
      "genremvt": 3,
      "nb_mvt": 152,
      "qte_totale": 312,
      "mnt_ht": 4200.00,
      "mnt_ttc": 4830.00,
      "marge": 1050.00
    }
  ]
}
```

---

## Performance

### Chiffre d'affaires
```
GET /api/performance/ca
```

| Paramètre   | Type            | Description                  | Défaut     |
|-------------|-----------------|------------------------------|------------|
| `dateDebut` | date            | Date début `YYYY-MM-DD`      | 2024-01-01 |
| `dateFin`   | date            | Date fin `YYYY-MM-DD`        | 2099-12-31 |
| `site`      | string          | Filtrer par site             | tous       |
| `groupBy`   | `jour` / `mois` | Granularité                  | `jour`     |

**Réponse** :
```json
{
  "groupBy": "mois",
  "dateDebut": "2024-01-01",
  "dateFin": "2024-12-31",
  "site": "tous",
  "ca": [
    {
      "periode": "2024-06",
      "site": "001",
      "nb_ventes": 4820,
      "qte_vendue": 9640,
      "ca_ht": 85200.00,
      "ca_ttc": 97980.00,
      "retours_ttc": 1250.00,
      "marge": 21300.00,
      "taux_marge": 21.74
    }
  ]
}
```

---

### Hit-parade articles
```
GET /api/performance/hitparade
```

| Paramètre   | Type                  | Description                    | Défaut     |
|-------------|-----------------------|--------------------------------|------------|
| `dateDebut` | date                  | Date début `YYYY-MM-DD`        | 2024-01-01 |
| `dateFin`   | date                  | Date fin `YYYY-MM-DD`          | 2099-12-31 |
| `site`      | string                | Filtrer par site               | tous       |
| `limit`     | int                   | Nombre d'articles (max 500)    | 50         |
| `groupBy`   | `ca` / `qte` / `marge`| Critère de classement          | `ca`       |

**Réponse** :
```json
{
  "classementPar": "ca",
  "hitparade": [
    {
      "codein": "ABC123",
      "libelle1": "MON ARTICLE",
      "site": "001",
      "nb_passages": 320,
      "qte_vendue": 640,
      "ca_ht": 5760.00,
      "ca_ttc": 6624.00,
      "marge": 1440.00,
      "taux_marge": 21.74,
      "derniere_vente": "2024-06-15T00:00:00.000Z"
    }
  ]
}
```

---

### CA par fournisseur
```
GET /api/performance/ca/fournisseur?dateDebut=&dateFin=&site=
```

| Paramètre   | Type   | Description              | Défaut     |
|-------------|--------|--------------------------|------------|
| `dateDebut` | date   | Date début `YYYY-MM-DD`  | 2024-01-01 |
| `dateFin`   | date   | Date fin `YYYY-MM-DD`    | 2099-12-31 |
| `site`      | string | Filtrer par site         | tous       |

**Réponse** :
```json
{
  "ca_par_fournisseur": [
    {
      "fournisseur": "DURACELL",
      "code_fournisseur": "D034",
      "nb_articles": 12,
      "qte_vendue": 840,
      "ca_ht": 4200.00,
      "ca_ttc": 5040.00,
      "marge": 1680.00,
      "taux_marge": 40.00
    }
  ]
}
```

> La jointure se fait via `artfou1.preference = true` (fournisseur principal de l'article) → `fouident.code` pour le nom.

---

### CA par nomenclature (rayon)
```
GET /api/performance/ca/nomenclature?dateDebut=&dateFin=&site=&niveau=1
```

| Paramètre | Type | Description                          |
|-----------|------|--------------------------------------|
| `niveau`  | int  | Niveau de nomenclature (1 = racine)  |

**Réponse** :
```json
{
  "ca_par_nomenclature": [
    {
      "code_nomen": "RAY01",
      "libelle": "RAYON MAISON",
      "niveau": 1,
      "nb_articles": 420,
      "qte_vendue": 8400,
      "ca_ht": 95000.00,
      "ca_ttc": 109250.00,
      "marge": 23750.00
    }
  ]
}
```

---

### CA par gamme
```
GET /api/performance/ca/gamme?dateDebut=&dateFin=&site=
```

**Réponse** :
```json
{
  "ca_par_gamme": [
    {
      "gamme": "ETE2024",
      "libelle_gamme": "ÉTÉ 2024",
      "nb_articles": 85,
      "qte_vendue": 1700,
      "ca_ttc": 28900.00,
      "marge": 6450.00
    }
  ]
}
```

---

## Ranking (classement réseau)

### Recherche ranking
```
GET /api/ranking?gencod=&codein=&site=&foucentrale=&limit=
```

Retourne les classements réseau et magasin des articles. Les données proviennent des fichiers d'import centrale (TURBOCAR, AUXENCE, etc.).

| Paramètre     | Type   | Description                        | Défaut |
|---------------|--------|------------------------------------|--------|
| `gencod`      | string | Code EAN exact                     | —      |
| `codein`      | string | Code interne article               | —      |
| `site`        | string | Filtrer par site (partiel)         | tous   |
| `foucentrale` | string | Filtrer par fournisseur (partiel)  | tous   |
| `limit`       | int    | Nombre max (max 500)               | 50     |

**Réponse** :
```json
{
  "count": 3,
  "ranking": [
    {
      "gencod": "3700536100805",
      "site": "292",
      "libelle": "ETUI 10 CARTES FIDELITE COUSU FANTASIA",
      "foucentrale": "FFTURBOC",
      "nomfoucentrale": "TURBOCAR",
      "ranking_ca": 90,
      "ranking_qte": 39,
      "ranking_mag_ca": 0,
      "ranking_mag_qte": 0,
      "ranking_mag_marge": 0,
      "pv_calcule": 3.89,
      "pv_mag": 0,
      "pv_cen": null,
      "codefamille": "350402",
      "libellefamille": "EQUIPEMENT INTERIEUR ET EXTERIEUR",
      "date_maj": "2024-05-06T00:00:00.000Z",
      "date_calcul_mag": "1901-01-01T00:00:00.000Z",
      "art_no_id": "2920000106042",
      "codein": "129632"
    }
  ]
}
```

**Clés ranking** :
| Champ | Description |
|-------|-------------|
| `ranking_ca` | Classement réseau par chiffre d'affaires (1 = meilleur) |
| `ranking_qte` | Classement réseau par quantité vendue |
| `ranking_mag_ca` | Classement magasin par CA |
| `ranking_mag_qte` | Classement magasin par quantité |
| `ranking_mag_marge` | Classement magasin par marge |
| `pv_calcule` | Prix de vente calculé par la centrale |
| `pv_mag` | Prix de vente magasin |
| `pv_cen` | Prix de vente centrale |

> Le ranking est lié aux articles via le **GENCOD** (EAN). La jointure avec `art_gtin` permet de retrouver le `no_id` et le `codein`.

---

### Ranking d'un article
```
GET /api/ranking/article/:id
```

**Paramètre** : `id` = `no_id` de l'article

Retourne le ranking pour tous les sites où l'article est référencé.

```json
{
  "artNoId": "2920000106042",
  "ranking": [
    {
      "gencod": "3700536100805",
      "site": "292",
      "ranking_ca": 90,
      "ranking_qte": 39,
      "ranking_mag_ca": 0,
      "ranking_mag_qte": 0,
      "ranking_mag_marge": 0,
      "foucentrale": "FFTURBOC",
      "nomfoucentrale": "TURBOCAR",
      "pv_calcule": 3.89,
      "pv_mag": 0,
      "pv_cen": null,
      "codefamille": "350402",
      "libellefamille": "EQUIPEMENT INTERIEUR ET EXTERIEUR",
      "date_maj": "2024-05-06T00:00:00.000Z",
      "date_calcul_mag": "1901-01-01T00:00:00.000Z"
    }
  ]
}
```

> Le ranking est aussi inclus dans la fiche référentiel : `GET /api/articles/:id/referentiel` → champ `ranking`.

---

## Synchronisation

### État de la sync
```
GET /api/sync/status
```

**Réponse** :
```json
{
  "sync": [
    {
      "table_name": "articles",
      "last_sync": "2026-03-18T03:00:00.000Z",
      "rows_synced": 42062,
      "status": "ok",
      "error_msg": null
    }
  ]
}
```

**Tables synchronisées** :
| Table | Mode | Fréquence |
|-------|------|-----------|
| `articles` | Delta (SUIVIDATEMODIF) | Nuit |
| `article_infosup` | Delta | Nuit |
| `art_gtin` | Upsert complet | Nuit |
| `artfou1` | Delta | Nuit |
| `artfou2` | Upsert complet | Nuit |
| `fouadr1` | Refresh complet | Nuit |
| `cube_stock` | Refresh complet | Nuit |
| `cube_pa` | Refresh complet | Nuit |
| `cube_pv` | Refresh complet | Nuit |
| `mvtart` | Append par NO_ID (PK SQL Server) | Nuit |
| `mvtreg` | Append par date | Nuit |
| `nomenclature` | Refresh complet | Nuit |
| `gammes` | Refresh complet | Nuit |
| `art_gamme_saison` | Refresh complet | Nuit |
| `saisons` | Refresh complet | Nuit |
| `fouident` | Refresh complet | Nuit |
| `cdefou_vivant` | Refresh complet | Nuit |
| `cdefou_reception` | Delta | Nuit |
| `cdefou_receplig` | Delta | Nuit |
| `commande_fou` | Upsert (NO_ID) | Nuit |
| `cdefou_ligne` | Upsert (NO_ID) | Nuit |
| `commande_auto_qtepropo` | Refresh complet | Nuit |
| `plan_reappro` | Refresh complet | Nuit |
| `ranking` | Refresh complet | Nuit |
| `stat_dispoperm` | Upsert | Nuit |
| `phenix_quantite_conseille` | Refresh complet | Nuit |
| `statopca` | Upsert (site+date) | Nuit |

---

## Exemples d'intégration JavaScript

```javascript
const API_BASE = 'https://api.ffnancy.fr';

// Rechercher des articles
async function rechercherArticles(search, page = 1) {
  const params = new URLSearchParams({ search, page, limit: 50 });
  const res = await fetch(`${API_BASE}/api/articles?${params}`);
  return res.json();
}

// Fiche complète d'un article
async function getFicheArticle(id) {
  const res = await fetch(`${API_BASE}/api/articles/${id}/referentiel`);
  return res.json();
}

// CA du mois en cours par jour
async function getCaMoisEnCours() {
  const now = new Date();
  const debut = new Date(now.getFullYear(), now.getMonth(), 1).toISOString().slice(0, 10);
  const fin   = new Date(now.getFullYear(), now.getMonth() + 1, 0).toISOString().slice(0, 10);
  const params = new URLSearchParams({ dateDebut: debut, dateFin: fin, groupBy: 'jour' });
  const res = await fetch(`${API_BASE}/api/performance/ca?${params}`);
  return res.json();
}

// Top 10 articles par CA sur 30 jours
async function getTop10() {
  const fin   = new Date().toISOString().slice(0, 10);
  const debut = new Date(Date.now() - 30 * 86400000).toISOString().slice(0, 10);
  const params = new URLSearchParams({ dateDebut: debut, dateFin: fin, limit: 10, groupBy: 'ca' });
  const res = await fetch(`${API_BASE}/api/performance/hitparade?${params}`);
  return res.json();
}

// Stock d'un article
async function getStock(artNoId) {
  const res = await fetch(`${API_BASE}/api/stock/article/${artNoId}`);
  return res.json();
}

// Valorisation globale du stock
async function getValorisationStock() {
  const res = await fetch(`${API_BASE}/api/stock/valorisation`);
  return res.json();
}

// Commandes en cours d'un fournisseur
async function getCommandesFournisseur(codeFou) {
  const res = await fetch(`${API_BASE}/api/fournisseurs/${codeFou}/commandes`);
  return res.json();
}
```

---

## Codes d'erreur HTTP

| Code | Description |
|------|-------------|
| `200` | Succès |
| `404` | Ressource introuvable (article, commande…) |
| `500` | Erreur serveur (détail dans le champ `error`) |

**Format erreur** :
```json
{ "error": "Article introuvable" }
```

---

## Notes importantes

- Les données sont en lecture seule — aucun endpoint d'écriture
- Les données sont décalées d'au plus 24h (sync automatique à 6h45 heure Paris)
- Les champs dates sont au format ISO 8601 UTC
- La pagination commence à `page=1`
- Le champ `no_id` est l'identifiant interne SQL Server de l'article
- Les codes sites (`001`, `002`…) correspondent aux différents points de vente
