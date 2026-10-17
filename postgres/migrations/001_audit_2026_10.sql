-- ============================================================
-- Migration 001 — audit octobre 2026
-- Met à niveau une base existante vers le schéma de init.sql.
-- Idempotente : sans effet sur une base créée depuis le nouveau init.sql.
-- Exécutée automatiquement par le service sync (table schema_migrations),
-- AVANT init.sql (dont certains index dépendent des colonnes ajoutées ici).
-- ============================================================

-- -- articles : colonnes supplémentaires ---------------------
ALTER TABLE IF EXISTS articles
  ADD COLUMN IF NOT EXISTS unite_aff        TEXT,
  ADD COLUMN IF NOT EXISTS clas_code        TEXT,
  ADD COLUMN IF NOT EXISTS mod_code         TEXT,
  ADD COLUMN IF NOT EXISTS mar_no_id        BIGINT,
  ADD COLUMN IF NOT EXISTS qte_uc           NUMERIC(12,3),
  ADD COLUMN IF NOT EXISTS qte_valo         NUMERIC(12,3),
  ADD COLUMN IF NOT EXISTS poids_brut_uc    NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS poids_net_uc     NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS long_encomb      NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS large_encomb     NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS haut_encomb      NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS vente_coupe      TEXT,
  ADD COLUMN IF NOT EXISTS coef_achat_vente NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS artchamp1        TEXT,
  ADD COLUMN IF NOT EXISTS artchamp2        TEXT,
  ADD COLUMN IF NOT EXISTS artchamp3        TEXT,
  ADD COLUMN IF NOT EXISTS artchamp4        TEXT,
  ADD COLUMN IF NOT EXISTS artchamp5        TEXT;

-- -- mvtart : type de mouvement, caisse, heure du ticket... ----
ALTER TABLE IF EXISTS mvtart
  ADD COLUMN IF NOT EXISTS mvttypnoid     BIGINT,
  ADD COLUMN IF NOT EXISTS mvtgrpnoid     BIGINT,
  ADD COLUMN IF NOT EXISTS idptdist       BIGINT,
  ADD COLUMN IF NOT EXISTS tiersref       TEXT,
  ADD COLUMN IF NOT EXISTS tierstyp       TEXT,
  ADD COLUMN IF NOT EXISTS nomnoid        BIGINT,
  ADD COLUMN IF NOT EXISTS mntprmpmvt     NUMERIC(14,4),
  ADD COLUMN IF NOT EXISTS codetva        TEXT,
  ADD COLUMN IF NOT EXISTS tauxtva        NUMERIC(6,3),
  ADD COLUMN IF NOT EXISTS ticket_ts      TIMESTAMP,
  ADD COLUMN IF NOT EXISTS prix_unitaire  NUMERIC(12,4),
  ADD COLUMN IF NOT EXISTS champ3         TEXT,
  ADD COLUMN IF NOT EXISTS champ4         TEXT,
  ADD COLUMN IF NOT EXISTS gtin           TEXT,
  ADD COLUMN IF NOT EXISTS suividatemodif TIMESTAMP;
-- (les lignes existantes sont complétées automatiquement : la sync détecte
--  mvttypnoid IS NULL et recharge les 2 dernières années mois par mois)

-- -- mvtreg : clé primaire (corrige les doublons de la sync « force ») --
ALTER TABLE IF EXISTS mvtreg
  ADD COLUMN IF NOT EXISTS no_id   BIGINT,
  ADD COLUMN IF NOT EXISTS regnoid BIGINT,
  ADD COLUMN IF NOT EXISTS numlot  TEXT;
DO $$ BEGIN
  IF to_regclass('public.mvtreg') IS NOT NULL THEN
    -- Les anciennes lignes n'ont pas d'identifiant source et contiennent des
    -- doublons : on les supprime, la sync recharge 2 ans proprement.
    DELETE FROM mvtreg WHERE no_id IS NULL;
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                   WHERE conrelid = 'public.mvtreg'::regclass AND contype = 'p') THEN
      ALTER TABLE mvtreg ADD PRIMARY KEY (no_id);
    END IF;
  END IF;
END $$;

-- -- cdefou_reception : fournisseur, n° réception, BL, état --
ALTER TABLE IF EXISTS cdefou_reception
  ADD COLUMN IF NOT EXISTS fouident_code          TEXT,
  ADD COLUMN IF NOT EXISTS recnum                 TEXT,
  ADD COLUMN IF NOT EXISTS sit_cod_desti          TEXT,
  ADD COLUMN IF NOT EXISTS cdefou_livraison_no_id BIGINT,
  ADD COLUMN IF NOT EXISTS com_no_id              BIGINT,
  ADD COLUMN IF NOT EXISTS recetat                TEXT,
  ADD COLUMN IF NOT EXISTS recdate                TIMESTAMP,
  ADD COLUMN IF NOT EXISTS recbl                  TEXT,
  ADD COLUMN IF NOT EXISTS recbldate              TIMESTAMP,
  ADD COLUMN IF NOT EXISTS recblinfo              TEXT;

-- -- Forcer un rechargement complet des tables en delta enrichies --
DO $$ BEGIN
  IF to_regclass('public.sync_log') IS NOT NULL THEN
    DELETE FROM sync_log WHERE table_name IN ('articles', 'article_infosup', 'cdefou_reception');
  END IF;
END $$;
