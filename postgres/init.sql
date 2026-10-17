-- ============================================================
-- Schéma PostgreSQL — foirfouille API
-- Tables sélectionnées depuis SQL Server (copie nightly via sync)
-- ============================================================

-- Contrôle de synchronisation
CREATE TABLE IF NOT EXISTS sync_log (
  table_name   TEXT PRIMARY KEY,
  last_sync    TIMESTAMP,
  rows_synced  INTEGER,
  status       TEXT,
  error_msg    TEXT
);

-- Index photos lafoirfouille.fr (construit depuis les sitemaps)
CREATE TABLE IF NOT EXISTS ff_photo_index (
  photo_code   TEXT PRIMARY KEY,
  product_url  TEXT NOT NULL,
  indexed_at   TIMESTAMP DEFAULT NOW()
);

-- ============================================================
-- Référentiel articles
-- ============================================================

CREATE TABLE IF NOT EXISTS articles (
  no_id             BIGINT PRIMARY KEY,
  codein            TEXT,
  libelle1          TEXT,
  libelle2          TEXT,
  lib_ticket        TEXT,
  tax_code          TEXT,
  ach_code          TEXT,
  utilisable        TEXT,
  actif             TEXT,
  suspendu          TEXT,
  suividatecreation TIMESTAMP,
  suividatemodif    TIMESTAMP,
  nom_no_id         BIGINT,
  artcentrale       TEXT,
  unite_aff         TEXT,
  clas_code         TEXT,
  mod_code          TEXT,
  mar_no_id         BIGINT,
  qte_uc            NUMERIC(12,3),
  qte_valo          NUMERIC(12,3),
  poids_brut_uc     NUMERIC(12,4),
  poids_net_uc      NUMERIC(12,4),
  long_encomb       NUMERIC(12,4),
  large_encomb      NUMERIC(12,4),
  haut_encomb       NUMERIC(12,4),
  vente_coupe       TEXT,
  coef_achat_vente  NUMERIC(12,4),
  artchamp1         TEXT,
  artchamp2         TEXT,
  artchamp3         TEXT,
  artchamp4         TEXT,
  artchamp5         TEXT
);
CREATE INDEX IF NOT EXISTS idx_articles_codein     ON articles (codein);
CREATE INDEX IF NOT EXISTS idx_articles_libelle1   ON articles (libelle1);
CREATE INDEX IF NOT EXISTS idx_articles_nom_no_id  ON articles (nom_no_id);

CREATE TABLE IF NOT EXISTS article_infosup (
  artnoid           BIGINT PRIMARY KEY,
  prix_vente_mini   NUMERIC(12,4),
  prix_vente_maxi   NUMERIC(12,4),
  eco_ht            NUMERIC(12,4),
  eco_ttc           NUMERIC(12,4),
  on_web            TEXT,
  interdit_remise   TEXT,
  nomphoto          TEXT,
  datedebvente      TIMESTAMP,
  datefinvente      TIMESTAMP,
  datevaliditeachat TIMESTAMP,
  commentaire       TEXT,
  pv_conseille      NUMERIC(12,4),
  fraislogistic     NUMERIC(12,4),
  transit           NUMERIC(12,4),
  distribution      NUMERIC(12,4)
);

CREATE TABLE IF NOT EXISTS art_gtin (
  idarticle   BIGINT,
  gtin        TEXT,
  preferentiel SMALLINT,
  PRIMARY KEY (idarticle, gtin)
);
CREATE INDEX IF NOT EXISTS idx_art_gtin_article ON art_gtin (idarticle);
CREATE INDEX IF NOT EXISTS idx_art_gtin_gtin    ON art_gtin (gtin);

-- ============================================================
-- Nomenclature & Gammes
-- ============================================================

CREATE TABLE IF NOT EXISTS nomenclature (
  no_id       BIGINT PRIMARY KEY,
  code        TEXT,
  libelle     TEXT,
  niveau      INTEGER,
  chemin_pere TEXT
);
CREATE INDEX IF NOT EXISTS idx_nomenclature_niveau ON nomenclature (niveau);

CREATE TABLE IF NOT EXISTS gammes (
  no_id   BIGINT PRIMARY KEY,
  code    TEXT,
  libelle TEXT
);

CREATE TABLE IF NOT EXISTS saisons (
  no_id   BIGINT PRIMARY KEY,
  code    TEXT,
  libelle TEXT
);

CREATE TABLE IF NOT EXISTS art_gamme_saison (
  artnoid  BIGINT,
  idgamme  BIGINT,
  idsaison BIGINT,
  PRIMARY KEY (artnoid, idgamme, idsaison)
);
CREATE INDEX IF NOT EXISTS idx_ags_artnoid ON art_gamme_saison (artnoid);

-- ============================================================
-- Fournisseurs
-- ============================================================

CREATE TABLE IF NOT EXISTS artfou1 (
  no_id             BIGINT PRIMARY KEY,
  art_no_id         BIGINT,
  code              TEXT,
  reference         TEXT,
  ean13             TEXT,
  itf               TEXT,
  qteua             NUMERIC(10,3),
  pcb               NUMERIC(10,3),
  spcb              NUMERIC(10,3),
  delai             INTEGER,
  securite          NUMERIC(10,3),
  preference        INTEGER,
  suspendu          TEXT,
  suividatecreation TIMESTAMP,
  suividatemodif    TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_artfou1_art_no_id ON artfou1 (art_no_id);
CREATE INDEX IF NOT EXISTS idx_artfou1_code      ON artfou1 (code);
CREATE INDEX IF NOT EXISTS idx_artfou1_art_pref  ON artfou1 (art_no_id, preference);

CREATE TABLE IF NOT EXISTS artfou2 (
  idartfou1         BIGINT PRIMARY KEY,
  prixachat         NUMERIC(12,4),
  remise_promotion  NUMERIC(10,4)
);

CREATE TABLE IF NOT EXISTS fouadr1 (
  code         TEXT,
  sit_code     TEXT,
  raisonsociale TEXT,
  adrligne1    TEXT,
  telephone    TEXT,
  email        TEXT,
  PRIMARY KEY (code, sit_code)
);

CREATE TABLE IF NOT EXISTS fouident (
  code              TEXT PRIMARY KEY,
  fon_code          TEXT,
  cnuf              TEXT,
  nom               TEXT,
  actif             BOOLEAN,
  suspendu          BOOLEAN,
  permanen          TEXT,
  foucentrale       TEXT,
  suividatecreation TIMESTAMP,
  suividatemodif    TIMESTAMP
);

-- ============================================================
-- Cubes de stock et prix
-- ============================================================

CREATE TABLE IF NOT EXISTS cube_stock (
  artnoid                  BIGINT,
  site                     TEXT,
  qte                      NUMERIC(12,3),
  prmp                     NUMERIC(12,4),
  valstock                 NUMERIC(14,4),
  pv                       NUMERIC(12,4),
  stockdispo               NUMERIC(12,3),
  stockmort                NUMERIC(12,3),
  stockcolis               NUMERIC(12,3),
  dernierevente            TIMESTAMP,
  dernierereception        TIMESTAMP,
  premierevente            TIMESTAMP,
  nbjoursdernierMouvement  INTEGER,
  nbjoursdernierevente     INTEGER,
  nbjoursdernierrereception INTEGER,
  interditachat            TEXT,
  codefou                  TEXT,
  PRIMARY KEY (artnoid, site)
);
CREATE INDEX IF NOT EXISTS idx_cube_stock_site ON cube_stock (site);

CREATE TABLE IF NOT EXISTS cube_pa (
  artnoid BIGINT PRIMARY KEY,
  pa      NUMERIC(12,4)
);

CREATE TABLE IF NOT EXISTS cube_pv (
  artnoid BIGINT,
  site    TEXT,
  pv      NUMERIC(12,4),
  PRIMARY KEY (artnoid, site)
);
CREATE INDEX IF NOT EXISTS idx_cube_pv_site ON cube_pv (site);

-- ============================================================
-- Mouvements
-- ============================================================

CREATE TABLE IF NOT EXISTS mvtart (
  no_id      BIGINT PRIMARY KEY,
  artnoid    BIGINT,
  datmvt     TIMESTAMP,
  site       TEXT,
  libmvt     TEXT,
  genremvt   INTEGER,
  qtemvt     NUMERIC(12,3),
  valmvt     NUMERIC(14,4),
  mntmvtht   NUMERIC(14,4),
  mntmvtttc  NUMERIC(14,4),
  margemvt   NUMERIC(14,4),
  qtestock   NUMERIC(12,3),
  prmp       NUMERIC(12,4),
  valstock   NUMERIC(14,4),
  codefou    TEXT,
  -- Colonnes ajoutées oct. 2026 (audit)
  mvttypnoid    BIGINT,          -- type de mouvement -> mvttype.no_id (11 = vente client)
  mvtgrpnoid    BIGINT,          -- groupe -> mvtgrp.no_id
  idptdist      BIGINT,          -- point de distribution / caisse
  tiersref      TEXT,            -- référence tiers (carte client...)
  tierstyp      TEXT,            -- CLI, FOU...
  nomnoid       BIGINT,          -- nomenclature
  mntprmpmvt    NUMERIC(14,4),
  codetva       TEXT,
  tauxtva       NUMERIC(6,3),
  ticket_ts     TIMESTAMP,       -- horodatage du ticket (Champ1) : heure de vente
  prix_unitaire NUMERIC(12,4),   -- Champ2 : prix unitaire de vente
  champ3        TEXT,
  champ4        TEXT,            -- ex. « ART LOT: ... »
  gtin          TEXT,            -- Champ5 : code-barres scanné
  suividatemodif TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_mvtart_artnoid   ON mvtart (artnoid);
CREATE INDEX IF NOT EXISTS idx_mvtart_datmvt    ON mvtart (datmvt);
CREATE INDEX IF NOT EXISTS idx_mvtart_site      ON mvtart (site);
CREATE INDEX IF NOT EXISTS idx_mvtart_genremvt  ON mvtart (genremvt);
CREATE INDEX IF NOT EXISTS idx_mvtart_art_date  ON mvtart (artnoid, datmvt DESC);
CREATE INDEX IF NOT EXISTS idx_mvtart_genre_date ON mvtart (genremvt, datmvt);
CREATE INDEX IF NOT EXISTS idx_mvtart_art_site_genre_date ON mvtart (artnoid, site, genremvt, datmvt DESC);
CREATE INDEX IF NOT EXISTS idx_mvtart_type_date ON mvtart (mvttypnoid, datmvt);
CREATE INDEX IF NOT EXISTS idx_mvtart_ticket    ON mvtart (libmvt) WHERE mvttypnoid IN (11, 12);
CREATE INDEX IF NOT EXISTS idx_mvtart_ticket_ts ON mvtart (ticket_ts);

CREATE TABLE IF NOT EXISTS mvtreg (
  no_id             BIGINT PRIMARY KEY,
  regnoid           BIGINT,          -- mode de règlement -> mode_reglement.no_id
  numlot            TEXT,
  datmvt            TIMESTAMP,
  codtick           TEXT,
  codcartecli       TEXT,
  coddev            TEXT,
  mntreg            NUMERIC(14,4),
  mntregdev         NUMERIC(14,4),
  clientnom         TEXT,
  echeance          TIMESTAMP,
  reference         TEXT,
  typereg           TEXT,
  suividatecreation TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_mvtreg_datmvt ON mvtreg (datmvt);
CREATE INDEX IF NOT EXISTS idx_mvtreg_codtick ON mvtreg (codtick);

-- ============================================================
-- Ranking réseau / magasin
-- ============================================================

CREATE TABLE IF NOT EXISTS ranking (
  gencod            TEXT,
  site              TEXT,
  libelle           TEXT,
  foucentrale       TEXT,
  nomfoucentrale    TEXT,
  ranking_ca        NUMERIC(10,2),
  ranking_qte       NUMERIC(10,2),
  ranking_mag_ca    NUMERIC(10,2),
  ranking_mag_qte   NUMERIC(10,2),
  ranking_mag_marge NUMERIC(10,2),
  pv_calcule        NUMERIC(12,4),
  pv_mag            NUMERIC(12,4),
  pv_cen            NUMERIC(12,4),
  codefamille       TEXT,
  libellefamille    TEXT,
  fichier           TEXT,
  date_maj          TIMESTAMP,
  date_integration  TIMESTAMP,
  date_calcul_mag   TIMESTAMP,
  PRIMARY KEY (gencod, site)
);
CREATE INDEX IF NOT EXISTS idx_ranking_gencod ON ranking (gencod);
CREATE INDEX IF NOT EXISTS idx_ranking_site   ON ranking (site);
CREATE INDEX IF NOT EXISTS idx_ranking_ca     ON ranking (ranking_ca);

-- ============================================================
-- Commandes fournisseurs
-- ============================================================

CREATE TABLE IF NOT EXISTS cdefou_vivant (
  cdefou_ligne_com_no_id  BIGINT,
  artfou1_no_id           BIGINT,
  articles_codein         TEXT,
  articles_libelle1       TEXT,
  articles_libelle2       TEXT,
  artfou1_reference       TEXT,
  artfou1_ean13           TEXT,
  artfou1_itf             TEXT,
  cdefou_ligne_qtecde     NUMERIC(12,3),
  cdefou_ligne_prixbrut   NUMERIC(12,4),
  cdefou_ligne_remise     NUMERIC(10,4),
  cdefou_ligne_remise2    NUMERIC(10,4),
  cdefou_ligne_remise3    NUMERIC(10,4),
  cdefou_ligne_prixnet    NUMERIC(12,4),
  cdefou_ligne_montant    NUMERIC(14,4),
  cdefou_ligne_prixvente  NUMERIC(12,4),
  cdefou_ligne_gratuit    NUMERIC(12,3),
  cdefou_ligne_qteacc     NUMERIC(12,3),
  cdefou_ligne_qteann     NUMERIC(12,3),
  cdefou_ligne_qteatt     NUMERIC(12,3),
  cdefou_ligne_qterel     NUMERIC(12,3),
  cdefou_ligne_cdeligtard TIMESTAMP,
  cdefou_ligne_cdeligtot  TIMESTAMP,
  cotisation_logistique   NUMERIC(14,4),
  fraislogistic           NUMERIC(12,4),
  transit                 NUMERIC(12,4),
  distribution            NUMERIC(12,4),
  taxe                    NUMERIC(12,4),
  commentaire             TEXT,
  suividatecreation       TIMESTAMP,
  PRIMARY KEY (cdefou_ligne_com_no_id, artfou1_no_id)
);
CREATE INDEX IF NOT EXISTS idx_cdefou_vivant_artfou1 ON cdefou_vivant (artfou1_no_id);
CREATE INDEX IF NOT EXISTS idx_cdefou_vivant_date    ON cdefou_vivant (suividatecreation);

CREATE TABLE IF NOT EXISTS cdefou_reception (
  no_id                  BIGINT PRIMARY KEY,
  fouident_code          TEXT,
  recnum                 TEXT,
  sit_cod_desti          TEXT,
  cdefou_livraison_no_id BIGINT,
  com_no_id              BIGINT,
  recetat                TEXT,
  recdate                TIMESTAMP,
  recbl                  TEXT,
  recbldate              TIMESTAMP,
  recblinfo              TEXT,
  suividatecreation      TIMESTAMP,
  suividatemodif         TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cdefou_reception_fou  ON cdefou_reception (fouident_code);
CREATE INDEX IF NOT EXISTS idx_cdefou_reception_date ON cdefou_reception (recdate);

CREATE TABLE IF NOT EXISTS cdefou_receplig (
  no_id                    BIGINT PRIMARY KEY,
  cdefou_reception_no_id   BIGINT,
  artfou1_no_id            BIGINT,
  qtebl                    NUMERIC(12,3),
  qterec                   NUMERIC(12,3),
  qteacc                   NUMERIC(12,3),
  qteref                   NUMERIC(12,3),
  qteaff                   NUMERIC(12,3),
  pribrut                  NUMERIC(12,4),
  remise                   NUMERIC(10,4),
  prirec                   NUMERIC(12,4),
  mntrec                   NUMERIC(14,4),
  motifrefus               TEXT,
  recpb                    SMALLINT,
  recpbok                  SMALLINT,
  suividatecreation        TIMESTAMP,
  suividatemodif           TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_receplig_artfou1 ON cdefou_receplig (artfou1_no_id);
CREATE INDEX IF NOT EXISTS idx_receplig_date    ON cdefou_receplig (suividatecreation);
CREATE INDEX IF NOT EXISTS idx_receplig_reception ON cdefou_receplig (cdefou_reception_no_id);

-- ============================================================
-- CA journalier + trafic (StatOpCAJour SQL Server)
-- ============================================================

CREATE TABLE IF NOT EXISTS statopca (
  site     TEXT        NOT NULL,
  datmvt   DATE        NOT NULL,
  mnt      NUMERIC(14,4),
  nbticket INTEGER,
  PRIMARY KEY (site, datmvt)
);
CREATE INDEX IF NOT EXISTS idx_statopca_datmvt ON statopca (datmvt);

-- ============================================================
-- Gamme conseillée par site (STAT_DISPOPERM SQL Server)
-- ============================================================

CREATE TABLE IF NOT EXISTS stat_dispoperm (
  codesite   TEXT        NOT NULL,
  code_art   TEXT        NOT NULL,
  recommand  TEXT,
  statut     TEXT,
  qtemoyer   NUMERIC(10,3),
  qtemoyes   NUMERIC(10,3),
  camoyes    NUMERIC(12,4),
  nbreclient INTEGER,
  PRIMARY KEY (codesite, code_art)
);
CREATE INDEX IF NOT EXISTS idx_stat_dispoperm_code_art ON stat_dispoperm (code_art);

-- ============================================================
-- Journal de commande conseillé (PHENIX_QUANTITE_CONSEILLE)
-- ============================================================

CREATE TABLE IF NOT EXISTS phenix_quantite_conseille (
  ident              BIGINT PRIMARY KEY,
  site               TEXT,
  codein             TEXT,
  libelle_article    TEXT,
  code1              TEXT,
  libelle1           TEXT,
  idartfou1          BIGINT,
  art_no_id          BIGINT,
  colisage           NUMERIC(10,3),
  moyenne_vente_jour NUMERIC(12,6),
  nb_jour_vte        INTEGER,
  besoin_calcule     NUMERIC(12,4),
  stock_mini         NUMERIC(12,3),
  stock_maxi         NUMERIC(12,3),
  qte_non_recue      NUMERIC(12,3),
  qte_stock          NUMERIC(12,3),
  besoin_pris        NUMERIC(12,4),
  a_commander        NUMERIC(12,3),
  montant            NUMERIC(14,4)
);
CREATE INDEX IF NOT EXISTS idx_phenix_cde_site     ON phenix_quantite_conseille (site);
CREATE INDEX IF NOT EXISTS idx_phenix_cde_codein   ON phenix_quantite_conseille (codein);
CREATE INDEX IF NOT EXISTS idx_phenix_cde_artfou1  ON phenix_quantite_conseille (idartfou1);

-- ============================================================
-- Commandes fournisseurs historique + réapprovisionnement
-- ============================================================

CREATE TABLE IF NOT EXISTS commande_fou (
  no_id             BIGINT PRIMARY KEY,
  cdenum            TEXT,
  sit_cod_emet      TEXT,
  sit_cod_desti     TEXT,
  fouident_code     TEXT,
  foutarif_no_id    BIGINT,
  port_code         TEXT,
  cdedate           TIMESTAMP,
  cdeetat           TEXT,
  cdetot            NUMERIC(14,4),
  cdetard           TIMESTAMP,
  cdeportmontant    NUMERIC(12,4),
  cdeannulation     TEXT,
  suividatecreation TIMESTAMP,
  suividatemodif    TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_commande_fou_fouident ON commande_fou (fouident_code);
CREATE INDEX IF NOT EXISTS idx_commande_fou_date     ON commande_fou (cdedate);
CREATE INDEX IF NOT EXISTS idx_commande_fou_etat     ON commande_fou (cdeetat);

CREATE TABLE IF NOT EXISTS cdefou_ligne (
  no_id              BIGINT PRIMARY KEY,
  artfou1_no_id      BIGINT,
  commande_fou_no_id BIGINT,
  cdelig             TEXT,
  cdeligtard         TIMESTAMP,
  cdeligtot          TIMESTAMP,
  prixbrut           NUMERIC(12,4),
  remise             NUMERIC(10,4),
  prixnet            NUMERIC(12,4),
  qtecde             NUMERIC(12,3),
  gratuit            NUMERIC(12,3),
  montant            NUMERIC(14,4),
  qteacc             NUMERIC(12,3),
  qteann             NUMERIC(12,3),
  qteatt             NUMERIC(12,3),
  qterel             NUMERIC(12,3),
  colisage           TEXT,
  suividatecreation  TIMESTAMP,
  suividatemodif     TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_cdefou_ligne_artfou1  ON cdefou_ligne (artfou1_no_id);
CREATE INDEX IF NOT EXISTS idx_cdefou_ligne_commande ON cdefou_ligne (commande_fou_no_id);

CREATE TABLE IF NOT EXISTS commande_auto_qtepropo (
  fou_code          TEXT,
  art_no_id         BIGINT,
  codesite          TEXT,
  qtepropo          NUMERIC(12,3),
  parametre_numero  INTEGER,
  suividatecreation TIMESTAMP,
  suividatemodif    TIMESTAMP,
  PRIMARY KEY (fou_code, art_no_id, codesite)
);
CREATE INDEX IF NOT EXISTS idx_caqp_art_no_id ON commande_auto_qtepropo (art_no_id);
CREATE INDEX IF NOT EXISTS idx_caqp_codesite  ON commande_auto_qtepropo (codesite);

CREATE TABLE IF NOT EXISTS foucad (
  foucode           TEXT PRIMARY KEY,
  franco            NUMERIC(12,2),
  actif             BOOLEAN,
  duree             INTEGER,
  unite             INTEGER,
  dateref           TIMESTAMP,
  datecom           TIMESTAMP,
  montantcdeauto    NUMERIC(12,2),
  datecdeauto       TIMESTAMP,
  suividatecreation TIMESTAMP,
  suividatemodif    TIMESTAMP
);

-- Conditions de port fournisseur (source officielle du franco)
CREATE TABLE IF NOT EXISTS fouport (
  no_id             NUMERIC(20,0) PRIMARY KEY,
  code              TEXT,
  sit_code          TEXT,
  suividatecreation TIMESTAMP,
  suividatemodif    TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fouport_code ON fouport (code);

CREATE TABLE IF NOT EXISTS fouport2 (
  fou_no_id         NUMERIC(20,0),
  no_id             NUMERIC(20,0) PRIMARY KEY,
  seuil             NUMERIC(12,2),
  mode              SMALLINT,
  valeur            NUMERIC(12,2),
  suividatecreation TIMESTAMP,
  suividatemodif    TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_fouport2_fou_no_id ON fouport2 (fou_no_id);

-- ============================================================
-- Publicités (FBENTPUB + ECOULEMENT + ECOULEMENT_DETAIL SQL Server)
-- ============================================================

CREATE TABLE IF NOT EXISTS pub_entetes (
  ent_npub   TEXT PRIMARY KEY,
  ent_datdeb TIMESTAMP,
  ent_datfin TIMESTAMP,
  ent_titre  TEXT
);

CREATE TABLE IF NOT EXISTS pub_ecoulement (
  site                 TEXT NOT NULL,
  tcr_code             TEXT NOT NULL,
  tcr_libelle          TEXT,
  tcr_nature           SMALLINT,
  tcr_type             SMALLINT,
  tcrd_datedeb         TIMESTAMP,
  tcrd_datefin         TIMESTAMP,
  datecalcul           TIMESTAMP,
  stock_datedebut      NUMERIC(14,3),
  ca_total_periode_pub NUMERIC(14,4),
  ca_pub_periode_pub   NUMERIC(14,4),
  pourc_capub_catotal  NUMERIC(8,4),
  qte_vendue_pub       NUMERIC(14,3),
  client_total_periode NUMERIC(14,3),
  client_pub_periode   NUMERIC(14,3),
  stock_datefin        NUMERIC(14,3),
  ca_pub_30_jours      NUMERIC(14,4),
  stock_30_jours       NUMERIC(14,3),
  ca_pub_60_jours      NUMERIC(14,4),
  stock_60_jours       NUMERIC(14,3),
  ca_pub_90_jours      NUMERIC(14,4),
  stock_90_jours       NUMERIC(14,3),
  ca_pub_180_jours     NUMERIC(14,4),
  stock_180_jours      NUMERIC(14,3),
  ca_depuis_finpub     NUMERIC(14,4),
  taux_sortie          NUMERIC(8,4),
  marge                NUMERIC(14,4),
  taux_marge           NUMERIC(8,4),
  PRIMARY KEY (site, tcr_code)
);
CREATE INDEX IF NOT EXISTS idx_pub_ecoulement_tcr_code   ON pub_ecoulement (tcr_code);
CREATE INDEX IF NOT EXISTS idx_pub_ecoulement_tcrd_datedeb ON pub_ecoulement (tcrd_datedeb);

CREATE TABLE IF NOT EXISTS pub_ecoulement_detail (
  site                 TEXT NOT NULL,
  tcr_code             TEXT NOT NULL,
  artnoid              BIGINT NOT NULL,
  tcr_libelle          TEXT,
  tcrd_datedeb         TIMESTAMP,
  tcrd_datefin         TIMESTAMP,
  datecalcul           TIMESTAMP,
  ca_pub_periode_pub   NUMERIC(14,4),
  qte_vendue_pub       NUMERIC(14,3),
  stock_datedebut      NUMERIC(14,3),
  stock_datefin        NUMERIC(14,3),
  ca_pub_30_jours      NUMERIC(14,4),
  ca_pub_60_jours      NUMERIC(14,4),
  ca_pub_90_jours      NUMERIC(14,4),
  ca_pub_180_jours     NUMERIC(14,4),
  ca_depuis_finpub     NUMERIC(14,4),
  taux_sortie          NUMERIC(8,4),
  marge                NUMERIC(14,4),
  taux_marge           NUMERIC(8,4),
  codein               TEXT,
  libelle              TEXT,
  prmp                 NUMERIC(12,4),
  pa                   NUMERIC(12,4),
  pv                   NUMERIC(12,4),
  prixpub              TEXT,
  PRIMARY KEY (site, tcr_code, artnoid)
);
CREATE INDEX IF NOT EXISTS idx_pub_detail_tcr_code ON pub_ecoulement_detail (tcr_code);
CREATE INDEX IF NOT EXISTS idx_pub_detail_artnoid   ON pub_ecoulement_detail (artnoid);
CREATE INDEX IF NOT EXISTS idx_pub_detail_codein    ON pub_ecoulement_detail (codein);

CREATE TABLE IF NOT EXISTS plan_reappro (
  id                   INTEGER PRIMARY KEY,
  magasin              TEXT,
  codein               TEXT,
  bu_no_id             BIGINT,
  stockmag             NUMERIC(12,3),
  encours_palette_mag  NUMERIC(12,3),
  attente_prepa_mag    NUMERIC(12,3),
  colis_rea            NUMERIC(12,3),
  colis_ajout_web      NUMERIC(12,3),
  stock_dispo          NUMERIC(12,3),
  attente_prepa_depot  NUMERIC(12,3),
  stock_dispo_calcul   NUMERIC(12,3),
  colisage             NUMERIC(10,3),
  bloque               TEXT,
  date_besoin          TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_plan_reappro_magasin ON plan_reappro (magasin);
CREATE INDEX IF NOT EXISTS idx_plan_reappro_codein  ON plan_reappro (codein);

-- ############################################################
-- AJOUTS OCTOBRE 2026 (audit) — nouvelles sources SQL Server
-- ############################################################

-- Suivi des migrations appliquées par le service sync
CREATE TABLE IF NOT EXISTS schema_migrations (
  name       TEXT PRIMARY KEY,
  applied_at TIMESTAMP DEFAULT NOW()
);

-- Recherche LIKE '%x%' rapide : extension pg_trgm (ignorée si indisponible)
DO $$ BEGIN
  CREATE EXTENSION IF NOT EXISTS pg_trgm;
  CREATE INDEX IF NOT EXISTS idx_articles_libelle1_trgm ON articles USING gin (libelle1 gin_trgm_ops);
  CREATE INDEX IF NOT EXISTS idx_articles_libelle2_trgm ON articles USING gin (libelle2 gin_trgm_ops);
  CREATE INDEX IF NOT EXISTS idx_articles_codein_trgm   ON articles USING gin (codein gin_trgm_ops);
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'pg_trgm indisponible : %', SQLERRM;
END $$;

-- ============================================================
-- Référentiels
-- ============================================================

CREATE TABLE IF NOT EXISTS mvttype (            -- MvtType
  no_id       BIGINT PRIMARY KEY,
  mvtgrpnoid  BIGINT,
  codemvt     TEXT,
  libellemvt  TEXT,
  sensmvt     SMALLINT,
  genremvt    SMALLINT,
  cumul       SMALLINT
);

CREATE TABLE IF NOT EXISTS mvtgrp (             -- MvtGrp
  no_id          BIGINT PRIMARY KEY,
  codegrpmvt     TEXT,
  libellegrpmvt  TEXT
);

CREATE TABLE IF NOT EXISTS mode_reglement (     -- ModeReglement
  no_id   BIGINT PRIMARY KEY,
  codreg  TEXT,
  libreg  TEXT,
  codcai  TEXT,
  compta  BOOLEAN
);

CREATE TABLE IF NOT EXISTS sites (              -- SITE
  code      TEXT PRIMARY KEY,
  sit_code  TEXT,
  nom       TEXT,
  niveau    SMALLINT,
  est_base  BOOLEAN,
  is_depot  BOOLEAN
);

CREATE TABLE IF NOT EXISTS tarif_client (       -- TARIF_CLIENT
  no_id        BIGINT PRIMARY KEY,
  devise_code  TEXT,
  tc_code      TEXT,
  tc_nom       TEXT
);

CREATE TABLE IF NOT EXISTS etat_commande (      -- ETAT_COMMANDE
  no_id    BIGINT PRIMARY KEY,
  etat     TEXT,
  libelle  TEXT,
  couleur  TEXT
);

CREATE TABLE IF NOT EXISTS motifs_refus (       -- MOTIFS_REFUS
  no_id    BIGINT PRIMARY KEY,
  code     TEXT,
  libelle  TEXT
);

-- ============================================================
-- Ventes : remises caisse, démarque, fidélité
-- ============================================================

CREATE TABLE IF NOT EXISTS mvtart_motif (       -- MvtArt_Motif (1 ligne par ligne de vente)
  mvtart_no_id  BIGINT PRIMARY KEY,
  datmvt        TIMESTAMP,       -- dénormalisé depuis MvtArt (fenêtre de sync)
  prix_orig     NUMERIC(12,4),
  motif         TEXT,            -- PUB, OPLIQ..., TARIF_CLIENT...
  motif_valeur  TEXT,
  champ1        TEXT,
  champ2        TEXT,
  champ3        TEXT,
  champ4        TEXT,
  champ5        TEXT,
  champ6        TEXT
);
CREATE INDEX IF NOT EXISTS idx_mvtart_motif_datmvt ON mvtart_motif (datmvt);
CREATE INDEX IF NOT EXISTS idx_mvtart_motif_motif  ON mvtart_motif (motif, motif_valeur);

CREATE TABLE IF NOT EXISTS demarques (          -- Demarques (historique depuis 2004)
  no_id       BIGINT PRIMARY KEY,
  artnoid     BIGINT,
  site        TEXT,
  datmvt      TIMESTAMP,
  mvttypnoid  BIGINT,
  qtemvt      NUMERIC(12,3),
  valmvt      NUMERIC(14,4),
  motifid     BIGINT,
  libmvt      TEXT,
  gtin        TEXT
);
CREATE INDEX IF NOT EXISTS idx_demarques_datmvt  ON demarques (datmvt);
CREATE INDEX IF NOT EXISTS idx_demarques_artnoid ON demarques (artnoid);

-- Agrégat quotidien de MVTFIDELITE (aucune donnée nominative)
CREATE TABLE IF NOT EXISTS fidelite_jour (
  site          TEXT NOT NULL,
  date          DATE NOT NULL,
  nb_mouvements INTEGER,
  nb_tickets    INTEGER,
  nb_cartes     INTEGER,
  ca_achat      NUMERIC(14,4),
  points        INTEGER,
  PRIMARY KEY (site, date)
);

-- ============================================================
-- Pilotage : budgets / objectifs (BUDGET_PILOTAGE*)
-- ============================================================

CREATE TABLE IF NOT EXISTS budget_pilotage (
  site                   TEXT   NOT NULL,
  datefiscale            DATE   NOT NULL,
  nomennoid_n1           BIGINT NOT NULL,
  nomennoid_n2           BIGINT NOT NULL,   -- 0 = total du niveau 1
  datecalcul             TIMESTAMP,
  ca_ht_an2              NUMERIC(16,4),
  ca_an2                 NUMERIC(16,4),
  ca_ht_an1              NUMERIC(16,4),
  ca_an1                 NUMERIC(16,4),
  ca_coeff               NUMERIC(14,6),
  ca_ht_objectif         NUMERIC(16,4),
  ca_objectif            NUMERIC(16,4),
  ca_ht_an               NUMERIC(16,4),
  ca_an                  NUMERIC(16,4),
  stk_an1                NUMERIC(16,4),
  stk_coeff              NUMERIC(14,6),
  stk_objectif           NUMERIC(16,4),
  stk_an                 NUMERIC(16,4),
  marge_valeur_an2       NUMERIC(16,4),
  marge_taux_an2         NUMERIC(12,4),
  marge_valeur_an1       NUMERIC(16,4),
  marge_taux_an1         NUMERIC(12,4),
  marge_taux             NUMERIC(12,4),
  marge_valeur_an        NUMERIC(16,4),
  marge_taux_an          NUMERIC(12,4),
  demarque_connue_an2    NUMERIC(16,4),
  demarque_inconnue_an2  NUMERIC(16,4),
  demarque_connue_an1    NUMERIC(16,4),
  demarque_inconnue_an1  NUMERIC(16,4),
  demarque_connue        NUMERIC(16,4),
  demarque_taux          NUMERIC(12,4),
  demarque_taux_connue   NUMERIC(12,4),
  lineaire_unite         NUMERIC(14,4),
  PRIMARY KEY (site, datefiscale, nomennoid_n1, nomennoid_n2)
);

-- Les 12 colonnes mensuelles (CA_HT_MOIS1..12, STK_MOIS..., MARGE_...) en format long
CREATE TABLE IF NOT EXISTS budget_pilotage_mois (
  site                 TEXT     NOT NULL,
  datefiscale          DATE     NOT NULL,
  nomennoid_n1         BIGINT   NOT NULL,
  nomennoid_n2         BIGINT   NOT NULL,
  mois                 SMALLINT NOT NULL,   -- 1..12 (mois de l'exercice)
  ca_ht                NUMERIC(16,4),
  ca_ttc               NUMERIC(16,4),
  ca_ht_exercice       NUMERIC(16,4),
  ca_ttc_exercice      NUMERIC(16,4),
  stk                  NUMERIC(16,4),
  stk_exercice         NUMERIC(16,4),
  marge_exercice       NUMERIC(16,4),
  taux_marge_exercice  NUMERIC(12,4),
  marge_n1             NUMERIC(16,4),
  taux_marge_n1        NUMERIC(12,4),
  PRIMARY KEY (site, datefiscale, nomennoid_n1, nomennoid_n2, mois)
);

CREATE TABLE IF NOT EXISTS budget_achat_mois (
  site          TEXT     NOT NULL,
  datefiscale   DATE     NOT NULL,
  nomennoid_n1  BIGINT   NOT NULL,
  nomennoid_n2  BIGINT   NOT NULL,
  mois          SMALLINT NOT NULL,
  achat_n       NUMERIC(16,4),
  achat_n1      NUMERIC(16,4),
  achat_n2      NUMERIC(16,4),
  achat_n3      NUMERIC(16,4),
  achat_n4      NUMERIC(16,4),
  PRIMARY KEY (site, datefiscale, nomennoid_n1, nomennoid_n2, mois)
);

-- ============================================================
-- Météo
-- ============================================================

CREATE TABLE IF NOT EXISTS meteo_temps (        -- MeteoTemps (sans l'image)
  no_id    BIGINT PRIMARY KEY,
  libelle  TEXT
);

CREATE TABLE IF NOT EXISTS meteo (              -- Meteo (relevé quotidien depuis 2015)
  site         TEXT NOT NULL,
  date         DATE NOT NULL,
  temps_no_id  BIGINT,
  temperature  INTEGER,
  commentaire  TEXT,
  PRIMARY KEY (site, date)
);

CREATE TABLE IF NOT EXISTS meteo_prevision (    -- Meteo_Prevision
  site              TEXT NOT NULL,
  date_meteo        DATE NOT NULL,
  weather_code      INTEGER,
  temperature_min   NUMERIC(6,2),
  temperature_max   NUMERIC(6,2),
  temperature_moy   NUMERIC(6,2),
  precipitation_mm  NUMERIC(8,2),
  neige_cm          NUMERIC(8,2),
  rafale_max_kmh    NUMERIC(8,2),
  date_import       TIMESTAMP,
  PRIMARY KEY (site, date_meteo)
);

-- ============================================================
-- Articles : compléments
-- ============================================================

CREATE TABLE IF NOT EXISTS note_articles (      -- NoteArticles (dédoublonné : dernière entrée)
  artnoid        BIGINT NOT NULL,
  site           TEXT   NOT NULL,
  colisage       NUMERIC(12,3),
  datederentree  TIMESTAMP,
  qtederentree   NUMERIC(12,3),
  totqte         NUMERIC(14,3),
  nbjour         INTEGER,
  note           NUMERIC(12,4),
  PRIMARY KEY (artnoid, site)
);

CREATE TABLE IF NOT EXISTS artsite (            -- ArtSite : paramètres de réassort
  artnoid         BIGINT NOT NULL,
  site            TEXT   NOT NULL,
  qtemini         NUMERIC(12,3),
  qtemaxi         NUMERIC(12,3),
  saisonnier      BOOLEAN,
  modereappro     SMALLINT,
  suividatemodif  TIMESTAMP,
  PRIMARY KEY (artnoid, site)
);

CREATE TABLE IF NOT EXISTS histo_art_gamme (    -- HISTO_ART_GAMME
  artnoid     BIGINT,
  oldidgamme  BIGINT,
  newidgamme  BIGINT,
  date        TIMESTAMP,
  util        TEXT
);
CREATE INDEX IF NOT EXISTS idx_histo_art_gamme_art  ON histo_art_gamme (artnoid);
CREATE INDEX IF NOT EXISTS idx_histo_art_gamme_date ON histo_art_gamme (date);

CREATE TABLE IF NOT EXISTS article_lot (        -- ARTICLE_LOT
  artnoid       BIGINT NOT NULL,
  art_lot_noid  BIGINT NOT NULL,
  quantite      NUMERIC(12,3),
  pourc_marge   NUMERIC(10,4),
  idartfou1     BIGINT,
  PRIMARY KEY (artnoid, art_lot_noid)
);

CREATE TABLE IF NOT EXISTS article_lot_composant ( -- ARTICLE_LOT_COMPOSANT
  lot_code   TEXT NOT NULL,
  lot_codea  TEXT NOT NULL,
  lot_libe   TEXT,
  lot_qte    NUMERIC(12,3),
  lot_pc     NUMERIC(12,4),
  lot_pv     NUMERIC(12,4),
  PRIMARY KEY (lot_code, lot_codea)
);

-- Historique des prix de vente, décodé depuis LIG_TC_MAGASIN / LIG_TC_CENTRALE
-- (format source : "[01/01/1901,4.99:26/09/2022,7.99:...]"). site '000' = centrale.
CREATE TABLE IF NOT EXISTS prix_vente_historique (
  article_no_id       BIGINT NOT NULL,
  site                TEXT   NOT NULL,
  tarif_client_no_id  BIGINT NOT NULL,
  date_effet          DATE   NOT NULL,
  prix                NUMERIC(12,4),
  PRIMARY KEY (article_no_id, site, tarif_client_no_id, date_effet)
);
CREATE INDEX IF NOT EXISTS idx_pvh_date ON prix_vente_historique (date_effet);

CREATE TABLE IF NOT EXISTS etiquettes_auto_pv ( -- Etiquettes_Auto_PV : changements de prix à étiqueter
  no_id           BIGINT PRIMARY KEY,
  code_fourn      TEXT,
  codin           TEXT,
  devise          TEXT,
  prixach         NUMERIC(12,4),
  lig_prix        NUMERIC(12,4),
  pub             TEXT,
  gtin            TEXT,
  colisage        NUMERIC(12,3),
  qte_stock       NUMERIC(12,3),
  article_no_id   BIGINT,
  nb_etiqu_u      INTEGER,
  nb_etiqu_g      INTEGER,
  dateetiq        TIMESTAMP,
  traite          INTEGER,
  datetraitement  TIMESTAMP,
  catalogue       INTEGER
);
CREATE INDEX IF NOT EXISTS idx_etiq_pv_article ON etiquettes_auto_pv (article_no_id);
CREATE INDEX IF NOT EXISTS idx_etiq_pv_date    ON etiquettes_auto_pv (dateetiq);

-- ============================================================
-- Promotions / remises caisse (TC_REMISE*)
-- ============================================================

CREATE TABLE IF NOT EXISTS tc_remise (
  no_id               BIGINT PRIMARY KEY,
  tarif_client_no_id  BIGINT,
  tcr_code            TEXT,
  tcr_libelle         TEXT,
  tcr_nature          SMALLINT,
  tcr_type            SMALLINT,
  suividatecreation   TIMESTAMP,
  suividatemodif      TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_tc_remise_code ON tc_remise (tcr_code);

CREATE TABLE IF NOT EXISTS tc_remise_date (
  no_id                BIGINT PRIMARY KEY,
  tc_remise_no_id      BIGINT,
  tc_promo_stat_no_id  BIGINT,
  datedeb              TIMESTAMP,
  datefin              TIMESTAMP,
  critere              SMALLINT,
  regime               TEXT
);
CREATE INDEX IF NOT EXISTS idx_tc_remise_date_remise ON tc_remise_date (tc_remise_no_id);
CREATE INDEX IF NOT EXISTS idx_tc_remise_date_dates  ON tc_remise_date (datedeb, datefin);

CREATE TABLE IF NOT EXISTS tc_remise_filtre (
  no_id                 BIGINT PRIMARY KEY,
  tc_remise_date_no_id  BIGINT,
  tcrf_type             SMALLINT,      -- 4 = article
  tcrf_noid             BIGINT,        -- id article quand type = 4
  tcrf_remise           TEXT,          -- ex. "-50.00%"
  remise_pct            NUMERIC(8,3)   -- décodé depuis tcrf_remise quand c'est un pourcentage
);
CREATE INDEX IF NOT EXISTS idx_tc_remise_filtre_date ON tc_remise_filtre (tc_remise_date_no_id);
CREATE INDEX IF NOT EXISTS idx_tc_remise_filtre_noid ON tc_remise_filtre (tcrf_noid);

CREATE TABLE IF NOT EXISTS remise_ref_article (
  no_id                   BIGINT PRIMARY KEY,
  tc_remise_filtre_no_id  BIGINT,
  reassortiment           BOOLEAN,
  pos_page                SMALLINT,
  quantite_cons           NUMERIC(12,3),
  commentaire             TEXT
);
CREATE INDEX IF NOT EXISTS idx_rra_filtre ON remise_ref_article (tc_remise_filtre_no_id);

CREATE TABLE IF NOT EXISTS remise_site (
  tc_remise_date_no_id  BIGINT NOT NULL,
  sitecode              TEXT   NOT NULL,
  datedeb               TIMESTAMP,
  datefin               TIMESTAMP,
  commentaire           TEXT,
  PRIMARY KEY (tc_remise_date_no_id, sitecode)
);

-- ============================================================
-- Fournisseurs / commandes : suivi, litiges, conditions
-- ============================================================

CREATE TABLE IF NOT EXISTS commande_fou_suivi_etat (
  no_id                    BIGINT PRIMARY KEY,
  commande_fou_no_id       BIGINT,
  etat_commande_no_id      BIGINT,
  envoye                   BOOLEAN,
  dateetat                 TIMESTAMP,
  sit_cod_desti            TEXT,
  numero_container         TEXT,
  comment_container        TEXT,
  code_incoterm            TEXT,
  ville_incoterm           TEXT,
  date_depart_incoterm     TIMESTAMP,
  port_arrivee_incoterm    TEXT,
  numero_dossier_incoterm  TEXT,
  type_transit_incoterm    INTEGER,
  commentaire_incoterm     TEXT,
  reception_auto           BOOLEAN,
  date_reception_auto      TIMESTAMP,
  date_rdv_depot           TIMESTAMP,
  heure_rdv_depot          INTEGER
);
CREATE INDEX IF NOT EXISTS idx_cfse_commande ON commande_fou_suivi_etat (commande_fou_no_id);

CREATE TABLE IF NOT EXISTS receplig_refus (     -- RECEPLIG_REFUS
  no_id                  BIGINT PRIMARY KEY,
  cdefou_receplig_no_id  BIGINT,
  motif_refus_no_id      BIGINT,
  qterefus               NUMERIC(12,3),
  suividatecreation      TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_receplig_refus_lig ON receplig_refus (cdefou_receplig_no_id);

CREATE TABLE IF NOT EXISTS foureg (             -- FOUREG : conditions de règlement
  no_id               BIGINT PRIMARY KEY,
  code                TEXT,
  reg_code            TEXT,
  reg2_code           TEXT,
  reg3_code           TEXT,
  reg4_code           TEXT,
  reg5_code           TEXT,
  sit_code            TEXT,
  plagedeb            TIMESTAMP,
  plagefin            TIMESTAMP,
  regcasnum           SMALLINT,
  regechnum           SMALLINT,
  regechescompte      NUMERIC(10,4),
  regechpart          NUMERIC(10,4),
  regechmini          NUMERIC(14,4),
  regechmaxi          NUMERIC(14,4),
  regechtranche       NUMERIC(14,4),
  actif               BOOLEAN,
  regremouverture     NUMERIC(10,4),
  regremlogistique    NUMERIC(10,4),
  regdatvalouverture  TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_foureg_code ON foureg (code);

CREATE TABLE IF NOT EXISTS fouregle (           -- FOUREGLE : délais fournisseur
  no_id          BIGINT PRIMARY KEY,
  sit_code       TEXT,
  fou_code       TEXT,
  delaisecurite  TEXT,
  delainormal    TEXT,
  gesreliquat    SMALLINT
);
CREATE INDEX IF NOT EXISTS idx_fouregle_fou ON fouregle (fou_code);

CREATE TABLE IF NOT EXISTS cdefou_reg (         -- CDEFOU_REG : règlement par commande
  no_id               BIGINT PRIMARY KEY,
  commande_fou_no_id  BIGINT,
  regmoyen_code       TEXT,
  regdatdeb_code      TEXT,
  regdelai_code       TEXT,
  regarrivee_code     TEXT,
  regdepart_code      TEXT,
  cderegcas           SMALLINT,
  cderegech           SMALLINT,
  cderegesc           NUMERIC(10,4),
  cdereglib           TEXT,
  cderegmax           NUMERIC(14,4),
  cderegmin           NUMERIC(14,4),
  cderegpart          NUMERIC(10,4),
  cderegtranche       NUMERIC(14,4)
);
CREATE INDEX IF NOT EXISTS idx_cdefou_reg_cde ON cdefou_reg (commande_fou_no_id);

-- ============================================================
-- Rotation / réappro / classements
-- ============================================================

CREATE TABLE IF NOT EXISTS statpermtemp (       -- STATPERMTEMP
  site               TEXT NOT NULL,
  codein             TEXT NOT NULL,
  artcentrale        TEXT,
  dispoperm          TEXT,
  dateactualisation  TIMESTAMP,
  pv                 NUMERIC(12,4),
  stock              NUMERIC(12,3),
  ventesn_4          NUMERIC(12,3),   -- ventes des 4 dernières semaines
  ventesn1_4         NUMERIC(12,3),   -- mêmes 4 semaines en N-1
  encours            NUMERIC(12,3),
  proposition        NUMERIC(12,3),
  gamme              TEXT,
  saison             TEXT,
  propoauto          BOOLEAN,
  PRIMARY KEY (site, codein)
);

CREATE TABLE IF NOT EXISTS stockage_rotation (  -- STOCKAGE_ROTATION
  codesite       TEXT   NOT NULL,
  artnoid        BIGINT NOT NULL,
  codein         TEXT,
  libticket      TEXT,
  fourncode      TEXT,
  fournnom       TEXT,
  nomencode      TEXT,
  nomenlib       TEXT,
  marquecode     TEXT,
  marquelib      TEXT,
  gammecode      TEXT,
  gammelib       TEXT,
  saisoncode     TEXT,
  saisonlib      TEXT,
  gtin           TEXT,
  stock          NUMERIC(12,3),
  vente          NUMERIC(12,3),
  ratio          NUMERIC(14,4),
  prixachat      NUMERIC(12,4),
  prixachatnet   NUMERIC(12,4),
  prmp           NUMERIC(12,4),
  prixvente      NUMERIC(12,4),
  derentree      TIMESTAMP,
  dervente       TIMESTAMP,
  reference      TEXT,
  derinventaire  TIMESTAMP,
  PRIMARY KEY (codesite, artnoid)
);

CREATE TABLE IF NOT EXISTS ranking_helios (     -- RANKING_HELIOS
  artnoid     BIGINT NOT NULL,
  site        TEXT   NOT NULL,
  ranking     INTEGER,
  saisonnier  INTEGER,
  PRIMARY KEY (artnoid, site)
);

CREATE TABLE IF NOT EXISTS commande_auto_detail_semaine ( -- COMMANDE_AUTO_QTEPROPO_DETAIL_SEMAINE
  fou_code          TEXT    NOT NULL,
  codesite          TEXT    NOT NULL,
  type              TEXT    NOT NULL,
  artnoid           BIGINT  NOT NULL,
  annee             TEXT    NOT NULL,
  numsem            INTEGER NOT NULL,
  parametre_numero  INTEGER NOT NULL,
  valeurstk         NUMERIC(14,3),
  valeurvte         NUMERIC(14,3),
  utilcalcul        INTEGER,
  sequentiel        INTEGER,
  PRIMARY KEY (fou_code, codesite, type, artnoid, annee, numsem, parametre_numero)
);
CREATE INDEX IF NOT EXISTS idx_cads_art ON commande_auto_detail_semaine (artnoid);

-- ============================================================
-- Inventaires
-- ============================================================

CREATE TABLE IF NOT EXISTS inv_suivi (
  no_id           BIGINT PRIMARY KEY,
  datedebut       TIMESTAMP,
  datefin         TIMESTAMP,
  typeinventaire  SMALLINT,
  datevalidation  TIMESTAMP,
  libelle         TEXT,
  datemvt         TIMESTAMP,
  ventesjourinv   BOOLEAN,
  codesite        TEXT
);

CREATE TABLE IF NOT EXISTS inv_articles (
  codart    TEXT,
  codin     TEXT,
  qte       NUMERIC(12,3),
  datemvt   TIMESTAMP,
  codesite  TEXT
);

CREATE TABLE IF NOT EXISTS inv_releve_detail (
  idreleve  BIGINT  NOT NULL,
  numligne  INTEGER NOT NULL,
  codin     TEXT,
  codart    TEXT,
  qte       NUMERIC(12,3),
  PRIMARY KEY (idreleve, numligne)
);

-- ============================================================
-- Commandes clients & web (sans données nominatives — RGPD)
-- ============================================================

CREATE TABLE IF NOT EXISTS commande_client (    -- COMMANDECLIENT (sans nom/adresse/tél/mail)
  idcdecli         BIGINT PRIMARY KEY,
  numcdecli        TEXT,
  typcdecli        SMALLINT,
  typliv           SMALLINT,
  datcdecli        TIMESTAMP,
  etatcdecli       SMALLINT,
  datlivsou        TIMESTAMP,
  mnthtbrut        NUMERIC(14,4),
  mntremht         NUMERIC(14,4),
  mnthtnet         NUMERIC(14,4),
  mntttc           NUMERIC(14,4),
  mnt_a_encaisser  NUMERIC(14,4),
  mnt_encaisse     NUMERIC(14,4),
  acompte_pc       NUMERIC(8,4),
  ddisposition     TIMESTAMP,
  idvendeur        BIGINT,
  etat_second      TEXT,
  cdeclitot        TIMESTAMP,
  cdeclitard       TIMESTAMP,
  dcre             TIMESTAMP,
  dmod             TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_commande_client_date ON commande_client (datcdecli);

CREATE TABLE IF NOT EXISTS commande_client_ligne ( -- CDECLILIGNE
  idcdeclilig         BIGINT PRIMARY KEY,
  idcdecli            BIGINT,
  idarticle           BIGINT,
  idartfou1           BIGINT,
  nlcdeclilig         SMALLINT,
  refcdeclilig        TEXT,
  eancdeclilig        TEXT,
  lib1cdeclilig       TEXT,
  qtecdeclilig        NUMERIC(12,3),
  puttcnetcdeclilig   NUMERIC(12,4),
  mnthtnetcdeclilig   NUMERIC(14,4),
  mntttcnetcdeclilig  NUMERIC(14,4),
  etatclilig          INTEGER,
  dpaiement           TIMESTAMP,
  dlivraison          TIMESTAMP,
  cdeclilig_tot       TIMESTAMP,
  cdeclilig_tard      TIMESTAMP,
  dateclientinforme   TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_ccl_cde     ON commande_client_ligne (idcdecli);
CREATE INDEX IF NOT EXISTS idx_ccl_article ON commande_client_ligne (idarticle);

CREATE TABLE IF NOT EXISTS web_commande (       -- SoClozWebCommande (sans le XML nominatif)
  numcommande     TEXT PRIMARY KEY,
  daterecup       TIMESTAMP,
  datetraitement  TIMESTAMP,
  deliver         TIMESTAMP,
  status          TEXT
);

CREATE TABLE IF NOT EXISTS art_web_stock (      -- ArtWebStock : stock exposé au web
  idarticle    TEXT NOT NULL,
  idmag        TEXT NOT NULL,
  qtestock     NUMERIC(12,3),
  qteencde     NUMERIC(12,3),
  pvmag        NUMERIC(12,4),
  interditvte  BOOLEAN,
  datemaj      TIMESTAMP,
  PRIMARY KEY (idarticle, idmag)
);

-- ============================================================
-- Index complémentaires pour les requêtes de l'API (oct. 2026)
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_fouident_trim_code   ON fouident (TRIM(code));
CREATE INDEX IF NOT EXISTS idx_artfou1_trim_code    ON artfou1 (TRIM(code));
CREATE INDEX IF NOT EXISTS idx_foucad_trim_code     ON foucad (TRIM(foucode));
CREATE INDEX IF NOT EXISTS idx_fouport_trim_code    ON fouport (TRIM(code));
CREATE INDEX IF NOT EXISTS idx_fouadr1_trim_code    ON fouadr1 (TRIM(code)) WHERE sit_code = '000';
CREATE INDEX IF NOT EXISTS idx_mvtart_site_date     ON mvtart (site, datmvt);
CREATE INDEX IF NOT EXISTS idx_caqp_fou_code        ON commande_auto_qtepropo (UPPER(TRIM(fou_code)));
CREATE INDEX IF NOT EXISTS idx_pub_ecoulement_site  ON pub_ecoulement (site, tcrd_datedeb);
CREATE INDEX IF NOT EXISTS idx_pub_entetes_datdeb   ON pub_entetes (ent_datdeb);
CREATE INDEX IF NOT EXISTS idx_commande_fou_trim    ON commande_fou (TRIM(fouident_code));
CREATE INDEX IF NOT EXISTS idx_cdefou_reception_com ON cdefou_reception (com_no_id);
CREATE INDEX IF NOT EXISTS idx_stockage_rotation_site ON stockage_rotation (codesite, ratio DESC);
DO $$ BEGIN
  CREATE INDEX IF NOT EXISTS idx_art_gtin_gtin_trgm ON art_gtin USING gin (gtin gin_trgm_ops);
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE 'index trigramme art_gtin ignoré : %', SQLERRM;
END $$;
