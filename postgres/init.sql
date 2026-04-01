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
  artcentrale       TEXT
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
  codefou    TEXT
);
CREATE INDEX IF NOT EXISTS idx_mvtart_artnoid   ON mvtart (artnoid);
CREATE INDEX IF NOT EXISTS idx_mvtart_datmvt    ON mvtart (datmvt);
CREATE INDEX IF NOT EXISTS idx_mvtart_site      ON mvtart (site);
CREATE INDEX IF NOT EXISTS idx_mvtart_genremvt  ON mvtart (genremvt);

CREATE TABLE IF NOT EXISTS mvtreg (
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
  no_id             BIGINT PRIMARY KEY,
  suividatecreation TIMESTAMP,
  suividatemodif    TIMESTAMP
);

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
