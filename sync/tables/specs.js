/**
 * Specs déclaratives des tables synchronisées par le moteur générique (engine.js).
 * Chaque colonne : [colonne_pg, colonne_source | fn(row), conversion].
 */
'use strict';
const { conv: c } = require('../engine');

// ============================================================
// Mouvements (fenêtre glissante + rechargement historique)
// ============================================================

const MVTART = {
  name: 'mvtart', table: 'mvtart', mode: 'window',
  dateCol: 'datmvt', windowDays: 10, initialMonths: 24, pk: ['no_id'],
  // colonnes ajoutées en oct. 2026 encore vides → recharger l'historique
  backfillCheck: `SELECT EXISTS (SELECT 1 FROM mvtart WHERE mvttypnoid IS NULL
                  AND datmvt >= now() - interval '23 months') AS needed`,
  source: `
    SELECT NO_ID, ArtNoId, DatMvt, Site, LibMvt, GenreMvt, QteMvt, ValMvt,
           MntMvtHt, MntMvtTTC, MargeMvt, QteStock, Prmp, ValStock, CODEFOU,
           MvtTypNoId, MvtGrpNoId, IdPtDist, TiersRef, TiersTyp, NomNoId,
           MntPrmpMvt, CodeTva, TauxTva, Champ1, Champ2, Champ3, Champ4, Champ5,
           SUIVIDATEMODIF
    FROM MvtArt WITH (NOLOCK)
    WHERE DatMvt >= @d1 AND DatMvt < @d2`,
  cols: [
    ['no_id', 'NO_ID', c.id], ['artnoid', 'ArtNoId', c.id], ['datmvt', 'DatMvt', c.ts],
    ['site', 'Site', c.str], ['libmvt', 'LibMvt', c.str], ['genremvt', 'GenreMvt', c.int],
    ['qtemvt', 'QteMvt', c.num], ['valmvt', 'ValMvt', c.num], ['mntmvtht', 'MntMvtHt', c.num],
    ['mntmvtttc', 'MntMvtTTC', c.num], ['margemvt', 'MargeMvt', c.num], ['qtestock', 'QteStock', c.num],
    ['prmp', 'Prmp', c.num], ['valstock', 'ValStock', c.num], ['codefou', 'CODEFOU', c.str],
    ['mvttypnoid', 'MvtTypNoId', c.id], ['mvtgrpnoid', 'MvtGrpNoId', c.id], ['idptdist', 'IdPtDist', c.id],
    ['tiersref', 'TiersRef', c.str], ['tierstyp', 'TiersTyp', c.str], ['nomnoid', 'NomNoId', c.id],
    ['mntprmpmvt', 'MntPrmpMvt', c.num], ['codetva', 'CodeTva', c.str], ['tauxtva', 'TauxTva', c.num],
    ['ticket_ts', 'Champ1', c.tsfr], ['prix_unitaire', 'Champ2', c.num], ['champ3', 'Champ3', c.str],
    ['champ4', 'Champ4', c.str], ['gtin', 'Champ5', c.str], ['suividatemodif', 'SUIVIDATEMODIF', c.ts],
  ],
};

const MVTREG = {
  name: 'mvtreg', table: 'mvtreg', mode: 'window',
  dateCol: 'datmvt', windowDays: 10, initialMonths: 24, pk: ['no_id'],
  source: `
    SELECT No_Id, DatMvt, CodTick, CodCarteCli, RegNoId, CodDev, MntReg, MntRegDev,
           NumLot, ClientNom, Echeance, REFERENCE, TYPEREG, SuiviDateCreation
    FROM MvtReg WITH (NOLOCK)
    WHERE DatMvt >= @d1 AND DatMvt < @d2`,
  cols: [
    ['no_id', 'No_Id', c.id], ['datmvt', 'DatMvt', c.ts], ['codtick', 'CodTick', c.str],
    ['codcartecli', 'CodCarteCli', c.str], ['regnoid', 'RegNoId', c.id], ['coddev', 'CodDev', c.str],
    ['mntreg', 'MntReg', c.num], ['mntregdev', 'MntRegDev', c.num], ['numlot', 'NumLot', c.str],
    ['clientnom', 'ClientNom', c.str], ['echeance', 'Echeance', c.ts], ['reference', 'REFERENCE', c.str],
    ['typereg', 'TYPEREG', c.str], ['suividatecreation', 'SuiviDateCreation', c.ts],
  ],
};

const STATOPCA = {
  name: 'statopca', table: 'statopca', mode: 'window',
  dateCol: 'datmvt', windowDays: 10, initialMonths: 24, pk: ['site', 'datmvt'],
  source: `SELECT SITE, DatMvt, mnt, nbTicket FROM StatOpCAJour
           WHERE DatMvt >= @d1 AND DatMvt < @d2`,
  cols: [['site', 'SITE', c.str], ['datmvt', 'DatMvt', c.day], ['mnt', 'mnt', c.num], ['nbticket', 'nbTicket', c.int]],
};

const MVTART_MOTIF = {
  name: 'mvtart_motif', table: 'mvtart_motif', mode: 'window',
  dateCol: 'datmvt', windowDays: 10, initialMonths: 24, pk: ['mvtart_no_id'],
  source: `
    SELECT mm.Mvtart_NO_ID, m.DatMvt, mm.Prix_Orig, mm.Motif, mm.Motif_Valeur,
           mm.Champ1, mm.Champ2, mm.Champ3, mm.Champ4, mm.Champ5, mm.Champ6
    FROM MvtArt_Motif mm WITH (NOLOCK)
    JOIN MvtArt m WITH (NOLOCK) ON m.NO_ID = mm.Mvtart_NO_ID
    WHERE m.DatMvt >= @d1 AND m.DatMvt < @d2`,
  cols: [
    ['mvtart_no_id', 'Mvtart_NO_ID', c.id], ['datmvt', 'DatMvt', c.ts], ['prix_orig', 'Prix_Orig', c.num],
    ['motif', 'Motif', c.str], ['motif_valeur', 'Motif_Valeur', c.str],
    ['champ1', 'Champ1', c.str], ['champ2', 'Champ2', c.str], ['champ3', 'Champ3', c.str],
    ['champ4', 'Champ4', c.str], ['champ5', 'Champ5', c.str], ['champ6', 'Champ6', c.str],
  ],
};

const FIDELITE_JOUR = {
  name: 'fidelite_jour', table: 'fidelite_jour', mode: 'window',
  dateCol: 'date', windowDays: 10, initialMonths: 36, pk: ['site', 'date'],
  // Agrégat uniquement : aucune donnée nominative ne quitte SQL Server
  source: `
    SELECT CODSITE, CAST(DATMVTFIDEL AS date) AS D, COUNT(*) AS N,
           COUNT(DISTINCT NUMTICKET) AS NT, COUNT(DISTINCT IDCARTE) AS NC,
           SUM(MNTACHAT) AS CA, SUM(NBPOINTS) AS PTS
    FROM MVTFIDELITE WITH (NOLOCK)
    WHERE DATMVTFIDEL >= @d1 AND DATMVTFIDEL < @d2
    GROUP BY CODSITE, CAST(DATMVTFIDEL AS date)`,
  cols: [
    ['site', 'CODSITE', c.str], ['date', 'D', c.day], ['nb_mouvements', 'N', c.int],
    ['nb_tickets', 'NT', c.int], ['nb_cartes', 'NC', c.int], ['ca_achat', 'CA', c.num], ['points', 'PTS', c.int],
  ],
};

const HISTO_ART_GAMME = {
  name: 'histo_art_gamme', table: 'histo_art_gamme', mode: 'window',
  dateCol: 'date', windowDays: 10, initialMonths: 132,
  source: `SELECT ARTNOID, OLDIDGAMME, NEWIDGAMME, [DATE], UTIL FROM HISTO_ART_GAMME
           WHERE [DATE] >= @d1 AND [DATE] < @d2`,
  cols: [
    ['artnoid', 'ARTNOID', c.id], ['oldidgamme', 'OLDIDGAMME', c.id], ['newidgamme', 'NEWIDGAMME', c.id],
    ['date', 'DATE', c.ts], ['util', 'UTIL', c.str],
  ],
};

// ============================================================
// Référentiels (full, petites tables)
// ============================================================

const full = (name, source, pk, cols, extra = {}) => ({ name, table: name, mode: 'full', source, pk, cols, ...extra });

const REFERENTIELS = [
  full('mvttype', `SELECT NO_ID, MvtGrpNoId, CodeMvt, LibelleMvt, SensMvt, GenreMvt, Cumul FROM MvtType`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['mvtgrpnoid', 'MvtGrpNoId', c.id], ['codemvt', 'CodeMvt', c.str],
    ['libellemvt', 'LibelleMvt', c.str], ['sensmvt', 'SensMvt', c.int], ['genremvt', 'GenreMvt', c.int], ['cumul', 'Cumul', c.int],
  ]),
  full('mvtgrp', `SELECT NO_ID, CodeGrpMvt, LibelleGrpMvt FROM MvtGrp`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['codegrpmvt', 'CodeGrpMvt', c.str], ['libellegrpmvt', 'LibelleGrpMvt', c.str],
  ]),
  full('mode_reglement', `SELECT No_Id, CodReg, LibReg, CodCai, Compta FROM ModeReglement`, ['no_id'], [
    ['no_id', 'No_Id', c.id], ['codreg', 'CodReg', c.str], ['libreg', 'LibReg', c.str],
    ['codcai', 'CodCai', c.str], ['compta', 'Compta', c.bool],
  ]),
  full('sites', `SELECT CODE, SIT_CODE, NOM, NIVEAU, EST_BASE, isDepot FROM SITE`, ['code'], [
    ['code', 'CODE', c.str], ['sit_code', 'SIT_CODE', c.str], ['nom', 'NOM', c.str],
    ['niveau', 'NIVEAU', c.int], ['est_base', 'EST_BASE', c.bool], ['is_depot', 'isDepot', c.bool],
  ]),
  full('tarif_client', `SELECT NO_ID, DEVISE_CODE, TC_CODE, TC_NOM FROM TARIF_CLIENT`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['devise_code', 'DEVISE_CODE', c.str], ['tc_code', 'TC_CODE', c.str], ['tc_nom', 'TC_NOM', c.str],
  ]),
  full('etat_commande', `SELECT NO_ID, ETAT, LIBELLE, COULEUR FROM ETAT_COMMANDE`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['etat', 'ETAT', c.str], ['libelle', 'LIBELLE', c.str], ['couleur', 'COULEUR', c.str],
  ]),
  full('motifs_refus', `SELECT NO_ID, CODE, LIBELLE FROM MOTIFS_REFUS`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['code', 'CODE', c.str], ['libelle', 'LIBELLE', c.str],
  ]),
  full('meteo_temps', `SELECT NO_ID, LIBELLE FROM MeteoTemps`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['libelle', 'LIBELLE', c.str],
  ]),
];

// ============================================================
// Pilotage : budgets (colonnes mensuelles dépivotées)
// ============================================================

const BUDGET_KEY = [
  ['site', 'SITE', c.str], ['datefiscale', 'DATEFISCALE', c.day],
  ['nomennoid_n1', 'NOMENNOID_N1', (v) => c.id(v) ?? 0], ['nomennoid_n2', 'NOMENNOID_N2', (v) => c.id(v) ?? 0],
];
const keyOf = (r) => ({
  site: c.str(r.SITE), datefiscale: c.day(r.DATEFISCALE),
  nomennoid_n1: c.id(r.NOMENNOID_N1) ?? 0, nomennoid_n2: c.id(r.NOMENNOID_N2) ?? 0,
});
const MONTHS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

const BUDGET_SCALAIRES = [
  'CA_HT_AN2', 'CA_AN2', 'CA_HT_AN1', 'CA_AN1', 'CA_COEFF', 'CA_HT_OBJECTIF', 'CA_OBJECTIF', 'CA_HT_AN', 'CA_AN',
  'STK_AN1', 'STK_COEFF', 'STK_OBJECTIF', 'STK_AN', 'MARGE_VALEUR_AN2', 'MARGE_TAUX_AN2', 'MARGE_VALEUR_AN1',
  'MARGE_TAUX_AN1', 'MARGE_TAUX', 'MARGE_VALEUR_AN', 'MARGE_TAUX_AN', 'DEMARQUE_CONNUE_AN2', 'DEMARQUE_INCONNUE_AN2',
  'DEMARQUE_CONNUE_AN1', 'DEMARQUE_INCONNUE_AN1', 'DEMARQUE_CONNUE', 'DEMARQUE_TAUX', 'DEMARQUE_TAUX_CONNUE', 'LINEAIRE_UNITE',
];

const BUDGETS = [
  full('budget_pilotage', `SELECT * FROM BUDGET_PILOTAGE`, ['site', 'datefiscale', 'nomennoid_n1', 'nomennoid_n2'], [
    ...BUDGET_KEY, ['datecalcul', 'DATECALCUL', c.ts],
    ...BUDGET_SCALAIRES.map((k) => [k.toLowerCase(), k, c.num]),
  ]),
  {
    name: 'budget_pilotage_mois', table: 'budget_pilotage_mois', mode: 'full',
    source: `SELECT * FROM BUDGET_PILOTAGE`,
    pk: ['site', 'datefiscale', 'nomennoid_n1', 'nomennoid_n2', 'mois'],
    pgCols: ['site', 'datefiscale', 'nomennoid_n1', 'nomennoid_n2', 'mois', 'ca_ht', 'ca_ttc', 'ca_ht_exercice',
      'ca_ttc_exercice', 'stk', 'stk_exercice', 'marge_exercice', 'taux_marge_exercice', 'marge_n1', 'taux_marge_n1'],
    expand: (r) => MONTHS.map((m) => ({
      ...keyOf(r), mois: m,
      ca_ht: c.num(r[`CA_HT_MOIS${m}`]), ca_ttc: c.num(r[`CA_MOIS${m}`]),
      ca_ht_exercice: c.num(r[`CA_HT_EXERCICE_MOIS${m}`]), ca_ttc_exercice: c.num(r[`CA_EXERCICE_MOIS${m}`]),
      stk: c.num(r[`STK_MOIS${m}`]), stk_exercice: c.num(r[`STK_EXERCICE_MOIS${m}`]),
      marge_exercice: c.num(r[`MARGE_MARGE_EX_MOIS${m}`]), taux_marge_exercice: c.num(r[`MARGE_TAUX_EX_MOIS${m}`]),
      marge_n1: c.num(r[`MARGE_MARGE_N1_MOIS${m}`]), taux_marge_n1: c.num(r[`MARGE_TAUX_N1_MOIS${m}`]),
    })),
  },
  {
    name: 'budget_achat_mois', table: 'budget_achat_mois', mode: 'full',
    source: `SELECT * FROM BUDGET_PILOTAGE_ACHAT`,
    pk: ['site', 'datefiscale', 'nomennoid_n1', 'nomennoid_n2', 'mois'],
    pgCols: ['site', 'datefiscale', 'nomennoid_n1', 'nomennoid_n2', 'mois', 'achat_n', 'achat_n1', 'achat_n2', 'achat_n3', 'achat_n4'],
    expand: (r) => MONTHS.map((m) => ({
      ...keyOf(r), mois: m,
      achat_n: c.num(r[`ACHAT_N_MOIS${m}`]), achat_n1: c.num(r[`ACHAT_N1_MOIS${m}`]),
      achat_n2: c.num(r[`ACHAT_N2_MOIS${m}`]), achat_n3: c.num(r[`ACHAT_N3_MOIS${m}`]), achat_n4: c.num(r[`ACHAT_N4_MOIS${m}`]),
    })),
  },
];

// ============================================================
// Météo, démarque
// ============================================================

const METEO = [
  full('meteo', `SELECT SITE, [DATE], TEMPS_NO_ID, TEMPERATURE, COMMENTAIRE FROM Meteo`, ['site', 'date'], [
    ['site', 'SITE', c.str], ['date', 'DATE', c.day], ['temps_no_id', 'TEMPS_NO_ID', c.id],
    ['temperature', 'TEMPERATURE', c.int], ['commentaire', 'COMMENTAIRE', c.str],
  ]),
  full('meteo_prevision', `SELECT * FROM Meteo_Prevision`, ['site', 'date_meteo'], [
    ['site', 'SITE', c.str], ['date_meteo', 'DATE_METEO', c.day], ['weather_code', 'WEATHER_CODE', c.int],
    ['temperature_min', 'TEMPERATURE_MIN', c.num], ['temperature_max', 'TEMPERATURE_MAX', c.num],
    ['temperature_moy', 'TEMPERATURE_MOY', c.num], ['precipitation_mm', 'PRECIPITATION_MM', c.num],
    ['neige_cm', 'NEIGE_CM', c.num], ['rafale_max_kmh', 'RAFALE_MAX_KMH', c.num], ['date_import', 'DATE_IMPORT', c.ts],
  ], { minRatio: 0 }),   // prévisions : le volume varie légitimement
];

const DEMARQUES = full('demarques',
  `SELECT no_id, ArtNoid, Site, DatMvt, MvtTypNoid, QteMvt, ValMvt, MotifId, LibMvt, GTIN FROM Demarques`,
  ['no_id'], [
    ['no_id', 'no_id', c.id], ['artnoid', 'ArtNoid', c.id], ['site', 'Site', c.str], ['datmvt', 'DatMvt', c.ts],
    ['mvttypnoid', 'MvtTypNoid', c.id], ['qtemvt', 'QteMvt', c.num], ['valmvt', 'ValMvt', c.num],
    ['motifid', 'MotifId', c.id], ['libmvt', 'LibMvt', c.str], ['gtin', 'GTIN', c.str],
  ]);

// ============================================================
// Articles : compléments
// ============================================================

const ARTICLES_PLUS = [
  // NoteArticles a des doublons (artnoid, site) : tri par date → la dernière gagne (ON CONFLICT DO UPDATE)
  full('note_articles', `SELECT artnoid, site, colisage, DateDerEntree, QteDerEntree, TotQte, NbJour, Note
                         FROM NoteArticles ORDER BY artnoid, site, DateDerEntree`, ['artnoid', 'site'], [
    ['artnoid', 'artnoid', c.id], ['site', 'site', c.str], ['colisage', 'colisage', c.num],
    ['datederentree', 'DateDerEntree', c.ts], ['qtederentree', 'QteDerEntree', c.num], ['totqte', 'TotQte', c.num],
    ['nbjour', 'NbJour', c.int], ['note', 'Note', c.num],
  ], { filter: (o) => o.artnoid != null && o.site != null }),
  full('artsite', `SELECT ArtNoid, Site, QteMini, QteMaxi, Saisonnier, ModeReappro, SuiviDateModif FROM ArtSite`, ['artnoid', 'site'], [
    ['artnoid', 'ArtNoid', c.id], ['site', 'Site', c.str], ['qtemini', 'QteMini', c.num], ['qtemaxi', 'QteMaxi', c.num],
    ['saisonnier', 'Saisonnier', c.bool], ['modereappro', 'ModeReappro', c.int], ['suividatemodif', 'SuiviDateModif', c.ts],
  ], { filter: (o) => o.artnoid != null && o.site != null }),
  full('article_lot', `SELECT ARTNOID, ART_LOT_NOID, QUANTITE, POURC_MARGE, IDARTFOU1 FROM ARTICLE_LOT`, ['artnoid', 'art_lot_noid'], [
    ['artnoid', 'ARTNOID', c.id], ['art_lot_noid', 'ART_LOT_NOID', c.id], ['quantite', 'QUANTITE', c.num],
    ['pourc_marge', 'POURC_MARGE', c.num], ['idartfou1', 'IDARTFOU1', c.id],
  ], { filter: (o) => o.artnoid != null && o.art_lot_noid != null }),
  full('article_lot_composant', `SELECT Lot_Code, Lot_CodeA, Lot_Libe, Lot_Qte, Lot_Pc, Lot_Pv FROM ARTICLE_LOT_COMPOSANT`, ['lot_code', 'lot_codea'], [
    ['lot_code', 'Lot_Code', c.str], ['lot_codea', 'Lot_CodeA', c.str], ['lot_libe', 'Lot_Libe', c.str],
    ['lot_qte', 'Lot_Qte', c.num], ['lot_pc', 'Lot_Pc', c.num], ['lot_pv', 'Lot_Pv', c.num],
  ], { filter: (o) => o.lot_code != null && o.lot_codea != null }),
  full('etiquettes_auto_pv', `SELECT * FROM Etiquettes_Auto_PV`, ['no_id'], [
    ['no_id', 'no_id', c.id], ['code_fourn', 'CODE_FOURN', c.str], ['codin', 'CODIN', c.str], ['devise', 'Devise', c.str],
    ['prixach', 'PrixAch', c.num], ['lig_prix', 'LIG_PRIX', c.num], ['pub', 'Pub', c.str], ['gtin', 'GTIN', c.str],
    ['colisage', 'Colisage', c.num], ['qte_stock', 'QTE_STOCK', c.num], ['article_no_id', 'ARTICLE_NO_ID', c.id],
    ['nb_etiqu_u', 'NB_ETIQU_U', c.int], ['nb_etiqu_g', 'NB_ETIQU_G', c.int], ['dateetiq', 'DateEtiq', c.ts],
    ['traite', 'Traite', c.int], ['datetraitement', 'DateTraitement', c.ts], ['catalogue', 'Catalogue', c.int],
  ]),
];

// ============================================================
// Promotions / remises
// ============================================================

const PROMOS = [
  full('tc_remise', `SELECT NO_ID, TARIF_CLIENT_NO_ID, TCR_CODE, TCR_LIBELLE, TCR_NATURE, TCR_TYPE, SUIVIDATECREATION, SUIVIDATEMODIF FROM TC_REMISE`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['tarif_client_no_id', 'TARIF_CLIENT_NO_ID', c.id], ['tcr_code', 'TCR_CODE', c.str],
    ['tcr_libelle', 'TCR_LIBELLE', c.str], ['tcr_nature', 'TCR_NATURE', c.int], ['tcr_type', 'TCR_TYPE', c.int],
    ['suividatecreation', 'SUIVIDATECREATION', c.ts], ['suividatemodif', 'SUIVIDATEMODIF', c.ts],
  ]),
  full('tc_remise_date', `SELECT NO_ID, TC_REMISE_NO_ID, TC_PROMO_STAT_NO_ID, TCRD_DATEDEB, TCRD_DATEFIN, TCRD_CRITERE, TCRD_REGIME FROM TC_REMISE_DATE`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['tc_remise_no_id', 'TC_REMISE_NO_ID', c.id], ['tc_promo_stat_no_id', 'TC_PROMO_STAT_NO_ID', c.id],
    ['datedeb', 'TCRD_DATEDEB', c.ts], ['datefin', 'TCRD_DATEFIN', c.ts], ['critere', 'TCRD_CRITERE', c.int], ['regime', 'TCRD_REGIME', c.str],
  ]),
  full('tc_remise_filtre', `SELECT NO_ID, TC_REMISE_DATE_NO_ID, TCRF_TYPE, TCRF_NOID, TCRF_REMISE FROM TC_REMISE_FILTRE`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['tc_remise_date_no_id', 'TC_REMISE_DATE_NO_ID', c.id], ['tcrf_type', 'TCRF_TYPE', c.int],
    ['tcrf_noid', 'TCRF_NOID', c.id], ['tcrf_remise', 'TCRF_REMISE', c.str], ['remise_pct', 'TCRF_REMISE', c.pct],
  ]),
  full('remise_ref_article', `SELECT NO_ID, TC_REMISE_FILTRE_NO_ID, REASSORTIMENT, POS_PAGE, QUANTITE_CONS, COMMENTAIRE FROM REMISE_REF_ARTICLE`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['tc_remise_filtre_no_id', 'TC_REMISE_FILTRE_NO_ID', c.id], ['reassortiment', 'REASSORTIMENT', c.bool],
    ['pos_page', 'POS_PAGE', c.int], ['quantite_cons', 'QUANTITE_CONS', c.num], ['commentaire', 'COMMENTAIRE', c.str],
  ]),
  full('remise_site', `SELECT TC_REMISE_DATE_NO_ID, SITECODE, DATEDEB, DATEFIN, COMMENTAIRE FROM REMISE_SITE`, ['tc_remise_date_no_id', 'sitecode'], [
    ['tc_remise_date_no_id', 'TC_REMISE_DATE_NO_ID', c.id], ['sitecode', 'SITECODE', c.str],
    ['datedeb', 'DATEDEB', c.ts], ['datefin', 'DATEFIN', c.ts], ['commentaire', 'COMMENTAIRE', c.str],
  ], { filter: (o) => o.tc_remise_date_no_id != null && o.sitecode != null }),
];

// ============================================================
// Fournisseurs / commandes
// ============================================================

const FOURNISSEURS_PLUS = [
  full('commande_fou_suivi_etat', `SELECT * FROM COMMANDE_FOU_SUIVI_ETAT`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['commande_fou_no_id', 'COMMANDE_FOU_NO_ID', c.id], ['etat_commande_no_id', 'ETAT_COMMANDE_NO_ID', c.id],
    ['envoye', 'ENVOYE', c.bool], ['dateetat', 'DATEETAT', c.ts], ['sit_cod_desti', 'SIT_COD_DESTI', c.str],
    ['numero_container', 'NUMERO_CONTAINER', c.str], ['comment_container', 'COMMENT_CONTAINER', c.str],
    ['code_incoterm', 'CODE_INCOTERM', c.str], ['ville_incoterm', 'VILLE_INCOTERM', c.str],
    ['date_depart_incoterm', 'DATE_DEPART_INCOTERM', c.ts], ['port_arrivee_incoterm', 'PORT_ARRIVEE_INCOTERM', c.str],
    ['numero_dossier_incoterm', 'NUMERO_DOSSIER_INCOTERM', c.str], ['type_transit_incoterm', 'TYPE_TRANSIT_INCOTERM', c.int],
    ['commentaire_incoterm', 'COMMENTAIRE_INCOTERM', c.str], ['reception_auto', 'RECEPTION_AUTO', c.bool],
    ['date_reception_auto', 'DATE_RECEPTION_AUTO', c.ts], ['date_rdv_depot', 'DATE_RDV_DEPOT', c.ts], ['heure_rdv_depot', 'HEURE_RDV_DEPOT', c.int],
  ]),
  full('receplig_refus', `SELECT NO_ID, CDEFOU_RECEPLIG_NO_ID, MOTIF_REFUS_NO_ID, QTEREFUS, SUIVIDATECREATION FROM RECEPLIG_REFUS`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['cdefou_receplig_no_id', 'CDEFOU_RECEPLIG_NO_ID', c.id], ['motif_refus_no_id', 'MOTIF_REFUS_NO_ID', c.id],
    ['qterefus', 'QTEREFUS', c.num], ['suividatecreation', 'SUIVIDATECREATION', c.ts],
  ]),
  full('foureg', `SELECT * FROM FOUREG`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['code', 'CODE', c.str], ['reg_code', 'REG_CODE', c.str], ['reg2_code', 'REG2_CODE', c.str],
    ['reg3_code', 'REG3_CODE', c.str], ['reg4_code', 'REG4_CODE', c.str], ['reg5_code', 'REG5_CODE', c.str], ['sit_code', 'SIT_CODE', c.str],
    ['plagedeb', 'PLAGEDEB', c.ts], ['plagefin', 'PLAGEFIN', c.ts], ['regcasnum', 'REGCASNUM', c.int], ['regechnum', 'REGECHNUM', c.int],
    ['regechescompte', 'REGECHESCOMPTE', c.num], ['regechpart', 'REGECHPART', c.num], ['regechmini', 'REGECHMINI', c.num],
    ['regechmaxi', 'REGECHMAXI', c.num], ['regechtranche', 'REGECHTRANCHE', c.num], ['actif', 'ACTIF', c.bool],
    ['regremouverture', 'REGREMOUVERTURE', c.num], ['regremlogistique', 'REGREMLOGISTIQUE', c.num], ['regdatvalouverture', 'REGDATVALOUVERTURE', c.ts],
  ]),
  full('fouregle', `SELECT NO_ID, SIT_CODE, FOU_CODE, DELAISECURITE, DELAINORMAL, GESRELIQUAT FROM FOUREGLE`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['sit_code', 'SIT_CODE', c.str], ['fou_code', 'FOU_CODE', c.str],
    ['delaisecurite', 'DELAISECURITE', c.str], ['delainormal', 'DELAINORMAL', c.str], ['gesreliquat', 'GESRELIQUAT', c.int],
  ]),
  full('cdefou_reg', `SELECT * FROM CDEFOU_REG`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['commande_fou_no_id', 'COMMANDE_FOU_NO_ID', c.id], ['regmoyen_code', 'REGMOYEN_CODE', c.str],
    ['regdatdeb_code', 'REGDATDEB_CODE', c.str], ['regdelai_code', 'REGDELAI_CODE', c.str], ['regarrivee_code', 'REGARRIVEE_CODE', c.str],
    ['regdepart_code', 'REGDEPART_CODE', c.str], ['cderegcas', 'CDEREGCAS', c.int], ['cderegech', 'CDEREGECH', c.int],
    ['cderegesc', 'CDEREGESC', c.num], ['cdereglib', 'CDEREGLIB', c.str], ['cderegmax', 'CDEREGMAX', c.num],
    ['cderegmin', 'CDEREGMIN', c.num], ['cderegpart', 'CDEREGPART', c.num], ['cderegtranche', 'CDEREGTRANCHE', c.num],
  ]),
];

// ============================================================
// Rotation / réappro / classements
// ============================================================

const ROTATION = [
  full('statpermtemp', `SELECT * FROM STATPERMTEMP`, ['site', 'codein'], [
    ['site', 'Site', c.str], ['codein', 'Codein', c.str], ['artcentrale', 'ArtCentrale', c.str], ['dispoperm', 'DispoPerm', c.str],
    ['dateactualisation', 'DateActualisation', c.ts], ['pv', 'PV', c.num], ['stock', 'Stock', c.num],
    ['ventesn_4', 'VentesN_4', c.num], ['ventesn1_4', 'VentesN1_4', c.num], ['encours', 'EnCours', c.num],
    ['proposition', 'Proposition', c.num], ['gamme', 'Gamme', c.str], ['saison', 'Saison', c.str], ['propoauto', 'PropoAuto', c.bool],
  ], { filter: (o) => o.site != null && o.codein != null }),
  full('stockage_rotation', `SELECT * FROM STOCKAGE_ROTATION`, ['codesite', 'artnoid'], [
    ['codesite', 'CodeSite', c.str], ['artnoid', 'ArtNoId', c.id], ['codein', 'CodeIn', c.str], ['libticket', 'LibTicket', c.str],
    ['fourncode', 'fournCode', c.str], ['fournnom', 'fournNom', c.str], ['nomencode', 'nomenCode', c.str], ['nomenlib', 'nomenLib', c.str],
    ['marquecode', 'marqueCode', c.str], ['marquelib', 'marqueLib', c.str], ['gammecode', 'gammeCode', c.str], ['gammelib', 'gammeLib', c.str],
    ['saisoncode', 'saisonCode', c.str], ['saisonlib', 'saisonLib', c.str], ['gtin', 'gtin', c.str], ['stock', 'Stock', c.num],
    ['vente', 'Vente', c.num], ['ratio', 'Ratio', c.num], ['prixachat', 'PrixAchat', c.num], ['prixachatnet', 'PrixAchatNet', c.num],
    ['prmp', 'Prmp', c.num], ['prixvente', 'PrixVente', c.num], ['derentree', 'DerEntree', c.ts], ['dervente', 'DerVente', c.ts],
    ['reference', 'Reference', c.str], ['derinventaire', 'DerInventaire', c.ts],
  ], { filter: (o) => o.codesite != null && o.artnoid != null, minRatio: 0.2 }),
  full('ranking_helios', `SELECT ARTNOID, Site, Ranking, Saisonnier FROM RANKING_HELIOS`, ['artnoid', 'site'], [
    ['artnoid', 'ARTNOID', c.id], ['site', 'Site', c.str], ['ranking', 'Ranking', c.int], ['saisonnier', 'Saisonnier', c.int],
  ], { filter: (o) => o.artnoid != null && o.site != null }),
  full('commande_auto_detail_semaine', `SELECT * FROM COMMANDE_AUTO_QTEPROPO_DETAIL_SEMAINE`,
    ['fou_code', 'codesite', 'type', 'artnoid', 'annee', 'numsem', 'parametre_numero'], [
      ['fou_code', 'FOU_CODE', c.str], ['codesite', 'CODESITE', c.str], ['type', 'TYPE', c.str], ['artnoid', 'ARTNOID', c.id],
      ['annee', 'ANNEE', c.str], ['numsem', 'NUMSEM', c.int], ['parametre_numero', 'PARAMETRE_NUMERO', c.int],
      ['valeurstk', 'VALEURSTK', c.num], ['valeurvte', 'VALEURVTE', c.num], ['utilcalcul', 'UTILCALCUL', c.int], ['sequentiel', 'SEQUENTIEL', c.int],
    ], { filter: (o) => o.fou_code != null && o.codesite != null && o.type != null && o.artnoid != null && o.annee != null, minRatio: 0 }),
];

// ============================================================
// Inventaires
// ============================================================

const INVENTAIRES = [
  full('inv_suivi', `SELECT * FROM InvSuivi`, ['no_id'], [
    ['no_id', 'NO_ID', c.id], ['datedebut', 'DateDébut', c.ts], ['datefin', 'DateFin', c.ts],
    ['typeinventaire', 'TypeInventaire', c.int], ['datevalidation', 'DateValidation', c.ts], ['libelle', 'LIBELLE', c.str],
    ['datemvt', 'DATEMVT', c.ts], ['ventesjourinv', 'VENTESJOURINV', c.bool], ['codesite', 'CodeSite', c.str],
  ]),
  full('inv_articles', `SELECT CODART, CODIN, Qte, DateMvt, CodeSite FROM InvArticles`, null, [
    ['codart', 'CODART', c.str], ['codin', 'CODIN', c.str], ['qte', 'Qte', c.num], ['datemvt', 'DateMvt', c.ts], ['codesite', 'CodeSite', c.str],
  ], { minRatio: 0 }),
  full('inv_releve_detail', `SELECT IdReleve, NUMLIGNE, CODIN, CODART, Qte FROM InvReleveDetail`, ['idreleve', 'numligne'], [
    ['idreleve', 'IdReleve', c.id], ['numligne', 'NUMLIGNE', c.int], ['codin', 'CODIN', c.str], ['codart', 'CODART', c.str], ['qte', 'Qte', c.num],
  ], { minRatio: 0 }),
];

// ============================================================
// Commandes clients & web (colonnes nominatives volontairement exclues)
// ============================================================

const CLIENTS_WEB = [
  full('commande_client', `
    SELECT IDCDECLI, NUMCDECLI, TYPCDECLI, TYPLIV, DATCDECLI, ETATCDECLI, DATLIVSOU,
           MNTHTBRUTCDECLI, MNTREMHTCDECLI, MNTHTNETCDECLI, MNTTTCCDECLI, MNT_A_ENCAISSER,
           MNT_ENCAISSE, ACOMPTE_PC, DDISPOSITION, IDVENDEUR, ETAT_SECOND, CDECLITOT, CDECLITARD, DCRE, DMOD
    FROM COMMANDECLIENT`, ['idcdecli'], [
    ['idcdecli', 'IDCDECLI', c.id], ['numcdecli', 'NUMCDECLI', c.str], ['typcdecli', 'TYPCDECLI', c.int], ['typliv', 'TYPLIV', c.int],
    ['datcdecli', 'DATCDECLI', c.ts], ['etatcdecli', 'ETATCDECLI', c.int], ['datlivsou', 'DATLIVSOU', c.ts],
    ['mnthtbrut', 'MNTHTBRUTCDECLI', c.num], ['mntremht', 'MNTREMHTCDECLI', c.num], ['mnthtnet', 'MNTHTNETCDECLI', c.num],
    ['mntttc', 'MNTTTCCDECLI', c.num], ['mnt_a_encaisser', 'MNT_A_ENCAISSER', c.num], ['mnt_encaisse', 'MNT_ENCAISSE', c.num],
    ['acompte_pc', 'ACOMPTE_PC', c.num], ['ddisposition', 'DDISPOSITION', c.ts], ['idvendeur', 'IDVENDEUR', c.id],
    ['etat_second', 'ETAT_SECOND', c.str], ['cdeclitot', 'CDECLITOT', c.ts], ['cdeclitard', 'CDECLITARD', c.ts],
    ['dcre', 'DCRE', c.ts], ['dmod', 'DMOD', c.ts],
  ]),
  full('commande_client_ligne', `
    SELECT IDCDECLILIGNE, IDCDECLI, IDARTICLE, IDARTFOU1, NLCDECLILIG, REFCDECLILIG, EANCDECLILIG, LIB1CDECLILIG,
           QTECDECLILIG, PUTTCNETCDECLILIG, MNTHTNETCDECLILIG, MNTTTCNETCDECLILIG, ETATCLILIG, DPAIEMENT, DLIVRAISON,
           CDECLILIGTOT, CDECLILIGTARD, DATECLIENTINFORME
    FROM CDECLILIGNE`, ['idcdeclilig'], [
    ['idcdeclilig', 'IDCDECLILIGNE', c.id], ['idcdecli', 'IDCDECLI', c.id], ['idarticle', 'IDARTICLE', c.id], ['idartfou1', 'IDARTFOU1', c.id],
    ['nlcdeclilig', 'NLCDECLILIG', c.int], ['refcdeclilig', 'REFCDECLILIG', c.str], ['eancdeclilig', 'EANCDECLILIG', c.str],
    ['lib1cdeclilig', 'LIB1CDECLILIG', c.str], ['qtecdeclilig', 'QTECDECLILIG', c.num], ['puttcnetcdeclilig', 'PUTTCNETCDECLILIG', c.num],
    ['mnthtnetcdeclilig', 'MNTHTNETCDECLILIG', c.num], ['mntttcnetcdeclilig', 'MNTTTCNETCDECLILIG', c.num], ['etatclilig', 'ETATCLILIG', c.int],
    ['dpaiement', 'DPAIEMENT', c.ts], ['dlivraison', 'DLIVRAISON', c.ts], ['cdeclilig_tot', 'CDECLILIGTOT', c.ts],
    ['cdeclilig_tard', 'CDECLILIGTARD', c.ts], ['dateclientinforme', 'DATECLIENTINFORME', c.ts],
  ]),
  full('web_commande', `SELECT NumCommande, DateRecup, DateTraitement, Deliver, Status FROM SoClozWebCommande`, ['numcommande'], [
    ['numcommande', 'NumCommande', c.str], ['daterecup', 'DateRecup', c.ts], ['datetraitement', 'DateTraitement', c.ts],
    ['deliver', 'Deliver', c.ts], ['status', 'Status', c.str],
  ], { filter: (o) => o.numcommande != null }),
  full('art_web_stock', `SELECT idArticle, idMag, QteStock, QteEnCde, PvMag, InterditVte, DateMaj FROM ArtWebStock`, ['idarticle', 'idmag'], [
    ['idarticle', 'idArticle', c.str], ['idmag', 'idMag', c.site], ['qtestock', 'QteStock', c.num], ['qteencde', 'QteEnCde', c.num],
    ['pvmag', 'PvMag', c.num], ['interditvte', 'InterditVte', c.bool], ['datemaj', 'DateMaj', c.ts],
  ], { filter: (o) => o.idarticle != null && o.idmag != null }),
];

module.exports = {
  MVTART, MVTREG, STATOPCA, MVTART_MOTIF, FIDELITE_JOUR, HISTO_ART_GAMME,
  REFERENTIELS, BUDGETS, METEO, DEMARQUES, ARTICLES_PLUS, PROMOS,
  FOURNISSEURS_PLUS, ROTATION, INVENTAIRES, CLIENTS_WEB,
};
