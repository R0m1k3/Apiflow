const { getMssql, getPg } = require('../db');
const { batchUpsert, fullRefresh, getLastSync, logSync, safeStr, safeDecimal } = require('../utils');

const COMMANDE_FOU_COLS = [
  'no_id','cdenum','sit_cod_emet','sit_cod_desti','fouident_code',
  'foutarif_no_id','port_code','cdedate','cdeetat','cdetot',
  'cdetard','cdeportmontant','cdeannulation','suividatecreation','suividatemodif',
];

const CDEFOU_LIGNE_COLS = [
  'no_id','artfou1_no_id','commande_fou_no_id','cdelig','cdeligtard','cdeligtot',
  'prixbrut','remise','prixnet','qtecde','gratuit','montant',
  'qteacc','qteann','qteatt','qterel','colisage','suividatecreation','suividatemodif',
];

const COMMANDE_AUTO_COLS = [
  'fou_code','art_no_id','codesite','qtepropo','parametre_numero','suividatecreation','suividatemodif',
];

const FOUCAD_COLS = [
  'foucode','franco','actif','duree','unite','dateref','datecom',
  'montantcdeauto','datecdeauto','suividatecreation','suividatemodif',
];

const PLAN_REAPPRO_COLS = [
  'id','magasin','codein','bu_no_id','stockmag','encours_palette_mag',
  'attente_prepa_mag','colis_rea','colis_ajout_web','stock_dispo',
  'attente_prepa_depot','stock_dispo_calcul','colisage','bloque','date_besoin',
];

async function syncAppro(force) {
  const ms = await getMssql();
  const pg  = getPg();

  // === COMMANDE_FOU (upsert delta) ===
  try {
    const lastSync = force ? null : await getLastSync(pg, 'commande_fou');
    const where    = lastSync ? `WHERE SUIVIDATEMODIF > '${lastSync.toISOString()}'` : '';

    const res = await ms.request().query(`
      SELECT NO_ID, CDENUM, SIT_COD_EMET, SIT_COD_DESTI, FOUIDENT_CODE,
             FOUTARIF_NO_ID, PORT_CODE, CDEDATE, CDEETAT, CDETOT,
             CDETARD, CDEPORTMONTANT, CDEANNULATION, SUIVIDATECREATION, SUIVIDATEMODIF
      FROM COMMANDE_FOU ${where}
    `);

    const rows = res.recordset.map(r => ({
      no_id:            r.NO_ID,
      cdenum:           r.CDENUM,
      sit_cod_emet:     r.SIT_COD_EMET,
      sit_cod_desti:    r.SIT_COD_DESTI,
      fouident_code:    r.FOUIDENT_CODE,
      foutarif_no_id:   r.FOUTARIF_NO_ID ?? null,
      port_code:        r.PORT_CODE,
      cdedate:          r.CDEDATE,
      cdeetat:          r.CDEETAT,
      cdetot:           safeDecimal(r.CDETOT),
      cdetard:          r.CDETARD ?? null,
      cdeportmontant:   safeDecimal(r.CDEPORTMONTANT),
      cdeannulation:    r.CDEANNULATION ?? null,
      suividatecreation: r.SUIVIDATECREATION,
      suividatemodif:   r.SUIVIDATEMODIF,
    }));

    const count = await batchUpsert(pg, 'commande_fou', rows, ['no_id'], COMMANDE_FOU_COLS);
    await logSync(pg, 'commande_fou', count, 'ok');
    console.log(`[commande_fou] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'commande_fou', 0, 'error', err.message);
    console.error(`[commande_fou] ERREUR: ${err.message}`);
  }

  // === CDEFOU_LIGNE (upsert delta) ===
  try {
    const lastSync = force ? null : await getLastSync(pg, 'cdefou_ligne');
    const where    = lastSync ? `WHERE SUIVIDATEMODIF > '${lastSync.toISOString()}'` : '';

    const res = await ms.request().query(`
      SELECT NO_ID, ARTFOU1_NO_ID, COMMANDE_FOU_NO_ID, CDELIG, CDELIGTARD, CDELIGTOT,
             PRIXBRUT, REMISE, PRIXNET, QTECDE, GRATUIT, MONTANT,
             QTEACC, QTEANN, QTEATT, QTEREL, COLISAGE, SUIVIDATECREATION, SUIVIDATEMODIF
      FROM CDEFOU_LIGNE ${where}
    `);

    const rows = res.recordset.map(r => ({
      no_id:               r.NO_ID,
      artfou1_no_id:       r.ARTFOU1_NO_ID,
      commande_fou_no_id:  r.COMMANDE_FOU_NO_ID,
      cdelig:              safeStr(r.CDELIG),
      cdeligtard:          r.CDELIGTARD ?? null,
      cdeligtot:           r.CDELIGTOT ?? null,
      prixbrut:            r.PRIXBRUT ?? null,
      remise:              r.REMISE ?? null,
      prixnet:             r.PRIXNET ?? null,
      qtecde:              r.QTECDE ?? null,
      gratuit:             r.GRATUIT ?? null,
      montant:             r.MONTANT ?? null,
      qteacc:              r.QTEACC ?? null,
      qteann:              r.QTEANN ?? null,
      qteatt:              r.QTEATT ?? null,
      qterel:              r.QTEREL ?? null,
      colisage:            safeStr(r.COLISAGE),
      suividatecreation:   r.SUIVIDATECREATION,
      suividatemodif:      r.SUIVIDATEMODIF,
    }));

    const count = await batchUpsert(pg, 'cdefou_ligne', rows, ['no_id'], CDEFOU_LIGNE_COLS);
    await logSync(pg, 'cdefou_ligne', count, 'ok');
    console.log(`[cdefou_ligne] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'cdefou_ligne', 0, 'error', err.message);
    console.error(`[cdefou_ligne] ERREUR: ${err.message}`);
  }

  // === COMMANDE_AUTO_QTEPROPO (full refresh — état courant) ===
  try {
    const res = await ms.request().query(`
      SELECT FOU_CODE, ART_NO_ID, CODESITE, QTEPROPO,
             PARAMETRE_NUMERO, SUIVIDATECREATION, SUIVIDATEMODIF
      FROM COMMANDE_AUTO_QTEPROPO
    `);

    const rows = res.recordset.map(r => ({
      fou_code:          r.FOU_CODE,
      art_no_id:         r.ART_NO_ID,
      codesite:          r.CODESITE,
      qtepropo:          r.QTEPROPO ?? null,
      parametre_numero:  r.PARAMETRE_NUMERO ?? null,
      suividatecreation: r.SUIVIDATECREATION,
      suividatemodif:    r.SUIVIDATEMODIF,
    }));

    const count = await fullRefresh(pg, 'commande_auto_qtepropo', rows, COMMANDE_AUTO_COLS);
    await logSync(pg, 'commande_auto_qtepropo', count, 'ok');
    console.log(`[commande_auto_qtepropo] ${count} lignes`);
  } catch (err) {
    await logSync(pg, 'commande_auto_qtepropo', 0, 'error', err.message);
    console.error(`[commande_auto_qtepropo] ERREUR: ${err.message}`);
  }

  // === FOUCAD (full refresh — config commande auto par fournisseur) ===
  try {
    const res = await ms.request().query(`
      SELECT FOUCODE, FRANCO, ACTIF, DUREE, UNITE, DATEREF, DATECOM,
             MONTANTCDEAUTO, DATECDEAUTO, SUIVIDATECREATION, SUIVIDATEMODIF
      FROM FOUCAD
    `);

    const rows = res.recordset.map(r => ({
      foucode:           r.FOUCODE,
      franco:            r.FRANCO ?? null,
      actif:             r.ACTIF ?? null,
      duree:             r.DUREE ?? null,
      unite:             r.UNITE ?? null,
      dateref:           r.DATEREF ?? null,
      datecom:           r.DATECOM ?? null,
      montantcdeauto:    r.MONTANTCDEAUTO ?? null,
      datecdeauto:       r.DATECDEAUTO ?? null,
      suividatecreation: r.SUIVIDATECREATION,
      suividatemodif:    r.SUIVIDATEMODIF,
    }));

    const count = await fullRefresh(pg, 'foucad', rows, FOUCAD_COLS);
    await logSync(pg, 'foucad', count, 'ok');
    console.log(`[foucad] ${count} lignes`);
  } catch (err) {
    await logSync(pg, 'foucad', 0, 'error', err.message);
    console.error(`[foucad] ERREUR: ${err.message}`);
  }

  // === PLAN_REAPPRO (full refresh — état courant) ===
  try {
    const res = await ms.request().query(`
      SELECT id, Magasin, Codein, BU_no_id, StockMag, EncoursPaletteMag,
             AttentePrepaMag, ColisRea, ColisAjoutWeb, StockDispo,
             AttentePrepaDepot, StockDispoCalcul, Colisage,
             [Bloqué] AS bloque, DateBesoin
      FROM PLAN_REAPPRO
    `);

    const rows = res.recordset.map(r => ({
      id:                   r.id,
      magasin:              r.Magasin,
      codein:               r.Codein,
      bu_no_id:             r.BU_no_id ?? null,
      stockmag:             r.StockMag ?? null,
      encours_palette_mag:  r.EncoursPaletteMag ?? null,
      attente_prepa_mag:    r.AttentePrepaMag ?? null,
      colis_rea:            r.ColisRea ?? null,
      colis_ajout_web:      r.ColisAjoutWeb ?? null,
      stock_dispo:          r.StockDispo ?? null,
      attente_prepa_depot:  r.AttentePrepaDepot ?? null,
      stock_dispo_calcul:   r.StockDispoCalcul ?? null,
      colisage:             r.Colisage ?? null,
      bloque:               r.bloque ?? null,
      date_besoin:          r.DateBesoin ?? null,
    }));

    const count = await fullRefresh(pg, 'plan_reappro', rows, PLAN_REAPPRO_COLS);
    await logSync(pg, 'plan_reappro', count, 'ok');
    console.log(`[plan_reappro] ${count} lignes`);
  } catch (err) {
    await logSync(pg, 'plan_reappro', 0, 'error', err.message);
    console.error(`[plan_reappro] ERREUR: ${err.message}`);
  }
}

module.exports = { syncAppro };
