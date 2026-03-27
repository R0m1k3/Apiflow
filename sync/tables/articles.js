const { getMssql, getPg } = require('../db');
const { batchUpsert, getLastSync, logSync, safeStr, safeBit } = require('../utils');

const ARTICLES_COLS = [
  'no_id','codein','libelle1','libelle2','lib_ticket',
  'tax_code','ach_code','utilisable','actif','suspendu',
  'suividatecreation','suividatemodif','nom_no_id','artcentrale',
];
const INFOSUP_COLS = [
  'artnoid','prix_vente_mini','prix_vente_maxi','eco_ht','eco_ttc',
  'on_web','interdit_remise','nomphoto','datedebvente','datefinvente',
  'datevaliditeachat','commentaire','pv_conseille','fraislogistic',
  'transit','distribution',
];
const GTIN_COLS = ['idarticle','gtin','preferentiel'];

async function syncArticles(force) {
  const ms = await getMssql();
  const pg  = getPg();

  // === ARTICLES ===
  try {
    const lastSync = force ? null : await getLastSync(pg, 'articles');
    const where    = lastSync ? `WHERE SUIVIDATEMODIF > '${lastSync.toISOString()}'` : '';

    const res = await ms.request().query(`
      SELECT NO_ID, CODEIN, LIBELLE1, LIBELLE2, LIB_TICKET,
             TAX_CODE, ACH_CODE, UTILISABLE, ACTIF, SUSPENDU,
             SUIVIDATECREATION, SUIVIDATEMODIF, NOM_NO_ID, ARTCENTRALE
      FROM ARTICLES ${where}
    `);

    const rows = res.recordset.map(r => ({
      no_id:             r.NO_ID,
      codein:            safeStr(r.CODEIN),
      libelle1:          safeStr(r.LIBELLE1),
      libelle2:          safeStr(r.LIBELLE2),
      lib_ticket:        safeStr(r.LIB_TICKET),
      tax_code:          safeStr(r.TAX_CODE),
      ach_code:          safeStr(r.ACH_CODE),
      utilisable:        safeStr(r.UTILISABLE),
      actif:             safeStr(r.ACTIF),
      suspendu:          safeStr(r.SUSPENDU),
      suividatecreation: r.SUIVIDATECREATION,
      suividatemodif:    r.SUIVIDATEMODIF,
      nom_no_id:         r.NOM_NO_ID,
      artcentrale:       r.ARTCENTRALE ?? null,
    }));

    const count = await batchUpsert(pg, 'articles', rows, ['no_id'], ARTICLES_COLS);
    await logSync(pg, 'articles', count, 'ok');
    console.log(`[articles] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'articles', 0, 'error', err.message);
    console.error(`[articles] ERREUR: ${err.message}`);
  }

  // === ARTICLE_INFOSUP ===
  try {
    const lastSync = force ? null : await getLastSync(pg, 'article_infosup');
    const where    = lastSync
      ? `WHERE ai.ARTNOID IN (SELECT NO_ID FROM ARTICLES WHERE SUIVIDATEMODIF > '${lastSync.toISOString()}')`
      : '';

    const res = await ms.request().query(`
      SELECT ARTNOID, PRIX_VENTE_MINI, PRIX_VENTE_MAXI, ECO_HT, ECO_TTC,
             ON_WEB, INTERDIT_REMISE, NOMPHOTO, DATEDEBVENTE, DATEFINVENTE,
             DATEVALIDITEACHAT, COMMENTAIRE, PV_CONSEILLE, FRAISLOGISTIC,
             TRANSIT, DISTRIBUTION
      FROM ARTICLE_INFOSUP ai ${where}
    `);

    const rows = res.recordset.map(r => ({
      artnoid:           r.ARTNOID,
      prix_vente_mini:   r.PRIX_VENTE_MINI,
      prix_vente_maxi:   r.PRIX_VENTE_MAXI,
      eco_ht:            r.ECO_HT,
      eco_ttc:           r.ECO_TTC,
      on_web:            r.ON_WEB,
      interdit_remise:   r.INTERDIT_REMISE,
      nomphoto:          r.NOMPHOTO,
      datedebvente:      r.DATEDEBVENTE,
      datefinvente:      r.DATEFINVENTE,
      datevaliditeachat: r.DATEVALIDITEACHAT,
      commentaire:       r.COMMENTAIRE,
      pv_conseille:      r.PV_CONSEILLE,
      fraislogistic:     r.FRAISLOGISTIC,
      transit:           r.TRANSIT,
      distribution:      r.DISTRIBUTION,
    }));

    const count = await batchUpsert(pg, 'article_infosup', rows, ['artnoid'], INFOSUP_COLS);
    await logSync(pg, 'article_infosup', count, 'ok');
    console.log(`[article_infosup] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'article_infosup', 0, 'error', err.message);
    console.error(`[article_infosup] ERREUR: ${err.message}`);
  }

  // === ART_GTIN (full refresh — petite table) ===
  try {
    const res = await ms.request().query(
      `SELECT IDARTICLE, GTIN, PREFERENTIEL FROM ART_GTIN`
    );
    const rows = res.recordset.map(r => ({
      idarticle:    r.IDARTICLE,
      gtin:         safeStr(r.GTIN),
      preferentiel: safeBit(r.PREFERENTIEL),
    }));
    const count = await batchUpsert(pg, 'art_gtin', rows, ['idarticle','gtin'], GTIN_COLS);
    await logSync(pg, 'art_gtin', count, 'ok');
    console.log(`[art_gtin] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'art_gtin', 0, 'error', err.message);
    console.error(`[art_gtin] ERREUR: ${err.message}`);
  }
}

module.exports = { syncArticles };
