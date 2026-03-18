const { getMssql, getPg } = require('../db');
const { fullRefresh, batchUpsert, logSync } = require('../utils');

const NOMEN_COLS  = ['no_id','code','libelle','niveau','chemin_pere'];
const GAMME_COLS  = ['no_id','code','libelle'];
const SAISON_COLS = ['no_id','code','libelle'];
const AGS_COLS    = ['artnoid','idgamme','idsaison'];

async function syncReferentiel(force) {
  const ms = await getMssql();
  const pg  = getPg();

  // === NOMENCLATURE (full refresh) ===
  try {
    const res = await ms.request().query(
      `SELECT NO_ID, CODE, LIBELLE, NIVEAU, CHEMIN_PERE FROM NOMENCLATURE`
    );
    const rows = res.recordset.map(r => ({
      no_id:       r.NO_ID,
      code:        r.CODE,
      libelle:     r.LIBELLE,
      niveau:      r.NIVEAU,
      chemin_pere: r.CHEMIN_PERE,
    }));
    const count = await batchUpsert(pg, 'nomenclature', rows, ['no_id'], NOMEN_COLS);
    await logSync(pg, 'nomenclature', count, 'ok');
    console.log(`[nomenclature] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'nomenclature', 0, 'error', err.message);
    console.error(`[nomenclature] ERREUR: ${err.message}`);
  }

  // === GAMMES (full refresh) ===
  try {
    const res = await ms.request().query(
      `SELECT NO_ID, CODE, LIBELLE FROM GAMMES`
    );
    const rows = res.recordset.map(r => ({
      no_id:   r.NO_ID,
      code:    r.CODE,
      libelle: r.LIBELLE,
    }));
    const count = await batchUpsert(pg, 'gammes', rows, ['no_id'], GAMME_COLS);
    await logSync(pg, 'gammes', count, 'ok');
    console.log(`[gammes] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'gammes', 0, 'error', err.message);
    console.error(`[gammes] ERREUR: ${err.message}`);
  }

  // === SAISONS (full refresh) ===
  try {
    const res = await ms.request().query(
      `SELECT NO_ID, CODE, LIBELLE FROM SAISONS`
    );
    const rows = res.recordset.map(r => ({
      no_id:   r.NO_ID,
      code:    r.CODE,
      libelle: r.LIBELLE,
    }));
    const count = await batchUpsert(pg, 'saisons', rows, ['no_id'], SAISON_COLS);
    await logSync(pg, 'saisons', count, 'ok');
    console.log(`[saisons] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'saisons', 0, 'error', err.message);
    console.error(`[saisons] ERREUR: ${err.message}`);
  }

  // === ART_GAMME_SAISON (full refresh) ===
  try {
    const res = await ms.request().query(
      `SELECT ARTNOID, IDGAMME, IDSAISON FROM ART_GAMME_SAISON`
    );
    const rows = res.recordset.map(r => ({
      artnoid:  r.ARTNOID,
      idgamme:  r.IDGAMME,
      idsaison: r.IDSAISON,
    }));
    const count = await fullRefresh(pg, 'art_gamme_saison', rows, AGS_COLS);
    await logSync(pg, 'art_gamme_saison', count, 'ok');
    console.log(`[art_gamme_saison] ${count} lignes refresh`);
  } catch (err) {
    await logSync(pg, 'art_gamme_saison', 0, 'error', err.message);
    console.error(`[art_gamme_saison] ERREUR: ${err.message}`);
  }
}

module.exports = { syncReferentiel };
