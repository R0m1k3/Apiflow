const { getMssql, getPg } = require('../db');
const { batchUpsert, fullRefresh, getLastSync, logSync, safeDecimal } = require('../utils');

const ARTFOU1_COLS = [
  'no_id','art_no_id','code','reference','ean13','itf',
  'qteua','pcb','spcb','delai','securite','preference','suspendu',
  'suividatecreation','suividatemodif',
];
const ARTFOU2_COLS = ['idartfou1','prixachat','remise_promotion'];
const FOUADR1_COLS = ['code','sit_code','raisonsociale','adrligne1','telephone','email'];

async function syncFournisseurs(force) {
  const ms = await getMssql();
  const pg  = getPg();

  // === ARTFOU1 (delta par SUIVIDATEMODIF) ===
  try {
    const lastSync = force ? null : await getLastSync(pg, 'artfou1');
    const where    = lastSync ? `WHERE SUIVIDATEMODIF > '${lastSync.toISOString()}'` : '';

    const res = await ms.request().query(`
      SELECT NO_ID, ART_NO_ID, CODE, REFERENCE, EAN13, ITF,
             QTEUA, PCB, SPCB, DELAI, SECURITE, PREFERENCE, SUSPENDU,
             SUIVIDATECREATION, SUIVIDATEMODIF
      FROM ARTFOU1 ${where}
    `);

    const rows = res.recordset.map(r => ({
      no_id:             r.NO_ID,
      art_no_id:         r.ART_NO_ID,
      code:              r.CODE,
      reference:         r.REFERENCE,
      ean13:             r.EAN13,
      itf:               r.ITF,
      qteua:             r.QTEUA,
      pcb:               r.PCB,
      spcb:              r.SPCB,
      delai:             r.DELAI,
      securite:          r.SECURITE,
      preference:        r.PREFERENCE,
      suspendu:          r.SUSPENDU,
      suividatecreation: r.SUIVIDATECREATION,
      suividatemodif:    r.SUIVIDATEMODIF,
    }));

    const count = await batchUpsert(pg, 'artfou1', rows, ['no_id'], ARTFOU1_COLS);
    await logSync(pg, 'artfou1', count, 'ok');
    console.log(`[artfou1] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'artfou1', 0, 'error', err.message);
    console.error(`[artfou1] ERREUR: ${err.message}`);
  }

  // === ARTFOU2 (full refresh — liée à ARTFOU1) ===
  try {
    const res = await ms.request().query(
      `SELECT IDARTFOU1, PRIXACHAT, REMISE_PROMOTION FROM ARTFOU2`
    );
    const rows = res.recordset.map(r => ({
      idartfou1:        r.IDARTFOU1,
      prixachat:        safeDecimal(r.PRIXACHAT),
      remise_promotion: safeDecimal(r.REMISE_PROMOTION),
    }));
    const count = await batchUpsert(pg, 'artfou2', rows, ['idartfou1'], ARTFOU2_COLS);
    await logSync(pg, 'artfou2', count, 'ok');
    console.log(`[artfou2] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'artfou2', 0, 'error', err.message);
    console.error(`[artfou2] ERREUR: ${err.message}`);
  }

  // === FOUADR1 (full refresh — ~500 lignes) ===
  try {
    const res = await ms.request().query(
      `SELECT CODE, SIT_CODE, RAISONSOCIALE, ADRLIGNE1, TELEPHONE, EMAIL FROM FOUADR1`
    );
    const rows = res.recordset.map(r => ({
      code:          r.CODE,
      sit_code:      r.SIT_CODE,
      raisonsociale: r.RAISONSOCIALE,
      adrligne1:     r.ADRLIGNE1,
      telephone:     r.TELEPHONE,
      email:         r.EMAIL,
    }));
    const count = await fullRefresh(pg, 'fouadr1', rows, FOUADR1_COLS);
    await logSync(pg, 'fouadr1', count, 'ok');
    console.log(`[fouadr1] ${count} lignes refresh`);
  } catch (err) {
    await logSync(pg, 'fouadr1', 0, 'error', err.message);
    console.error(`[fouadr1] ERREUR: ${err.message}`);
  }
}

module.exports = { syncFournisseurs };
