const { getMssql, getPg } = require('../db');
const { fullRefresh, batchUpsert, logSync, safeNum, safeInt, safeStr } = require('../utils');

const STOCK_COLS = [
  'artnoid','site','qte','prmp','valstock','pv',
  'stockdispo','stockmort','stockcolis',
  'dernierevente','dernierereception','premierevente',
  'nbjoursdernierMouvement','nbjoursdernierevente','nbjoursdernierrereception',
  'interditachat','codefou',
];
const PA_COLS = ['artnoid','pa'];
const PV_COLS = ['artnoid','site','pv'];

async function syncStock(force) {
  const ms = await getMssql();
  const pg  = getPg();

  // === Cube_Stock (full refresh — recalculé chaque nuit) ===
  try {
    const res = await ms.request().query(`
      SELECT ArtNoId, Site, QTE, Prmp, ValStock, PV,
             StockDispo, StockMort, StockColis,
             DerniereVente, DerniereReception, PremiereVente,
             NbJoursDernierMouvement, NbJoursDerniereVente,
             NbJoursDerniereReception, InterditAchat, CODEFOU
      FROM Cube_Stock
    `);

    const rows = res.recordset.map(r => ({
      artnoid:                   r.ArtNoId,
      site:                      safeStr(r.Site),
      qte:                       safeNum(r.QTE),
      prmp:                      safeNum(r.Prmp),
      valstock:                  safeNum(r.ValStock),
      pv:                        safeNum(r.PV),
      stockdispo:                safeNum(r.StockDispo),
      stockmort:                 safeNum(r.StockMort),
      stockcolis:                safeNum(r.StockColis),
      dernierevente:             r.DerniereVente,
      dernierereception:         r.DerniereReception,
      premierevente:             r.PremiereVente,
      nbjoursdernierMouvement:   safeInt(r.NbJoursDernierMouvement),
      nbjoursdernierevente:      safeInt(r.NbJoursDerniereVente),
      nbjoursdernierrereception: safeInt(r.NbJoursDerniereReception),
      interditachat:             safeStr(r.InterditAchat),
      codefou:                   safeStr(r.CODEFOU),
    }));

    const count = await fullRefresh(pg, 'cube_stock', rows, STOCK_COLS);
    await logSync(pg, 'cube_stock', count, 'ok');
    console.log(`[cube_stock] ${count} lignes refresh`);
  } catch (err) {
    await logSync(pg, 'cube_stock', 0, 'error', err.message);
    console.error(`[cube_stock] ERREUR: ${err.message}`);
  }

  // === Cube_PA (full refresh) ===
  try {
    const res = await ms.request().query(`SELECT ArtNoId, PA FROM Cube_PA`);
    const rows = res.recordset.map(r => ({ artnoid: r.ArtNoId, pa: r.PA }));
    const count = await fullRefresh(pg, 'cube_pa', rows, PA_COLS);
    await logSync(pg, 'cube_pa', count, 'ok');
    console.log(`[cube_pa] ${count} lignes refresh`);
  } catch (err) {
    await logSync(pg, 'cube_pa', 0, 'error', err.message);
    console.error(`[cube_pa] ERREUR: ${err.message}`);
  }

  // === Cube_PV (full refresh) ===
  try {
    const res = await ms.request().query(`SELECT ArtNoId, Site, PV FROM Cube_PV`);
    const rows = res.recordset.map(r => ({
      artnoid: r.ArtNoId, site: r.Site, pv: r.PV,
    }));
    const count = await batchUpsert(pg, 'cube_pv', rows, ['artnoid','site'], PV_COLS);
    await logSync(pg, 'cube_pv', count, 'ok');
    console.log(`[cube_pv] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'cube_pv', 0, 'error', err.message);
    console.error(`[cube_pv] ERREUR: ${err.message}`);
  }
}

module.exports = { syncStock };
