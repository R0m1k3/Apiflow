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

// ARTFOU2.PRIXACHAT empile l'historique des prix d'un fournisseur sous forme
// "[01/01/1901,0.990:16/05/2018,1.010]". Le dernier couple porte le prix courant
// et la date à laquelle il a été appliqué.
function lastPriceChange(s) {
  if (!s) return null;
  const body = String(s).trim().replace(/^\[/, '').replace(/\]$/, '');
  if (!body) return null;
  const pairs = body.split(':');
  const [d, p] = pairs[pairs.length - 1].split(',');
  const m = d && d.match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
  const price = parseFloat(p);
  if (!m || Number.isNaN(price)) return null;
  return { date: Date.UTC(+m[3], +m[2] - 1, +m[1]), price };
}

// Pour chaque artnoid en doublon, retrouve le prix dont le changement est le plus
// récent. Égalité de date de prix → on départage sur SUIVIDATEMODIF de la ligne.
async function resolveDuplicatePa(ms, artnoids) {
  const ids = artnoids.map(Number).filter(Number.isFinite);
  if (!ids.length) return new Map();

  const res = await ms.request().query(`
    SELECT f1.ART_NO_ID, f2.PRIXACHAT, f2.SUIVIDATEMODIF
    FROM ARTFOU1 f1
    JOIN ARTFOU2 f2 ON f2.IDARTFOU1 = f1.NO_ID
    WHERE f1.ART_NO_ID IN (${ids.join(',')})
  `);

  const best = new Map();
  for (const r of res.recordset) {
    const lp = lastPriceChange(r.PRIXACHAT);
    if (!lp) continue;
    const key   = String(r.ART_NO_ID);
    const modif = r.SUIVIDATEMODIF ? new Date(r.SUIVIDATEMODIF).getTime() : 0;
    const prev  = best.get(key);
    if (!prev || lp.date > prev.date || (lp.date === prev.date && modif > prev.modif)) {
      best.set(key, { date: lp.date, price: lp.price, modif });
    }
  }
  return best;
}

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
    const raw = res.recordset.map(r => ({ artnoid: r.ArtNoId, pa: r.PA }));

    // artnoid est PK côté PostgreSQL, mais le cube peut sortir deux lignes pour
    // un article livré par deux fournisseurs → l'INSERT violerait cube_pa_pkey.
    const groups = new Map();
    for (const r of raw) {
      const key = String(r.artnoid);
      const g = groups.get(key);
      if (g) g.push(r); else groups.set(key, [r]);
    }
    const dupIds = [...groups].filter(([, g]) => g.length > 1).map(([id]) => id);

    let rows = raw;
    if (dupIds.length) {
      const best = await resolveDuplicatePa(ms, dupIds);
      rows = [];
      for (const [key, g] of groups) {
        if (g.length === 1) { rows.push(g[0]); continue; }
        const b = best.get(key);
        let chosen = b && g.find(x => Math.abs(parseFloat(x.pa) - b.price) < 0.005);
        if (!chosen) {
          // Pas de prix fournisseur exploitable : on retient le PA le plus élevé,
          // qui sous-estime la marge plutôt que de l'inventer.
          chosen = g.reduce((a, x) => (parseFloat(x.pa) > parseFloat(a.pa) ? x : a));
          console.warn(`[cube_pa] artnoid ${key} non résolu → PA le plus élevé retenu (${chosen.pa})`);
        }
        rows.push(chosen);
      }
      console.log(`[cube_pa] ${dupIds.length} artnoid dédupliqués`);
    }

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
