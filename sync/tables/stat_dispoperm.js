const { getMssql, getPg } = require('../db');
const { batchUpsert, logSync } = require('../utils');

const COLS = ['codesite','code_art','recommand','statut','qtemoyer','qtemoyes','camoyes','nbreclient'];

async function syncStatDispoperm(force) {
  const ms = await getMssql();
  const pg  = getPg();

  try {
    const res = await ms.request().query(`
      SELECT CodeSite, Code_Art, Recommand, Statut,
             QteMoyAch, QteMoyRes, CaMoyRes, NbreClient
      FROM STAT_DISPOPERM
    `);

    const rows = res.recordset.map(r => ({
      codesite:   r.CodeSite,
      code_art:   r.Code_Art,
      recommand:  r.Recommand ?? null,
      statut:     r.Statut ?? null,
      qtemoyer:   r.QteMoyAch ?? null,
      qtemoyes:   r.QteMoyRes ?? null,
      camoyes:    r.CaMoyRes ?? null,
      nbreclient: r.NbreClient ?? null,
    }));

    const count = await batchUpsert(pg, 'stat_dispoperm', rows, ['codesite','code_art'], COLS);
    await logSync(pg, 'stat_dispoperm', count, 'ok');
    console.log(`[stat_dispoperm] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'stat_dispoperm', 0, 'error', err.message);
    console.error(`[stat_dispoperm] ERREUR: ${err.message}`);
  }
}

module.exports = { syncStatDispoperm };
