const { getMssql, getPg } = require('../db');
const { batchUpsert, logSync, safeStr, safeNum, safeDecimal } = require('../utils');

const COLS = [
  'gencod', 'site', 'libelle', 'foucentrale', 'nomfoucentrale',
  'ranking_ca', 'ranking_qte', 'ranking_mag_ca', 'ranking_mag_qte', 'ranking_mag_marge',
  'pv_calcule', 'pv_mag', 'pv_cen',
  'codefamille', 'libellefamille', 'fichier',
  'date_maj', 'date_integration', 'date_calcul_mag',
];

async function syncRanking(force) {
  const ms = await getMssql();
  const pg = getPg();

  try {
    const res = await ms.request().query(`
      SELECT GENCOD, SITE, LIBELLE, FOUCENTRALE, NOMFOUCENTRALE,
             RANKING_CA, RANKING_QTE, RANKING_MAG_CA, RANKING_MAG_QTE, RANKING_MAG_MARGE,
             PV_CALCULE, PV_MAG, PV_CEN,
             CODEFAMILLE, LIBELLEFAMILLE, FICHIER,
             DATE_MAJ, DATE_INTEGRATION, DATE_CALCUL_MAG
      FROM Ranking
      WHERE GENCOD IS NOT NULL
    `);

    const rows = res.recordset.map(r => ({
      gencod:            safeStr(r.GENCOD),
      site:              safeStr(r.SITE) || '000',
      libelle:           safeStr(r.LIBELLE),
      foucentrale:       safeStr(r.FOUCENTRALE),
      nomfoucentrale:    safeStr(r.NOMFOUCENTRALE),
      ranking_ca:        safeNum(r.RANKING_CA),
      ranking_qte:       safeNum(r.RANKING_QTE),
      ranking_mag_ca:    safeNum(r.RANKING_MAG_CA),
      ranking_mag_qte:   safeNum(r.RANKING_MAG_QTE),
      ranking_mag_marge: safeNum(r.RANKING_MAG_MARGE),
      pv_calcule:        safeDecimal(r.PV_CALCULE),
      pv_mag:            safeDecimal(r.PV_MAG),
      pv_cen:            safeDecimal(r.PV_CEN),
      codefamille:       safeStr(r.CODEFAMILLE),
      libellefamille:    safeStr(r.LIBELLEFAMILLE),
      fichier:           safeStr(r.FICHIER),
      date_maj:          r.DATE_MAJ || null,
      date_integration:  r.DATE_INTEGRATION || null,
      date_calcul_mag:   r.DATE_CALCUL_MAG || null,
    }));

    const count = await batchUpsert(pg, 'ranking', rows, ['gencod', 'site'], COLS);
    await logSync(pg, 'ranking', count, 'ok');
    console.log(`[ranking] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'ranking', 0, 'error', err.message);
    console.error(`[ranking] ERREUR: ${err.message}`);
  }
}

module.exports = { syncRanking };
