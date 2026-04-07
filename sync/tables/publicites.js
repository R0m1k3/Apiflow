const { getMssql, getPg } = require('../db');
const { logSync } = require('../utils');

// ============================================================
// pub_entetes — FBENTPUB (refresh complet)
// ============================================================
const ENTETES_COLS = ['ent_npub', 'ent_datdeb', 'ent_datfin', 'ent_titre'];

// ============================================================
// pub_ecoulement — ECOULEMENT (refresh complet)
// ============================================================
const ECOULEMENT_COLS = [
  'site', 'tcr_code', 'tcr_libelle', 'tcr_nature', 'tcr_type',
  'tcrd_datedeb', 'tcrd_datefin', 'datecalcul',
  'stock_datedebut', 'ca_total_periode_pub', 'ca_pub_periode_pub',
  'pourc_capub_catotal', 'qte_vendue_pub', 'client_total_periode',
  'client_pub_periode', 'stock_datefin',
  'ca_pub_30_jours', 'stock_30_jours',
  'ca_pub_60_jours', 'stock_60_jours',
  'ca_pub_90_jours', 'stock_90_jours',
  'ca_pub_180_jours', 'stock_180_jours',
  'ca_depuis_finpub', 'taux_sortie', 'marge', 'taux_marge',
];

// ============================================================
// pub_ecoulement_detail — ECOULEMENT_DETAIL (refresh complet)
// ============================================================
const DETAIL_COLS = [
  'site', 'tcr_code', 'artnoid', 'tcr_libelle',
  'tcrd_datedeb', 'tcrd_datefin', 'datecalcul',
  'ca_pub_periode_pub', 'qte_vendue_pub',
  'stock_datedebut', 'stock_datefin',
  'ca_pub_30_jours', 'ca_pub_60_jours', 'ca_pub_90_jours', 'ca_pub_180_jours',
  'ca_depuis_finpub', 'taux_sortie', 'marge', 'taux_marge',
  'codein', 'libelle', 'prmp', 'pa', 'pv', 'prixpub',
];

const CHUNK = 500;

async function insertChunk(pg, table, rows, cols, conflictTarget) {
  if (!rows.length) return;
  const values = [];
  const placeholders = rows.map((row, ri) => {
    const ph = cols.map((col, ci) => {
      values.push(row[col] ?? null);
      return `$${ri * cols.length + ci + 1}`;
    });
    return `(${ph.join(', ')})`;
  });
  const colList = cols.join(', ');
  const updateSet = cols
    .filter(c => !conflictTarget.split(', ').includes(c))
    .map(c => `${c}=EXCLUDED.${c}`)
    .join(', ');
  await pg.query(
    `INSERT INTO ${table} (${colList}) VALUES ${placeholders.join(', ')}
     ON CONFLICT (${conflictTarget}) DO UPDATE SET ${updateSet}`,
    values
  );
}

async function syncPublicites(force) {
  const ms = await getMssql();
  const pg  = getPg();

  // --- pub_entetes (FBENTPUB) ---
  try {
    const res = await ms.request().query(`
      SELECT ENT_NPUB, ENT_DATDEB, ENT_DATFIN, ENT_TITRE
      FROM FBENTPUB
    `);

    const rows = res.recordset.map(r => ({
      ent_npub:  String(r.ENT_NPUB),
      ent_datdeb: r.ENT_DATDEB && r.ENT_DATDEB.getFullYear() > 1901 ? r.ENT_DATDEB : null,
      ent_datfin: r.ENT_DATFIN && r.ENT_DATFIN.getFullYear() > 1901 ? r.ENT_DATFIN : null,
      ent_titre:  r.ENT_TITRE,
    }));

    await pg.query('TRUNCATE TABLE pub_entetes');
    for (let i = 0; i < rows.length; i += CHUNK) {
      const chunk = rows.slice(i, i + CHUNK);
      const values = [];
      const placeholders = chunk.map((row, ri) => {
        const ph = ENTETES_COLS.map((col, ci) => {
          values.push(row[col] ?? null);
          return `$${ri * ENTETES_COLS.length + ci + 1}`;
        });
        return `(${ph.join(', ')})`;
      });
      await pg.query(
        `INSERT INTO pub_entetes (${ENTETES_COLS.join(', ')}) VALUES ${placeholders.join(', ')}`,
        values
      );
    }

    await logSync(pg, 'pub_entetes', rows.length, 'ok');
    console.log(`[pub_entetes] ${rows.length} lignes`);
  } catch (err) {
    await logSync(pg, 'pub_entetes', 0, 'error', err.message);
    console.error(`[pub_entetes] ERREUR: ${err.message}`);
  }

  // --- pub_ecoulement (ECOULEMENT) ---
  try {
    const res = await ms.request().query(`
      SELECT
        SITE, TCR_CODE, TCR_LIBELLE, TCR_NATURE, TCR_TYPE,
        TCRD_DATEDEB, TCRD_DATEFIN, DATECALCUL,
        STOCK_DATEDEBUT, CA_TOTAL_PERIODE_PUB, CA_PUB_PERIODE_PUB,
        POURC_CAPUB_CATOTAL, QTE_VENDUE_PUB, CLIENT_TOTAL_PERIODE,
        CLIENT_PUB_PERIODE, STOCK_DATEFIN,
        CA_PUB_30_JOURS, STOCK_30_JOURS,
        CA_PUB_60_JOURS, STOCK_60_JOURS,
        CA_PUB_90_JOURS, STOCK_90_JOURS,
        CA_PUB_180_JOURS, STOCK_180_JOURS,
        CA_DEPUIS_FINPUB, TAUX_SORTIE, MARGE, TAUX_MARGE
      FROM ECOULEMENT
    `);

    const rows = res.recordset.map(r => ({
      site:                  r.SITE,
      tcr_code:              r.TCR_CODE,
      tcr_libelle:           r.TCR_LIBELLE,
      tcr_nature:            r.TCR_NATURE,
      tcr_type:              r.TCR_TYPE,
      tcrd_datedeb:          r.TCRD_DATEDEB,
      tcrd_datefin:          r.TCRD_DATEFIN,
      datecalcul:            r.DATECALCUL,
      stock_datedebut:       r.STOCK_DATEDEBUT,
      ca_total_periode_pub:  r.CA_TOTAL_PERIODE_PUB,
      ca_pub_periode_pub:    r.CA_PUB_PERIODE_PUB,
      pourc_capub_catotal:   r.POURC_CAPUB_CATOTAL,
      qte_vendue_pub:        r.QTE_VENDUE_PUB,
      client_total_periode:  r.CLIENT_TOTAL_PERIODE,
      client_pub_periode:    r.CLIENT_PUB_PERIODE,
      stock_datefin:         r.STOCK_DATEFIN,
      ca_pub_30_jours:       r.CA_PUB_30_JOURS,
      stock_30_jours:        r.STOCK_30_JOURS,
      ca_pub_60_jours:       r.CA_PUB_60_JOURS,
      stock_60_jours:        r.STOCK_60_JOURS,
      ca_pub_90_jours:       r.CA_PUB_90_JOURS,
      stock_90_jours:        r.STOCK_90_JOURS,
      ca_pub_180_jours:      r.CA_PUB_180_JOURS,
      stock_180_jours:       r.STOCK_180_JOURS,
      ca_depuis_finpub:      r.CA_DEPUIS_FINPUB,
      taux_sortie:           r.TAUX_SORTIE,
      marge:                 r.MARGE,
      taux_marge:            r.TAUX_MARGE,
    }));

    await pg.query('TRUNCATE TABLE pub_ecoulement_detail');
    await pg.query('TRUNCATE TABLE pub_ecoulement');
    for (let i = 0; i < rows.length; i += CHUNK) {
      await insertChunk(pg, 'pub_ecoulement', rows.slice(i, i + CHUNK), ECOULEMENT_COLS, 'site, tcr_code');
    }

    await logSync(pg, 'pub_ecoulement', rows.length, 'ok');
    console.log(`[pub_ecoulement] ${rows.length} lignes`);
  } catch (err) {
    await logSync(pg, 'pub_ecoulement', 0, 'error', err.message);
    console.error(`[pub_ecoulement] ERREUR: ${err.message}`);
  }

  // --- pub_ecoulement_detail (ECOULEMENT_DETAIL) ---
  try {
    const res = await ms.request().query(`
      SELECT
        SITE, TCR_CODE, ARTNOID, TCR_LIBELLE,
        TCRD_DATEDEB, TCRD_DATEFIN, DATECALCUL,
        CA_PUB_PERIODE_PUB, QTE_VENDUE_PUB,
        STOCK_DATEDEBUT, STOCK_DATEFIN,
        CA_PUB_30_JOURS, CA_PUB_60_JOURS, CA_PUB_90_JOURS, CA_PUB_180_JOURS,
        CA_DEPUIS_FINPUB, TAUX_SORTIE, MARGE, TAUX_MARGE,
        CODEIN, LIBELLE, PRMP, PA, PV, PRIXPUB
      FROM ECOULEMENT_DETAIL
    `);

    const rows = res.recordset.map(r => ({
      site:               r.SITE,
      tcr_code:           r.TCR_CODE,
      artnoid:            r.ARTNOID,
      tcr_libelle:        r.TCR_LIBELLE,
      tcrd_datedeb:       r.TCRD_DATEDEB,
      tcrd_datefin:       r.TCRD_DATEFIN,
      datecalcul:         r.DATECALCUL,
      ca_pub_periode_pub: r.CA_PUB_PERIODE_PUB,
      qte_vendue_pub:     r.QTE_VENDUE_PUB,
      stock_datedebut:    r.STOCK_DATEDEBUT,
      stock_datefin:      r.STOCK_DATEFIN,
      ca_pub_30_jours:    r.CA_PUB_30_JOURS,
      ca_pub_60_jours:    r.CA_PUB_60_JOURS,
      ca_pub_90_jours:    r.CA_PUB_90_JOURS,
      ca_pub_180_jours:   r.CA_PUB_180_JOURS,
      ca_depuis_finpub:   r.CA_DEPUIS_FINPUB,
      taux_sortie:        r.TAUX_SORTIE,
      marge:              r.MARGE,
      taux_marge:         r.TAUX_MARGE,
      codein:             r.CODEIN,
      libelle:            r.LIBELLE,
      prmp:               r.PRMP,
      pa:                 r.PA,
      pv:                 r.PV,
      prixpub:            r.PRIXPUB,
    }));

    for (let i = 0; i < rows.length; i += CHUNK) {
      await insertChunk(pg, 'pub_ecoulement_detail', rows.slice(i, i + CHUNK), DETAIL_COLS, 'site, tcr_code, artnoid');
    }

    await logSync(pg, 'pub_ecoulement_detail', rows.length, 'ok');
    console.log(`[pub_ecoulement_detail] ${rows.length} lignes`);
  } catch (err) {
    await logSync(pg, 'pub_ecoulement_detail', 0, 'error', err.message);
    console.error(`[pub_ecoulement_detail] ERREUR: ${err.message}`);
  }
}

module.exports = { syncPublicites };
