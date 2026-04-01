const { getMssql, getPg } = require('../db');
const { logSync } = require('../utils');

const STATOPCA_COLS = ['site', 'datmvt', 'mnt', 'nbticket'];

const MVTART_COLS = [
  'no_id','artnoid','datmvt','site','libmvt','genremvt',
  'qtemvt','valmvt','mntmvtht','mntmvtttc','margemvt',
  'qtestock','prmp','valstock','codefou',
];
const MVTREG_COLS = [
  'datmvt','codtick','codcartecli','coddev',
  'mntreg','mntregdev','clientnom','echeance',
  'reference','typereg','suividatecreation',
];

const CHUNK = 2000;

async function insertChunk(pg, table, rows, cols) {
  if (!rows.length) return;
  const values = [];
  const placeholders = rows.map((row, ri) => {
    const ph = cols.map((col, ci) => {
      values.push(row[col] ?? null);
      return `$${ri * cols.length + ci + 1}`;
    });
    return `(${ph.join(', ')})`;
  });
  await pg.query(
    `INSERT INTO ${table} (${cols.join(', ')}) VALUES ${placeholders.join(', ')} ON CONFLICT (no_id) DO NOTHING`,
    values
  );
}

async function syncMouvements(force) {
  const ms = await getMssql();
  const pg  = getPg();

  // === MvtArt (insert-only par date croissante) ===
  try {
    // Date pivot : max existant en pg, ou 2 ans en arrière pour sync initiale
    const maxRes = await pg.query(`SELECT MAX(datmvt) AS last FROM mvtart`);
    let lastDate  = maxRes.rows[0]?.last;
    if (!lastDate || force) {
      const twoYearsAgo = new Date();
      twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
      lastDate = twoYearsAgo;
    }

    const res = await ms.request()
      .input('lastDate', lastDate)
      .query(`
        SELECT NO_ID, ArtNoId, DatMvt, Site, LibMvt, GenreMvt,
               QteMvt, ValMvt, MntMvtHt, MntMvtTTC, MargeMvt,
               QteStock, Prmp, ValStock, CODEFOU
        FROM MvtArt
        WHERE DatMvt > @lastDate
        ORDER BY DatMvt
      `);

    const allRows = res.recordset.map(r => ({
      no_id:      r.NO_ID,
      artnoid:    r.ArtNoId,
      datmvt:     r.DatMvt,
      site:       r.Site,
      libmvt:     r.LibMvt,
      genremvt:   r.GenreMvt,
      qtemvt:     r.QteMvt,
      valmvt:     r.ValMvt,
      mntmvtht:   r.MntMvtHt,
      mntmvtttc:  r.MntMvtTTC,
      margemvt:   r.MargeMvt,
      qtestock:   r.QteStock,
      prmp:       r.Prmp,
      valstock:   r.ValStock,
      codefou:    r.CODEFOU,
    }));

    let inserted = 0;
    for (let i = 0; i < allRows.length; i += CHUNK) {
      await insertChunk(pg, 'mvtart', allRows.slice(i, i + CHUNK), MVTART_COLS);
      inserted += Math.min(CHUNK, allRows.length - i);
    }

    await logSync(pg, 'mvtart', inserted, 'ok');
    console.log(`[mvtart] ${inserted} nouvelles lignes`);
  } catch (err) {
    await logSync(pg, 'mvtart', 0, 'error', err.message);
    console.error(`[mvtart] ERREUR: ${err.message}`);
  }

  // === MvtReg (insert-only par date) ===
  try {
    const maxRes = await pg.query(`SELECT MAX(datmvt) AS last FROM mvtreg`);
    let lastDate  = maxRes.rows[0]?.last;
    if (!lastDate || force) {
      const twoYearsAgo = new Date();
      twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
      lastDate = twoYearsAgo;
    }

    const res = await ms.request()
      .input('lastDate', lastDate)
      .query(`
        SELECT DatMvt, CodTick, CodCarteCli, CodDev,
               MntReg, MntRegDev, ClientNom, Echeance,
               REFERENCE, TYPEREG, SuiviDateCreation
        FROM MvtReg
        WHERE DatMvt > @lastDate
        ORDER BY DatMvt
      `);

    const allRows = res.recordset.map(r => ({
      datmvt:            r.DatMvt,
      codtick:           r.CodTick,
      codcartecli:       r.CodCarteCli,
      coddev:            r.CodDev,
      mntreg:            r.MntReg,
      mntregdev:         r.MntRegDev,
      clientnom:         r.ClientNom,
      echeance:          r.Echeance,
      reference:         r.REFERENCE,
      typereg:           r.TYPEREG,
      suividatecreation: r.SuiviDateCreation,
    }));

    let inserted = 0;
    for (let i = 0; i < allRows.length; i += CHUNK) {
      await insertChunk(pg, 'mvtreg', allRows.slice(i, i + CHUNK), MVTREG_COLS);
      inserted += Math.min(CHUNK, allRows.length - i);
    }

    await logSync(pg, 'mvtreg', inserted, 'ok');
    console.log(`[mvtreg] ${inserted} nouvelles lignes`);
  } catch (err) {
    await logSync(pg, 'mvtreg', 0, 'error', err.message);
    console.error(`[mvtreg] ERREUR: ${err.message}`);
  }
}

// === StatOpCAJour (upsert par site+date) ===
async function syncStatOpCA(force) {
  const ms = await getMssql();
  const pg  = getPg();

  try {
    const maxRes = await pg.query(`SELECT MAX(datmvt) AS last FROM statopca`);
    let lastDate  = maxRes.rows[0]?.last;
    if (!lastDate || force) {
      const twoYearsAgo = new Date();
      twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
      lastDate = twoYearsAgo;
    }

    const res = await ms.request()
      .input('lastDate', lastDate)
      .query(`SELECT SITE, DatMvt, mnt, nbTicket FROM StatOpCAJour WHERE DatMvt > @lastDate ORDER BY DatMvt`);

    const rows = res.recordset.map(r => ({
      site:     r.SITE,
      datmvt:   r.DatMvt,
      mnt:      r.mnt,
      nbticket: r.nbTicket,
    }));

    for (let i = 0; i < rows.length; i += CHUNK) {
      const chunk = rows.slice(i, i + CHUNK);
      const values = [];
      const placeholders = chunk.map((row, ri) => {
        STATOPCA_COLS.forEach(col => values.push(row[col] ?? null));
        const ph = STATOPCA_COLS.map((_, ci) => `$${ri * STATOPCA_COLS.length + ci + 1}`);
        return `(${ph.join(', ')})`;
      });
      await pg.query(
        `INSERT INTO statopca (${STATOPCA_COLS.join(', ')}) VALUES ${placeholders.join(', ')}
         ON CONFLICT (site, datmvt) DO UPDATE SET mnt=EXCLUDED.mnt, nbticket=EXCLUDED.nbticket`,
        values
      );
    }

    await logSync(pg, 'statopca', rows.length, 'ok');
    console.log(`[statopca] ${rows.length} lignes`);
  } catch (err) {
    await logSync(pg, 'statopca', 0, 'error', err.message);
    console.error(`[statopca] ERREUR: ${err.message}`);
  }
}

module.exports = { syncMouvements, syncStatOpCA };
