/**
 * Upsert en masse dans PostgreSQL (chunks de 500 lignes).
 * @param {import('pg').Pool} pg
 * @param {string} table  - nom de la table PostgreSQL (lowercase)
 * @param {object[]} rows - lignes avec les valeurs à insérer
 * @param {string[]} pk   - colonnes formant la clé primaire
 * @param {string[]} cols - toutes les colonnes à upsert
 * @returns {number} total de lignes traitées
 */
async function batchUpsert(pg, table, rows, pk, cols) {
  if (!rows.length) return 0;
  const CHUNK = 500;
  let total = 0;
  for (let i = 0; i < rows.length; i += CHUNK) {
    const chunk = rows.slice(i, i + CHUNK);
    const values = [];
    const placeholders = chunk.map((row, ri) => {
      const ph = cols.map((col, ci) => {
        values.push(row[col] ?? null);
        return `$${ri * cols.length + ci + 1}`;
      });
      return `(${ph.join(', ')})`;
    });
    const updateCols = cols.filter(c => !pk.includes(c));
    const updateSet  = updateCols.map(c => `${c} = EXCLUDED.${c}`).join(', ');
    const sql = `
      INSERT INTO ${table} (${cols.join(', ')})
      VALUES ${placeholders.join(', ')}
      ON CONFLICT (${pk.join(', ')}) DO UPDATE SET ${updateSet}
    `;
    await pg.query(sql, values);
    total += chunk.length;
  }
  return total;
}

/**
 * Full refresh transactionnel : DELETE + INSERT dans UNE transaction.
 * - les lecteurs (API) continuent de voir l'ancienne version jusqu'au COMMIT ;
 * - en cas d'erreur, ROLLBACK : la table reste intacte ;
 * - garde-fou : si la source renvoie < 50 % des lignes existantes (cube vide,
 *   requête tronquée…), on annule, sauf `force`.
 */
async function fullRefresh(pg, table, rows, cols, { force = false, minRatio = 0.5 } = {}) {
  const CHUNK = Math.max(1, Math.floor(60000 / cols.length));
  const client = await pg.connect();
  try {
    await client.query('BEGIN');
    const before = Number((await client.query(`SELECT count(*)::int AS n FROM ${table}`)).rows[0].n);
    if (!force && before > 100 && rows.length < before * minRatio) {
      throw new Error(`Garde-fou volume : ${rows.length} lignes reçues contre ${before} existantes — table conservée`);
    }
    await client.query(`DELETE FROM ${table}`);
    let total = 0;
    for (let i = 0; i < rows.length; i += CHUNK) {
      const chunk = rows.slice(i, i + CHUNK);
      const values = [];
      const placeholders = chunk.map((row, ri) => {
        const ph = cols.map((col, ci) => {
          values.push(row[col] ?? null);
          return `$${ri * cols.length + ci + 1}`;
        });
        return `(${ph.join(', ')})`;
      });
      await client.query(`INSERT INTO ${table} (${cols.join(', ')}) VALUES ${placeholders.join(', ')}`, values);
      total += chunk.length;
    }
    await client.query('COMMIT');
    return total;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Date à partir de laquelle relire une table en delta : dernier sync réussi
 * moins 2 jours de recouvrement (last_sync est l'heure PG en UTC alors que
 * SUIVIDATEMODIF est l'heure locale SQL Server ; les upserts sont idempotents).
 */
async function getLastSync(pg, tableName, overlapDays = 2) {
  const res = await pg.query(
    `SELECT last_sync - make_interval(days => $2) AS since FROM sync_log WHERE table_name = $1 AND status = 'ok'`,
    [tableName, overlapDays]
  );
  return res.rows[0]?.since || null;
}

/**
 * Enregistre le résultat d'un sync dans sync_log. Ne lève jamais (un échec de
 * journalisation ne doit pas masquer l'erreur d'origine).
 */
async function logSync(pg, tableName, rowsSynced, status, errorMsg = null) {
  try {
    await pg.query(`
      INSERT INTO sync_log (table_name, last_sync, rows_synced, status, error_msg)
      VALUES ($1, NOW(), $2, $3, $4)
      ON CONFLICT (table_name) DO UPDATE SET
        last_sync   = NOW(),
        rows_synced = $2,
        status      = $3,
        error_msg   = $4
    `, [tableName, rowsSynced, status, errorMsg ? String(errorMsg).slice(0, 2000) : null]);
  } catch (err) {
    console.error(`[sync_log] impossible de journaliser ${tableName}: ${err.message}`);
  }
}

// ── Helpers de conversion SQL Server → PostgreSQL ───────────

/** Nettoie une chaîne : supprime null bytes (0x00), retourne null si vide */
function safeStr(v) {
  if (v == null) return null;
  return String(v).replace(/\0/g, '') || null;
}

/** Convertit en nombre, retourne null si NaN ou non numérique */
function safeNum(v) {
  if (v == null) return null;
  if (typeof v === 'number') return isNaN(v) ? null : v;
  const n = parseFloat(String(v).replace(/[^0-9.\-]/g, ''));
  return isNaN(n) ? null : n;
}

/** Convertit en entier, retourne null si NaN */
function safeInt(v) {
  if (v == null) return null;
  if (typeof v === 'number') return Math.round(v);
  const n = parseInt(v, 10);
  return isNaN(n) ? null : n;
}

/** Convertit un BIT SQL Server (true/false) en SMALLINT (1/0) */
function safeBit(v) {
  if (v == null) return null;
  return v ? 1 : 0;
}

/**
 * Convertit une valeur DECIMAL/MONEY qui peut être retournée par mssql
 * sous forme d'objet, tableau, ou string "[date,valeur]".
 */
function safeDecimal(v) {
  if (v == null) return null;
  if (typeof v === 'number') return isNaN(v) ? null : v;
  if (Array.isArray(v)) {
    const last = v[v.length - 1];
    return typeof last === 'number' ? last : safeNum(last);
  }
  // ex: "[01/01/1901,163.000]" → 163
  const m = String(v).match(/([\d]+\.?[\d]*)[\]]*$/);
  return m ? parseFloat(m[1]) : null;
}

module.exports = { batchUpsert, fullRefresh, getLastSync, logSync, safeStr, safeNum, safeInt, safeBit, safeDecimal };
