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
 * Full refresh : TRUNCATE puis INSERT en masse.
 */
async function fullRefresh(pg, table, rows, cols) {
  if (!rows.length) {
    await pg.query(`TRUNCATE TABLE ${table}`);
    return 0;
  }
  const CHUNK = 500;
  await pg.query(`TRUNCATE TABLE ${table}`);
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
    const sql = `INSERT INTO ${table} (${cols.join(', ')}) VALUES ${placeholders.join(', ')}`;
    await pg.query(sql, values);
    total += chunk.length;
  }
  return total;
}

/**
 * Lit la date du dernier sync réussi pour une table.
 */
async function getLastSync(pg, tableName) {
  const res = await pg.query(
    `SELECT last_sync FROM sync_log WHERE table_name = $1 AND status = 'ok'`,
    [tableName]
  );
  return res.rows[0]?.last_sync || null;
}

/**
 * Enregistre le résultat d'un sync dans sync_log.
 */
async function logSync(pg, tableName, rowsSynced, status, errorMsg = null) {
  await pg.query(`
    INSERT INTO sync_log (table_name, last_sync, rows_synced, status, error_msg)
    VALUES ($1, NOW(), $2, $3, $4)
    ON CONFLICT (table_name) DO UPDATE SET
      last_sync   = NOW(),
      rows_synced = $2,
      status      = $3,
      error_msg   = $4
  `, [tableName, rowsSynced, status, errorMsg]);
}

module.exports = { batchUpsert, fullRefresh, getLastSync, logSync };
