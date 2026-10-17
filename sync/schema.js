/**
 * Applique le schéma PostgreSQL au démarrage de chaque sync :
 *   1. migrations/*.sql non encore appliquées (table schema_migrations), dans l'ordre ;
 *   2. init.sql (idempotent : CREATE … IF NOT EXISTS) pour créer les nouvelles tables.
 *
 * Le dossier est monté depuis ../postgres (docker-compose : ./postgres:/app/schema:ro).
 */
'use strict';
const fs   = require('fs');
const path = require('path');
const { getPg } = require('./db');

const SCHEMA_DIR = process.env.SCHEMA_DIR || path.join(__dirname, 'schema');

async function applySchema() {
  const pg = getPg();
  if (!fs.existsSync(path.join(SCHEMA_DIR, 'init.sql'))) {
    console.warn(`[schema] ${SCHEMA_DIR}/init.sql introuvable — schéma non appliqué (monter ./postgres sur /app/schema)`);
    return { applied: [], skipped: true };
  }
  await pg.query(`CREATE TABLE IF NOT EXISTS schema_migrations (
    name TEXT PRIMARY KEY, applied_at TIMESTAMP DEFAULT NOW())`);

  const applied = [];
  const migDir = path.join(SCHEMA_DIR, 'migrations');
  if (fs.existsSync(migDir)) {
    const done = new Set((await pg.query('SELECT name FROM schema_migrations')).rows.map((r) => r.name));
    const files = fs.readdirSync(migDir).filter((f) => f.endsWith('.sql')).sort();
    for (const f of files) {
      if (done.has(f)) continue;
      const sql = fs.readFileSync(path.join(migDir, f), 'utf8');
      const client = await pg.connect();
      try {
        await client.query('BEGIN');
        await client.query(sql);
        await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [f]);
        await client.query('COMMIT');
        applied.push(f);
        console.log(`[schema] migration appliquée : ${f}`);
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {});
        throw new Error(`migration ${f} : ${err.message}`);
      } finally {
        client.release();
      }
    }
  }

  await pg.query(fs.readFileSync(path.join(SCHEMA_DIR, 'init.sql'), 'utf8'));
  return { applied, skipped: false };
}

module.exports = { applySchema };
