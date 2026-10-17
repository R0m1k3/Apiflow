// Introspection du schéma — monté uniquement si EXPOSE_SCHEMA=true (et derrière l'authentification).
// Ne renvoie jamais l'hôte ni le nom de la base.

const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { parseText, parsePositiveInt, parseCode, likeContains } = require('../utils/params');
const { HttpError } = require('../utils/errors');

// GET /api/schema - Structure complète du schéma public
router.get('/', async (req, res) => {
  const result = await getPool().query(`
      SELECT
        table_schema, table_name, table_type,
        column_name, ordinal_position, data_type,
        character_maximum_length, numeric_precision, numeric_scale,
        is_nullable, column_default
      FROM information_schema.tables
      JOIN information_schema.columns USING (table_schema, table_name)
      WHERE table_schema = 'public'
      ORDER BY table_name, ordinal_position
    `);

  const schema = {};
  for (const row of result.rows) {
    const key = `${row.table_schema}.${row.table_name}`;
    if (!schema[key]) {
      schema[key] = {
        schema: row.table_schema,
        table:  row.table_name,
        type:   row.table_type,
        columns: [],
      };
    }
    schema[key].columns.push({
      name:      row.column_name,
      position:  row.ordinal_position,
      type:      row.data_type,
      maxLength: row.character_maximum_length,
      precision: row.numeric_precision,
      scale:     row.numeric_scale,
      nullable:  row.is_nullable === 'YES',
      default:   row.column_default,
    });
  }

  const tables = Object.values(schema);
  res.json({
    tableCount: tables.filter(t => t.type === 'BASE TABLE').length,
    viewCount:  tables.filter(t => t.type === 'VIEW').length,
    tables,
  });
});

// GET /api/schema/tables?search= - Liste des tables avec nombre de lignes (estimé)
router.get('/tables', async (req, res) => {
  const search = parseText(req.query.search, 'search', { max: 100 });

  const result = await getPool().query(`
      SELECT
        schemaname AS table_schema,
        relname    AS table_name,
        'BASE TABLE' AS table_type,
        n_live_tup AS row_count
      FROM pg_stat_user_tables
      WHERE ($1 = '' OR relname LIKE $2)
      ORDER BY n_live_tup DESC, relname
    `, [search, likeContains(search)]);

  res.json({ count: result.rows.length, tables: result.rows });
});

// GET /api/schema/top?limit= - Top tables par nombre de lignes
router.get('/top', async (req, res) => {
  const limit = parsePositiveInt(req.query.limit, 'limit', { def: 50, min: 1, max: 200, clamp: true });

  const result = await getPool().query(`
      SELECT relname AS table_name, 'BASE TABLE' AS table_type, n_live_tup AS row_count
      FROM pg_stat_user_tables
      ORDER BY n_live_tup DESC
      LIMIT $1
    `, [limit]);

  res.json({ tables: result.rows });
});

// GET /api/schema/tables/:tableName - Colonnes d'une table
router.get('/tables/:tableName', async (req, res) => {
  const tableName = parseCode(req.params.tableName, 'tableName', { max: 63 });

  const result = await getPool().query(`
      SELECT
        column_name, ordinal_position, data_type,
        character_maximum_length, numeric_precision, numeric_scale,
        is_nullable, column_default
      FROM information_schema.columns
      WHERE table_name = $1 AND table_schema = 'public'
      ORDER BY ordinal_position
    `, [tableName]);

  if (!result.rows.length) throw new HttpError(404, 'Table introuvable');

  res.json({ table: tableName, columns: result.rows });
});

module.exports = router;
