const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/schema - Structure complète de la base de données
router.get('/', async (req, res) => {
  try {
    const pool = getPool();

    const result = await pool.query(`
      SELECT
        table_schema  AS TABLE_SCHEMA,
        table_name    AS TABLE_NAME,
        table_type    AS TABLE_TYPE,
        column_name   AS COLUMN_NAME,
        ordinal_position AS ORDINAL_POSITION,
        data_type     AS DATA_TYPE,
        character_maximum_length AS CHARACTER_MAXIMUM_LENGTH,
        numeric_precision AS NUMERIC_PRECISION,
        numeric_scale AS NUMERIC_SCALE,
        is_nullable   AS IS_NULLABLE,
        column_default AS COLUMN_DEFAULT
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
      database:   process.env.PG_DB,
      server:     process.env.PG_HOST,
      tableCount: tables.filter(t => t.type === 'BASE TABLE').length,
      viewCount:  tables.filter(t => t.type === 'VIEW').length,
      tables,
    });
  } catch (err) {
    res.status(500).json({ error: 'Erreur connexion base de données', detail: err.message });
  }
});

// GET /api/schema/tables?search= - Liste des tables avec nombre de lignes (estimé)
router.get('/tables', async (req, res) => {
  try {
    const pool = getPool();
    const search = req.query.search || '';

    const result = await pool.query(`
      SELECT
        schemaname AS TABLE_SCHEMA,
        tablename  AS TABLE_NAME,
        'BASE TABLE' AS TABLE_TYPE,
        n_live_tup AS ROW_COUNT
      FROM pg_stat_user_tables
      WHERE tablename LIKE $1
      ORDER BY n_live_tup DESC, tablename
    `, [`%${search}%`]);

    res.json({ count: result.rows.length, tables: result.rows });
  } catch (err) {
    res.status(500).json({ error: 'Erreur connexion base de données', detail: err.message });
  }
});

// GET /api/schema/top?limit= - Top tables par nombre de lignes
router.get('/top', async (req, res) => {
  try {
    const pool = getPool();
    const limitNum = Math.max(1, Math.min(parseInt(req.query.limit) || 50, 200));

    const result = await pool.query(`
      SELECT tablename AS TABLE_NAME, 'BASE TABLE' AS TABLE_TYPE, n_live_tup AS ROW_COUNT
      FROM pg_stat_user_tables
      ORDER BY n_live_tup DESC
      LIMIT $1
    `, [limitNum]);

    res.json({ tables: result.rows });
  } catch (err) {
    res.status(500).json({ error: 'Erreur connexion base de données', detail: err.message });
  }
});

// GET /api/schema/tables/:tableName - Colonnes d'une table
router.get('/tables/:tableName', async (req, res) => {
  try {
    const pool = getPool();

    const result = await pool.query(`
      SELECT
        column_name AS COLUMN_NAME,
        ordinal_position AS ORDINAL_POSITION,
        data_type AS DATA_TYPE,
        character_maximum_length AS CHARACTER_MAXIMUM_LENGTH,
        numeric_precision AS NUMERIC_PRECISION,
        numeric_scale AS NUMERIC_SCALE,
        is_nullable AS IS_NULLABLE,
        column_default AS COLUMN_DEFAULT
      FROM information_schema.columns
      WHERE table_name = $1 AND table_schema = 'public'
      ORDER BY ordinal_position
    `, [req.params.tableName]);

    if (!result.rows.length)
      return res.status(404).json({ error: `Table '${req.params.tableName}' introuvable` });

    res.json({ table: req.params.tableName, columns: result.rows });
  } catch (err) {
    res.status(500).json({ error: 'Erreur connexion base de données', detail: err.message });
  }
});

module.exports = router;
