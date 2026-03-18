const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/sync/status - Dernier état de chaque table synchronisée
router.get('/status', async (req, res) => {
  try {
    const pool = getPool();
    const result = await pool.query(
      `SELECT table_name, last_sync, rows_synced, status, error_msg
       FROM sync_log ORDER BY last_sync DESC NULLS LAST`
    );
    res.json({ sync: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
