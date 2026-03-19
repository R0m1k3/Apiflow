const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/ranking?gencod=&codein=&site=&foucentrale=&limit=
router.get('/', async (req, res) => {
  try {
    const pool = getPool();
    const { gencod = '', codein = '', site = '', foucentrale = '', limit = 50 } = req.query;
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 50, 500));

    const result = await pool.query(`
      SELECT
        r.gencod, r.site, r.libelle,
        r.foucentrale, r.nomfoucentrale,
        r.ranking_ca, r.ranking_qte,
        r.ranking_mag_ca, r.ranking_mag_qte, r.ranking_mag_marge,
        r.pv_calcule, r.pv_mag, r.pv_cen,
        r.codefamille, r.libellefamille,
        r.date_maj, r.date_calcul_mag,
        a.no_id AS art_no_id, a.codein
      FROM ranking r
      LEFT JOIN art_gtin g ON g.gtin = r.gencod
      LEFT JOIN articles a ON a.no_id = g.idarticle
      WHERE ($1 = '' OR r.gencod = $1)
        AND ($2 = '' OR a.codein = $2)
        AND ($3 = '' OR r.site LIKE $3)
        AND ($4 = '' OR r.foucentrale LIKE $4)
      ORDER BY r.ranking_ca ASC NULLS LAST
      LIMIT $5
    `, [gencod, codein, `%${site}%`, `%${foucentrale}%`, limitNum]);

    res.json({ count: result.rows.length, ranking: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/ranking/article/:id - Ranking pour un article (par no_id)
router.get('/article/:id', async (req, res) => {
  try {
    const pool = getPool();
    const artNoId = req.params.id;

    const result = await pool.query(`
      SELECT
        r.gencod, r.site, r.libelle,
        r.foucentrale, r.nomfoucentrale,
        r.ranking_ca, r.ranking_qte,
        r.ranking_mag_ca, r.ranking_mag_qte, r.ranking_mag_marge,
        r.pv_calcule, r.pv_mag, r.pv_cen,
        r.codefamille, r.libellefamille,
        r.date_maj, r.date_calcul_mag
      FROM ranking r
      JOIN art_gtin g ON g.gtin = r.gencod
      WHERE g.idarticle = $1
      ORDER BY r.site
    `, [artNoId]);

    res.json({ artNoId, ranking: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
