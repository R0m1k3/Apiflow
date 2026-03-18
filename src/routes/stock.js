const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/stock?site=&page=&limit= - Stock global
router.get('/', async (req, res) => {
  try {
    const pool = getPool();
    const { site = '', page = 1, limit = 100 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 100, 1000));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT
        a.CODEIN, a.LIBELLE1,
        s.Site, s.QTE, s.Prmp, s.ValStock, s.PV,
        s.StockDispo, s.StockMort, s.StockColis,
        s.DerniereVente, s.DerniereReception, s.PremiereVente,
        s.NbJoursDernierMouvement, s.NbJoursDerniereVente,
        s.NbJoursDerniereReception, s.InterditAchat
      FROM Cube_Stock s
      JOIN ARTICLES a ON a.NO_ID = s.ArtNoId
      WHERE s.Site LIKE $1
      ORDER BY a.LIBELLE1, s.Site
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [`%${site}%`]);

    res.json({ page: pageNum, limit: limitNum, stock: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stock/article/:id - Stock d'un article par site
router.get('/article/:id', async (req, res) => {
  try {
    const pool = getPool();

    const result = await pool.query(`
      SELECT
        s.Site, s.QTE, s.Prmp, s.ValStock, s.PV,
        s.StockDispo, s.StockMort, s.StockColis,
        s.DerniereVente, s.DerniereReception, s.PremiereVente,
        s.NbJoursDernierMouvement, s.NbJoursDerniereVente,
        s.NbJoursDerniereReception, s.InterditAchat, s.CODEFOU
      FROM Cube_Stock s
      WHERE s.ArtNoId = $1
      ORDER BY s.Site
    `, [req.params.id]);

    res.json({ artNoId: req.params.id, stock: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stock/site/:site - Tous les articles d'un site
router.get('/site/:site', async (req, res) => {
  try {
    const pool = getPool();
    const { page = 1, limit = 100 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 100, 1000));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT
        a.NO_ID AS ART_NO_ID, a.CODEIN, a.LIBELLE1,
        s.QTE, s.Prmp, s.ValStock, s.PV,
        s.StockDispo, s.StockMort, s.DerniereVente, s.DerniereReception,
        s.NbJoursDerniereVente, s.NbJoursDerniereReception
      FROM Cube_Stock s
      JOIN ARTICLES a ON a.NO_ID = s.ArtNoId
      WHERE s.Site = $1
      ORDER BY s.QTE DESC
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [req.params.site]);

    res.json({ site: req.params.site, page: pageNum, limit: limitNum, stock: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stock/valorisation - Valorisation du stock par site
router.get('/valorisation', async (req, res) => {
  try {
    const pool = getPool();

    const result = await pool.query(`
      SELECT
        Site,
        COUNT(*) AS NB_ARTICLES,
        SUM(QTE) AS QTE_TOTALE,
        SUM(ValStock) AS VAL_STOCK,
        SUM(QTE * PV) AS VAL_PV_THEORIQUE,
        AVG(Prmp) AS PRMP_MOYEN
      FROM Cube_Stock
      WHERE QTE > 0
      GROUP BY Site
      ORDER BY VAL_STOCK DESC
    `);

    res.json({ valorisation: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
