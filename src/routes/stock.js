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

// GET /api/stock/article/:id/historique?date=&site= - Stock d'un article à une date donnée
// Utilise le champ QteStock de MvtArt (stock enregistré au moment de chaque mouvement)
router.get('/article/:id/historique', async (req, res) => {
  try {
    const pool = getPool();
    const { date = new Date().toISOString().slice(0, 10), site = '' } = req.query;

    const result = await pool.query(`
      SELECT DISTINCT ON (Site)
        Site,
        QteStock AS qte,
        Prmp,
        DatMvt AS date_dernier_mouvement,
        LibMvt AS libelle_dernier_mouvement
      FROM MvtArt
      WHERE ArtNoId = $1
        AND DatMvt <= $2
        AND Site LIKE $3
      ORDER BY Site, DatMvt DESC
    `, [req.params.id, date + ' 23:59:59', `%${site}%`]);

    res.json({
      artNoId: req.params.id,
      date,
      stock: result.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/stock/article/:id/periode?dateDebut=&dateFin=&site= - Stock + ventes sur une période
router.get('/article/:id/periode', async (req, res) => {
  try {
    const pool = getPool();
    const {
      dateDebut = new Date(new Date().getFullYear(), new Date().getMonth(), 1).toISOString().slice(0, 10),
      dateFin   = new Date().toISOString().slice(0, 10),
      site = '',
    } = req.query;

    // Stock au début de la période (dernier mouvement avant dateDebut)
    const stockDebut = await pool.query(`
      SELECT DISTINCT ON (Site)
        Site, QteStock AS qte, Prmp, DatMvt AS date_mouvement
      FROM MvtArt
      WHERE ArtNoId = $1
        AND DatMvt < $2
        AND Site LIKE $3
      ORDER BY Site, DatMvt DESC
    `, [req.params.id, dateDebut, `%${site}%`]);

    // Stock à la fin de la période (dernier mouvement <= dateFin)
    const stockFin = await pool.query(`
      SELECT DISTINCT ON (Site)
        Site, QteStock AS qte, Prmp, DatMvt AS date_mouvement
      FROM MvtArt
      WHERE ArtNoId = $1
        AND DatMvt <= $2
        AND Site LIKE $3
      ORDER BY Site, DatMvt DESC
    `, [req.params.id, dateFin + ' 23:59:59', `%${site}%`]);

    // Ventes sur la période (GenreMvt = 3)
    const ventes = await pool.query(`
      SELECT
        Site,
        COUNT(*) AS nb_passages,
        SUM(QteMvt) AS qte_vendue,
        SUM(MntMvtHt) AS ca_ht,
        SUM(MntMvtTTC) AS ca_ttc,
        SUM(MargeMvt) AS marge,
        CASE WHEN SUM(MntMvtHt) != 0
          THEN ROUND(SUM(MargeMvt) / ABS(SUM(MntMvtHt)) * 100, 2)
          ELSE 0 END AS taux_marge,
        MIN(DatMvt) AS premiere_vente,
        MAX(DatMvt) AS derniere_vente
      FROM MvtArt
      WHERE ArtNoId = $1
        AND GenreMvt = 3
        AND DatMvt BETWEEN $2 AND $3
        AND Site LIKE $4
      GROUP BY Site
    `, [req.params.id, dateDebut, dateFin + ' 23:59:59', `%${site}%`]);

    // Réceptions sur la période (GenreMvt = 1)
    const receptions = await pool.query(`
      SELECT
        Site,
        COUNT(*) AS nb_receptions,
        SUM(QteMvt) AS qte_recue
      FROM MvtArt
      WHERE ArtNoId = $1
        AND GenreMvt = 1
        AND DatMvt BETWEEN $2 AND $3
        AND Site LIKE $4
      GROUP BY Site
    `, [req.params.id, dateDebut, dateFin + ' 23:59:59', `%${site}%`]);

    // Indexer par site pour faciliter l'assemblage
    const bysite = (rows) => Object.fromEntries(rows.map(r => [r.site, r]));
    const sd = bysite(stockDebut.rows);
    const sf = bysite(stockFin.rows);
    const sv = bysite(ventes.rows);
    const sr = bysite(receptions.rows);

    const sites = [...new Set([
      ...Object.keys(sd), ...Object.keys(sf),
      ...Object.keys(sv), ...Object.keys(sr),
    ])].sort();

    const data = sites.map(s => ({
      site: s,
      stock_debut:  sd[s] ? { qte: sd[s].qte, prmp: sd[s].prmp, au: sd[s].date_mouvement } : null,
      stock_fin:    sf[s] ? { qte: sf[s].qte, prmp: sf[s].prmp, au: sf[s].date_mouvement } : null,
      ventes:       sv[s] ?? null,
      receptions:   sr[s] ?? null,
    }));

    res.json({ artNoId: req.params.id, dateDebut, dateFin, data });
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
