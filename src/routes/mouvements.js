const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/mouvements/articles?dateDebut=&dateFin=&site=&codefou=&page=&limit=
router.get('/articles', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', site = '', codefou = '', page = 1, limit = 100 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 100, 1000));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT
        m.DatMvt, m.Site,
        a.CODEIN, a.LIBELLE1,
        m.LibMvt, m.GenreMvt, m.QteMvt, m.ValMvt,
        m.MntMvtHt, m.MntMvtTTC, m.MargeMvt,
        m.QteStock, m.Prmp, m.ValStock, m.CODEFOU
      FROM MvtArt m
      JOIN ARTICLES a ON a.NO_ID = m.ArtNoId
      WHERE m.DatMvt BETWEEN $1 AND $2
        AND m.Site LIKE $3
        AND m.CODEFOU LIKE $4
      ORDER BY m.DatMvt DESC
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [dateDebut, dateFin, `%${site}%`, `%${codefou}%`]);

    res.json({ page: pageNum, limit: limitNum, mouvements: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/mouvements/reglements?dateDebut=&dateFin=&page=&limit=
router.get('/reglements', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', page = 1, limit = 100 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 100, 1000));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT
        DatMvt, CodTick, CodCarteCli, CodDev,
        MntReg, MntRegDev, ClientNom, Echeance,
        REFERENCE, TYPEREG, SuiviDateCreation
      FROM MvtReg
      WHERE DatMvt BETWEEN $1 AND $2
      ORDER BY DatMvt DESC
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [dateDebut, dateFin]);

    res.json({ page: pageNum, limit: limitNum, reglements: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/mouvements/synthese?dateDebut=&dateFin=&site= - Synthèse par jour/site
router.get('/synthese', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', site = '' } = req.query;

    const result = await pool.query(`
      SELECT
        DatMvt::DATE AS Jour,
        Site,
        GenreMvt,
        COUNT(*) AS NB_MVT,
        SUM(QteMvt) AS QTE_TOTALE,
        SUM(MntMvtHt) AS MNT_HT,
        SUM(MntMvtTTC) AS MNT_TTC,
        SUM(MargeMvt) AS MARGE
      FROM MvtArt
      WHERE DatMvt BETWEEN $1 AND $2
        AND Site LIKE $3
      GROUP BY DatMvt::DATE, Site, GenreMvt
      ORDER BY Jour DESC, Site
    `, [dateDebut, dateFin, `%${site}%`]);

    res.json({ synthese: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
