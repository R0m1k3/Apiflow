const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/mouvements/types - Liste des types de mouvements distincts dans la base
router.get('/types', async (req, res) => {
  try {
    const pool = getPool();
    const result = await pool.query(`
      SELECT GenreMvt, LibMvt, COUNT(*) AS nb_occurrences
      FROM MvtArt
      GROUP BY GenreMvt, LibMvt
      ORDER BY GenreMvt, nb_occurrences DESC
    `);
    res.json({ types: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/mouvements/articles?dateDebut=&dateFin=&site=&codefou=&genremvt=&page=&limit=
router.get('/articles', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', site = '', codefou = '', genremvt = '', page = 1, limit = 100 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 100, 1000));
    const offsetNum = (pageNum - 1) * limitNum;
    const genreFilter = genremvt !== '' ? parseInt(genremvt) : null;

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
        AND ($5::int IS NULL OR m.GenreMvt = $5)
      ORDER BY m.DatMvt DESC
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [dateDebut, dateFin, `%${site}%`, `%${codefou}%`, genreFilter]);

    res.json({ page: pageNum, limit: limitNum, mouvements: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/mouvements/entrees?dateDebut=&dateFin=&site=&artNoId=&page=&limit=
// Entrées en stock uniquement (GenreMvt = 1) avec date de création article
router.get('/entrees', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', site = '', artNoId = '', page = 1, limit = 100 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 100, 1000));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT
        m.DatMvt AS date_entree,
        m.Site,
        a.NO_ID AS artnoid,
        a.CODEIN,
        a.LIBELLE1,
        a.SUIVIDATECREATION AS date_creation_article,
        m.LibMvt AS libelle_mouvement,
        m.QteMvt AS qte_entree,
        m.Prmp AS prmp,
        m.QteStock AS stock_apres_entree,
        m.CODEFOU AS codefou
      FROM MvtArt m
      JOIN ARTICLES a ON a.NO_ID = m.ArtNoId
      WHERE m.GenreMvt = 1
        AND m.DatMvt BETWEEN $1 AND $2
        AND m.Site LIKE $3
        AND ($4 = '' OR m.ArtNoId::text = $4)
      ORDER BY m.DatMvt DESC
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [dateDebut, dateFin, `%${site}%`, artNoId]);

    res.json({ page: pageNum, limit: limitNum, entrees: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/mouvements/regularisations?dateDebut=&dateFin=&site=&artNoId=&page=&limit=
// Mouvements hors ventes (GenreMvt=3) et hors entrées standard (GenreMvt=1)
// = inventaires, corrections, démarques, transferts, etc.
router.get('/regularisations', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', site = '', artNoId = '', page = 1, limit = 100 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 100, 1000));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT
        m.DatMvt AS date_mouvement,
        m.Site,
        a.NO_ID AS artnoid,
        a.CODEIN,
        a.LIBELLE1,
        m.GenreMvt,
        m.LibMvt AS libelle_mouvement,
        m.QteMvt AS qte,
        m.ValMvt AS valeur,
        m.QteStock AS stock_apres,
        m.Prmp AS prmp
      FROM MvtArt m
      JOIN ARTICLES a ON a.NO_ID = m.ArtNoId
      WHERE m.GenreMvt NOT IN (1, 3)
        AND m.DatMvt BETWEEN $1 AND $2
        AND m.Site LIKE $3
        AND ($4 = '' OR m.ArtNoId::text = $4)
      ORDER BY m.DatMvt DESC
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [dateDebut, dateFin, `%${site}%`, artNoId]);

    res.json({ page: pageNum, limit: limitNum, regularisations: result.rows });
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
