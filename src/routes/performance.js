const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/performance/ca?dateDebut=&dateFin=&site=&groupBy=jour|mois
router.get('/ca', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', site = '', groupBy = 'jour' } = req.query;

    const groupExpr = groupBy === 'mois'
      ? "TO_CHAR(DatMvt, 'YYYY-MM')"
      : "DatMvt::DATE";

    const result = await pool.query(`
      SELECT
        ${groupExpr} AS Periode,
        Site,
        COUNT(*) AS NB_VENTES,
        SUM(QteMvt) AS QTE_VENDUE,
        SUM(MntMvtHt) AS CA_HT,
        SUM(MntMvtTTC) AS CA_TTC,
        SUM(MargeMvt) AS MARGE,
        CASE WHEN SUM(MntMvtHt) != 0
          THEN ROUND(SUM(MargeMvt) / ABS(SUM(MntMvtHt)) * 100, 2)
          ELSE 0 END AS TAUX_MARGE
      FROM MvtArt
      WHERE GenreMvt = 3
        AND DatMvt BETWEEN $1 AND $2
        AND Site LIKE $3
      GROUP BY ${groupExpr}, Site
      ORDER BY Periode DESC, Site
    `, [dateDebut, dateFin, `%${site}%`]);

    res.json({
      groupBy,
      dateDebut,
      dateFin,
      site: site || 'tous',
      ca: result.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/performance/hitparade?dateDebut=&dateFin=&site=&limit=&groupBy=qte|ca|marge
router.get('/hitparade', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', site = '', limit = 50, groupBy = 'ca' } = req.query;
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 50, 500));

    const orderCol = groupBy === 'qte' ? 'QTE_VENDUE' : groupBy === 'marge' ? 'MARGE' : 'CA_HT';

    const result = await pool.query(`
      SELECT
        a.CODEIN,
        a.LIBELLE1,
        m.Site,
        COUNT(*) AS NB_PASSAGES,
        ABS(SUM(m.QteMvt)) AS QTE_VENDUE,
        ABS(SUM(m.MntMvtHt)) AS CA_HT,
        ABS(SUM(m.MntMvtTTC)) AS CA_TTC,
        SUM(m.MargeMvt) AS MARGE,
        CASE WHEN SUM(m.MntMvtHt) != 0
          THEN ROUND(SUM(m.MargeMvt) / ABS(SUM(m.MntMvtHt)) * 100, 2)
          ELSE 0 END AS TAUX_MARGE,
        MAX(m.DatMvt) AS DERNIERE_VENTE
      FROM MvtArt m
      JOIN ARTICLES a ON a.NO_ID = m.ArtNoId
      WHERE m.GenreMvt = 3
        AND m.DatMvt BETWEEN $1 AND $2
        AND m.Site LIKE $3
      GROUP BY a.CODEIN, a.LIBELLE1, m.Site
      ORDER BY ${orderCol} DESC
      LIMIT ${limitNum}
    `, [dateDebut, dateFin, `%${site}%`]);

    res.json({
      dateDebut,
      dateFin,
      site: site || 'tous',
      classementPar: groupBy,
      hitparade: result.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/performance/ca/nomenclature?dateDebut=&dateFin=&site=&niveau=
router.get('/ca/nomenclature', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', site = '', niveau = 1 } = req.query;

    const result = await pool.query(`
      SELECT
        n.CODE AS CODE_NOMEN,
        n.LIBELLE,
        n.NIVEAU,
        COUNT(DISTINCT m.ArtNoId) AS NB_ARTICLES,
        SUM(m.QteMvt) AS QTE_VENDUE,
        SUM(m.MntMvtHt) AS CA_HT,
        SUM(m.MntMvtTTC) AS CA_TTC,
        SUM(m.MargeMvt) AS MARGE
      FROM MvtArt m
      JOIN ARTICLES a ON a.NO_ID = m.ArtNoId
      JOIN NOMENCLATURE n ON n.NO_ID = a.NOM_NO_ID
      WHERE m.GenreMvt = 3
        AND m.DatMvt BETWEEN $1 AND $2
        AND m.Site LIKE $3
        AND n.NIVEAU = $4
      GROUP BY n.CODE, n.LIBELLE, n.NIVEAU
      ORDER BY CA_TTC DESC
    `, [dateDebut, dateFin, `%${site}%`, parseInt(niveau)]);

    res.json({ dateDebut, dateFin, niveau: parseInt(niveau), ca_par_nomenclature: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/performance/ca/gamme?dateDebut=&dateFin=&site=
router.get('/ca/gamme', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', site = '' } = req.query;

    const result = await pool.query(`
      SELECT
        g.CODE AS GAMME,
        g.LIBELLE AS LIBELLE_GAMME,
        COUNT(DISTINCT m.ArtNoId) AS NB_ARTICLES,
        SUM(m.QteMvt) AS QTE_VENDUE,
        SUM(m.MntMvtHt) AS CA_HT,
        SUM(m.MntMvtTTC) AS CA_TTC,
        SUM(m.MargeMvt) AS MARGE
      FROM MvtArt m
      JOIN ART_GAMME_SAISON ags ON ags.ARTNOID = m.ArtNoId
      JOIN GAMMES g ON g.NO_ID = ags.IDGAMME
      WHERE m.GenreMvt = 3
        AND m.DatMvt BETWEEN $1 AND $2
        AND m.Site LIKE $3
      GROUP BY g.CODE, g.LIBELLE
      ORDER BY CA_TTC DESC
    `, [dateDebut, dateFin, `%${site}%`]);

    res.json({ dateDebut, dateFin, ca_par_gamme: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
