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

// GET /api/performance/ca/fournisseur?dateDebut=&dateFin=&site=
router.get('/ca/fournisseur', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', site = '' } = req.query;

    const result = await pool.query(`
      SELECT
        COALESCE(fi.nom, af.code, 'Inconnu') AS fournisseur,
        af.code AS code_fournisseur,
        COUNT(DISTINCT m.artnoid) AS nb_articles,
        ABS(SUM(m.qtemvt))    AS qte_vendue,
        ABS(SUM(m.mntmvtht))  AS ca_ht,
        ABS(SUM(m.mntmvtttc)) AS ca_ttc,
        SUM(m.margemvt)       AS marge,
        CASE WHEN SUM(m.mntmvtht) != 0
          THEN ROUND(SUM(m.margemvt) / ABS(SUM(m.mntmvtht)) * 100, 2)
          ELSE 0 END AS taux_marge
      FROM mvtart m
      JOIN articles a ON a.no_id = m.artnoid
      LEFT JOIN artfou1 af ON af.art_no_id = a.no_id AND af.preference = true
      LEFT JOIN fouident fi ON fi.code = af.code
      WHERE m.genremvt = 3
        AND m.datmvt BETWEEN $1 AND $2
        AND m.site LIKE $3
      GROUP BY fi.nom, af.code
      ORDER BY ca_ttc DESC
    `, [dateDebut, dateFin, `%${site}%`]);

    res.json({ dateDebut, dateFin, site: site || 'tous', ca_par_fournisseur: result.rows });
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

// GET /api/performance/dashboard?date=2026-03-20&site=
// CA + trafic du jour vs N-1 + top 10 CA / qté / marge
router.get('/dashboard', async (req, res) => {
  try {
    const pool = getPool();
    const { date, site = '' } = req.query;

    const targetDate = date || new Date(Date.now() - 86400000).toISOString().slice(0, 10);
    const n1Date     = new Date(targetDate);
    n1Date.setFullYear(n1Date.getFullYear() - 1);
    const n1DateStr  = n1Date.toISOString().slice(0, 10);

    const siteFilter = site ? `AND site = $3` : '';
    const siteParams = site ? [targetDate, n1DateStr, site] : [targetDate, n1DateStr];

    const [caRows, top10CA, top10Qte, top10Marge] = await Promise.all([
      // CA + trafic N et N-1
      pool.query(`
        SELECT site, datmvt::DATE AS date, mnt AS ca_ttc, nbticket AS trafic
        FROM statopca
        WHERE datmvt IN ($1, $2) ${siteFilter}
        ORDER BY site, datmvt
      `, siteParams),

      // Top 10 CA par site
      pool.query(`
        SELECT codein, libelle1, site, ca_ttc, qte_vendue, marge FROM (
          SELECT a.codein, a.libelle1, m.site,
                 ABS(SUM(m.mntmvtttc)) AS ca_ttc,
                 ABS(SUM(m.qtemvt))    AS qte_vendue,
                 SUM(m.margemvt)       AS marge,
                 RANK() OVER (PARTITION BY m.site ORDER BY ABS(SUM(m.mntmvtttc)) DESC) AS rk
          FROM mvtart m
          JOIN articles a ON a.no_id = m.artnoid
          WHERE m.datmvt::DATE = $1 AND m.genremvt = 3 ${site ? 'AND m.site = $2' : ''}
          GROUP BY a.codein, a.libelle1, m.site
        ) t WHERE rk <= 10
        ORDER BY site, rk
      `, site ? [targetDate, site] : [targetDate]),

      // Top 10 Qté par site
      pool.query(`
        SELECT codein, libelle1, site, ca_ttc, qte_vendue, marge FROM (
          SELECT a.codein, a.libelle1, m.site,
                 ABS(SUM(m.mntmvtttc)) AS ca_ttc,
                 ABS(SUM(m.qtemvt))    AS qte_vendue,
                 SUM(m.margemvt)       AS marge,
                 RANK() OVER (PARTITION BY m.site ORDER BY ABS(SUM(m.qtemvt)) DESC) AS rk
          FROM mvtart m
          JOIN articles a ON a.no_id = m.artnoid
          WHERE m.datmvt::DATE = $1 AND m.genremvt = 3 ${site ? 'AND m.site = $2' : ''}
          GROUP BY a.codein, a.libelle1, m.site
        ) t WHERE rk <= 10
        ORDER BY site, rk
      `, site ? [targetDate, site] : [targetDate]),

      // Top 10 Marge par site
      pool.query(`
        SELECT codein, libelle1, site, ca_ttc, qte_vendue, marge FROM (
          SELECT a.codein, a.libelle1, m.site,
                 ABS(SUM(m.mntmvtttc)) AS ca_ttc,
                 ABS(SUM(m.qtemvt))    AS qte_vendue,
                 SUM(m.margemvt)       AS marge,
                 RANK() OVER (PARTITION BY m.site ORDER BY SUM(m.margemvt) DESC) AS rk
          FROM mvtart m
          JOIN articles a ON a.no_id = m.artnoid
          WHERE m.datmvt::DATE = $1 AND m.genremvt = 3 ${site ? 'AND m.site = $2' : ''}
          GROUP BY a.codein, a.libelle1, m.site
        ) t WHERE rk <= 10
        ORDER BY site, rk
      `, site ? [targetDate, site] : [targetDate]),
    ]);

    // Construire la comparaison N vs N-1 par site
    const byDate = {};
    for (const row of caRows.rows) {
      const d = row.date instanceof Date ? row.date.toISOString().slice(0, 10) : String(row.date).slice(0, 10);
      if (!byDate[row.site]) byDate[row.site] = {};
      byDate[row.site][d] = { ca_ttc: parseFloat(row.ca_ttc) || 0, trafic: row.trafic || 0 };
    }

    const sites = Object.keys(byDate).sort().map(s => {
      const n  = byDate[s]?.[targetDate] || { ca_ttc: 0, trafic: 0 };
      const n1 = byDate[s]?.[n1DateStr]  || { ca_ttc: 0, trafic: 0 };
      const evolCa     = n1.ca_ttc  > 0 ? Math.round((n.ca_ttc  / n1.ca_ttc  - 1) * 1000) / 10 : null;
      const evolTrafic = n1.trafic  > 0 ? Math.round((n.trafic  / n1.trafic  - 1) * 1000) / 10 : null;
      return {
        site:          s,
        ca_ttc:        n.ca_ttc,   ca_ttc_n1:    n1.ca_ttc,
        evol_ca:       evolCa !== null ? `${evolCa > 0 ? '+' : ''}${evolCa}%` : null,
        trafic:        n.trafic,   trafic_n1:    n1.trafic,
        evol_trafic:   evolTrafic !== null ? `${evolTrafic > 0 ? '+' : ''}${evolTrafic}%` : null,
      };
    });

    res.json({
      date:       targetDate,
      date_n1:    n1DateStr,
      sites,
      top10_ca:    top10CA.rows,
      top10_qte:   top10Qte.rows,
      top10_marge: top10Marge.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
