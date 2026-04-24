const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/fournisseurs - Liste des fournisseurs distincts
router.get('/', async (req, res) => {
  try {
    const pool = getPool();
    const { search = '' } = req.query;

    const result = await pool.query(`
      SELECT
        f.CODE AS CODEFOU,
        MAX(fi.nom) AS NOM,
        MAX(fa.ADRLIGNE1) AS ADRESSE,
        MAX(fa.TELEPHONE) AS TELEPHONE,
        MAX(fa.EMAIL) AS EMAIL,
        COUNT(DISTINCT f.ART_NO_ID) AS NB_ARTICLES,
        MIN(f.SUIVIDATECREATION) AS PREMIERE_ENTREE,
        MAX(f.SUIVIDATEMODIF) AS DERNIERE_MODIF,
        SUM(CASE WHEN f.SUSPENDU IS NULL THEN 1 ELSE 0 END) AS NB_ACTIFS,
        SUM(CASE WHEN f.SUSPENDU IS NOT NULL THEN 1 ELSE 0 END) AS NB_SUSPENDUS
      FROM ARTFOU1 f
      LEFT JOIN fouident fi ON fi.code = f.CODE
      LEFT JOIN FOUADR1 fa ON fa.CODE = f.CODE AND fa.SIT_CODE = '000'
      WHERE f.CODE LIKE $1
      GROUP BY f.CODE
      ORDER BY NB_ARTICLES DESC
    `, [`%${search}%`]);

    res.json({ count: result.rows.length, fournisseurs: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/fournisseurs/:code/articles?page=&limit=&suspendu=
router.get('/:code/articles', async (req, res) => {
  try {
    const pool = getPool();
    const { page = 1, limit = 50 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 50, 500));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT
        a.NO_ID, a.CODEIN, a.LIBELLE1, a.LIBELLE2,
        f.REFERENCE AS REF_FOU, f.EAN13, f.ITF,
        f.PCB, f.SPCB, f.DELAI, f.SECURITE, f.SUSPENDU, f.PREFERENCE,
        t2.PRIXACHAT, t2.REMISE_PROMOTION,
        pa.PA,
        pv.PV AS PV_CENTRAL
      FROM ARTFOU1 f
      JOIN ARTICLES a ON a.NO_ID = f.ART_NO_ID
      LEFT JOIN ARTFOU2 t2 ON t2.IDARTFOU1 = f.NO_ID
      LEFT JOIN Cube_PA pa ON pa.ArtNoId = a.NO_ID
      LEFT JOIN Cube_PV pv ON pv.ArtNoId = a.NO_ID AND pv.Site = '000'
      WHERE f.CODE = $1
      ORDER BY a.LIBELLE1
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [req.params.code]);

    res.json({ code: req.params.code, page: pageNum, limit: limitNum, articles: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/fournisseurs/:code/commandes?dateDebut=&dateFin=&page=&limit=
router.get('/:code/commandes', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2020-01-01', dateFin = '2099-12-31', page = 1, limit = 100 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 100, 1000));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT
        cv.CDEFOU_LIGNE_COM_NO_ID AS NO_COMMANDE,
        cv.ARTICLES_CODEIN AS CODEIN,
        cv.ARTICLES_LIBELLE1 AS LIBELLE,
        cv.ARTFOU1_REFERENCE AS REF_FOU,
        cv.ARTFOU1_EAN13 AS EAN13,
        cv.CDEFOU_LIGNE_QTECDE AS QTE_CDE,
        cv.CDEFOU_LIGNE_PRIXBRUT AS PRIX_BRUT,
        cv.CDEFOU_LIGNE_REMISE AS REMISE,
        cv.CDEFOU_LIGNE_PRIXNET AS PRIX_NET,
        cv.CDEFOU_LIGNE_MONTANT AS MONTANT,
        cv.CDEFOU_LIGNE_QTEACC AS QTE_ACCEPTEE,
        cv.CDEFOU_LIGNE_QTEANN AS QTE_ANNULEE,
        cv.CDEFOU_LIGNE_QTEATT AS QTE_ATTENTE,
        cv.CDEFOU_LIGNE_CDELIGTARD AS DATE_LIVRAISON,
        cv.SUIVIDATECREATION AS DATE_COMMANDE
      FROM CDEFOU_VIVANT cv
      JOIN ARTFOU1 f ON f.NO_ID = cv.ARTFOU1_NO_ID
      WHERE f.CODE = $1
        AND cv.SUIVIDATECREATION BETWEEN $2 AND $3
      ORDER BY cv.SUIVIDATECREATION DESC
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [req.params.code, dateDebut, dateFin]);

    res.json({ code: req.params.code, page: pageNum, limit: limitNum, commandes: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
