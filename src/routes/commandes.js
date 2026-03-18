const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/commandes?dateDebut=&dateFin=&codefou=&page=&limit=
router.get('/', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2020-01-01', dateFin = '2099-12-31', codefou = '', page = 1, limit = 100 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 100, 1000));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT
        cv.CDEFOU_LIGNE_COM_NO_ID    AS NO_COMMANDE,
        f.CODE                        AS CODEFOU,
        cv.ARTICLES_CODEIN            AS CODEIN,
        cv.ARTICLES_LIBELLE1          AS LIBELLE,
        cv.ARTFOU1_REFERENCE          AS REF_FOU,
        cv.ARTFOU1_EAN13              AS EAN13,
        cv.ARTFOU1_ITF                AS ITF,
        cv.CDEFOU_LIGNE_QTECDE        AS QTE_CDE,
        cv.CDEFOU_LIGNE_PRIXBRUT      AS PRIX_BRUT,
        cv.CDEFOU_LIGNE_REMISE        AS REMISE,
        cv.CDEFOU_LIGNE_PRIXNET       AS PRIX_NET,
        cv.CDEFOU_LIGNE_MONTANT       AS MONTANT,
        cv.CDEFOU_LIGNE_PRIXVENTE     AS PRIX_VENTE,
        cv.CDEFOU_LIGNE_QTEACC        AS QTE_ACCEPTEE,
        cv.CDEFOU_LIGNE_QTEANN        AS QTE_ANNULEE,
        cv.CDEFOU_LIGNE_QTEATT        AS QTE_ATTENTE,
        cv.CDEFOU_LIGNE_QTEREL        AS QTE_RELIQUAT,
        cv.CDEFOU_LIGNE_CDELIGTARD    AS DATE_LIVRAISON_CIBLE,
        cv.CDEFOU_LIGNE_CDELIGTOT     AS DATE_LIVRAISON_TOT,
        cv.COMMENTAIRE,
        cv.SUIVIDATECREATION          AS DATE_COMMANDE
      FROM CDEFOU_VIVANT cv
      JOIN ARTFOU1 f ON f.NO_ID = cv.ARTFOU1_NO_ID
      WHERE cv.SUIVIDATECREATION BETWEEN $1 AND $2
        AND f.CODE LIKE $3
      ORDER BY cv.SUIVIDATECREATION DESC
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [dateDebut, dateFin, `%${codefou}%`]);

    res.json({ page: pageNum, limit: limitNum, commandes: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/commandes/:noCommande - Détail d'une commande
router.get('/:noCommande', async (req, res) => {
  try {
    const pool = getPool();

    const result = await pool.query(`
      SELECT
        cv.CDEFOU_LIGNE_COM_NO_ID    AS NO_COMMANDE,
        f.CODE                        AS CODEFOU,
        cv.ARTICLES_CODEIN            AS CODEIN,
        cv.ARTICLES_LIBELLE1          AS LIBELLE,
        cv.ARTICLES_LIBELLE2          AS LIBELLE2,
        cv.ARTFOU1_REFERENCE          AS REF_FOU,
        cv.ARTFOU1_EAN13              AS EAN13,
        cv.ARTFOU1_ITF                AS ITF,
        cv.CDEFOU_LIGNE_QTECDE        AS QTE_CDE,
        cv.CDEFOU_LIGNE_PRIXBRUT      AS PRIX_BRUT,
        cv.CDEFOU_LIGNE_REMISE        AS REMISE,
        cv.CDEFOU_LIGNE_REMISE2       AS REMISE2,
        cv.CDEFOU_LIGNE_REMISE3       AS REMISE3,
        cv.CDEFOU_LIGNE_PRIXNET       AS PRIX_NET,
        cv.CDEFOU_LIGNE_MONTANT       AS MONTANT,
        cv.CDEFOU_LIGNE_PRIXVENTE     AS PRIX_VENTE,
        cv.CDEFOU_LIGNE_GRATUIT       AS QTE_GRATUITE,
        cv.CDEFOU_LIGNE_QTEACC        AS QTE_ACCEPTEE,
        cv.CDEFOU_LIGNE_QTEANN        AS QTE_ANNULEE,
        cv.CDEFOU_LIGNE_QTEATT        AS QTE_ATTENTE,
        cv.CDEFOU_LIGNE_QTEREL        AS QTE_RELIQUAT,
        cv.CDEFOU_LIGNE_CDELIGTARD    AS DATE_LIVRAISON_CIBLE,
        cv.COTISATION_LOGISTIQUE,
        cv.FRAISLOGISTIC, cv.TRANSIT, cv.DISTRIBUTION, cv.TAXE,
        cv.COMMENTAIRE,
        cv.SUIVIDATECREATION          AS DATE_COMMANDE
      FROM CDEFOU_VIVANT cv
      JOIN ARTFOU1 f ON f.NO_ID = cv.ARTFOU1_NO_ID
      WHERE cv.CDEFOU_LIGNE_COM_NO_ID = $1
      ORDER BY cv.ARTICLES_LIBELLE1
    `, [req.params.noCommande]);

    if (!result.rows.length)
      return res.status(404).json({ error: 'Commande introuvable' });

    res.json({
      noCommande: req.params.noCommande,
      nbLignes: result.rows.length,
      montantTotal: result.rows.reduce((s, l) => s + (l.montant || 0), 0),
      lignes: result.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/commandes/receptions/liste?dateDebut=&dateFin=&page=&limit=
router.get('/receptions/liste', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2020-01-01', dateFin = '2099-12-31', page = 1, limit = 100 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 100, 1000));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT
        rl.NO_ID, rl.CDEFOU_RECEPTION_NO_ID,
        rl.QTEBL, rl.QTEREC, rl.QTEACC, rl.QTEREF, rl.QTEAFF,
        rl.PRIBRUT, rl.REMISE, rl.PRIREC, rl.MNTREC,
        rl.MOTIFREFUS, rl.RECPB, rl.RECPBOK,
        f.CODE AS CODEFOU,
        f.REFERENCE AS REF_FOU, f.EAN13,
        a.CODEIN, a.LIBELLE1,
        rl.SUIVIDATECREATION AS DATE_RECEPTION
      FROM CDEFOU_RECEPLIG rl
      JOIN ARTFOU1 f ON f.NO_ID = rl.ARTFOU1_NO_ID
      JOIN ARTICLES a ON a.NO_ID = f.ART_NO_ID
      WHERE rl.SUIVIDATECREATION BETWEEN $1 AND $2
      ORDER BY rl.SUIVIDATECREATION DESC
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [dateDebut, dateFin]);

    res.json({ page: pageNum, limit: limitNum, receptions: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
