const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/articles?search=&codein=&ean=&actif=&codefou=&page=&limit=
router.get('/', async (req, res) => {
  try {
    const pool = getPool();
    const { search = '', codein = '', ean = '', actif = '', codefou = '', page = 1, limit = 50 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)  || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 50, 500));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT
        a.NO_ID, a.CODEIN, a.LIBELLE1, a.LIBELLE2, a.LIB_TICKET,
        a.TAX_CODE, a.ACH_CODE, a.UTILISABLE, a.ACTIF, a.SUSPENDU,
        a.SUIVIDATECREATION, a.SUIVIDATEMODIF,
        ai.PRIX_VENTE_MINI, ai.PRIX_VENTE_MAXI, ai.ECO_TTC,
        ai.ON_WEB, ai.INTERDIT_REMISE, ai.NOMPHOTO,
        ai.DATEDEBVENTE, ai.DATEFINVENTE,
        pa.PA,
        g.GTIN,
        fpref.CODE   AS CODEFOU_PRINCIPAL,
        fpref.NOM_FOU AS NOM_FOU_PRINCIPAL,
        fpref.REF    AS REF_FOU_PRINCIPALE,
        fpref.PCB    AS PCB_PRINCIPAL
      FROM ARTICLES a
      LEFT JOIN ARTICLE_INFOSUP ai ON ai.ARTNOID = a.NO_ID
      LEFT JOIN Cube_PA pa ON pa.ArtNoId = a.NO_ID
      LEFT JOIN ART_GTIN g ON g.IDARTICLE = a.NO_ID AND g.PREFERENTIEL = 1
      LEFT JOIN LATERAL (
        SELECT f.CODE, f.REFERENCE AS REF, f.QTEUA AS PCB,
               fa.RAISONSOCIALE AS NOM_FOU
        FROM ARTFOU1 f
        LEFT JOIN FOUADR1 fa ON fa.CODE = f.CODE AND fa.SIT_CODE = '000'
        WHERE f.ART_NO_ID = a.NO_ID
        ORDER BY f.PREFERENCE
        LIMIT 1
      ) fpref ON true
      WHERE
        ($1 = '%%' OR a.LIBELLE1 LIKE $1 OR a.LIBELLE2 LIKE $1)
        AND ($2 = '%%' OR a.CODEIN LIKE $2)
        AND ($3 = '%%' OR g.GTIN LIKE $3)
        AND ($5 = '' OR (
          $5 = '1' AND a.ACTIF IS NOT NULL AND a.SUSPENDU IS NULL
        ) OR (
          $5 = '0' AND a.SUSPENDU IS NOT NULL
        ))
        AND ($4 = '%%' OR EXISTS (
          SELECT 1 FROM ARTFOU1 f WHERE f.ART_NO_ID = a.NO_ID AND f.CODE LIKE $4
        ))
      ORDER BY a.LIBELLE1
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [`%${search}%`, `%${codein}%`, `%${ean}%`, `%${codefou}%`, actif]);

    res.json({ page: pageNum, limit: limitNum, articles: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/articles/:id - Détail complet d'un article
router.get('/:id', async (req, res) => {
  try {
    const pool = getPool();

    const article = await pool.query(`
      SELECT
        a.*, ai.PRIX_VENTE_MINI, ai.PRIX_VENTE_MAXI,
        ai.ECO_HT, ai.ECO_TTC, ai.ON_WEB, ai.INTERDIT_REMISE,
        ai.NOMPHOTO, ai.DATEDEBVENTE, ai.DATEFINVENTE,
        ai.DATEVALIDITEACHAT, ai.COMMENTAIRE, ai.PV_CONSEILLE,
        ai.FRAISLOGISTIC, ai.TRANSIT, ai.DISTRIBUTION,
        pa.PA
      FROM ARTICLES a
      LEFT JOIN ARTICLE_INFOSUP ai ON ai.ARTNOID = a.NO_ID
      LEFT JOIN Cube_PA pa ON pa.ArtNoId = a.NO_ID
      WHERE a.NO_ID = $1
    `, [req.params.id]);

    if (!article.rows.length)
      return res.status(404).json({ error: 'Article introuvable' });

    const gtins = await pool.query(
      `SELECT GTIN, PREFERENTIEL FROM ART_GTIN WHERE IDARTICLE = $1 ORDER BY PREFERENTIEL DESC`,
      [req.params.id]
    );

    const stock = await pool.query(`
      SELECT Site, QTE, Prmp, ValStock, PV, StockDispo,
             DerniereVente, DerniereReception, StockMort
      FROM Cube_Stock WHERE ArtNoId = $1 ORDER BY Site
    `, [req.params.id]);

    const prix = await pool.query(
      `SELECT Site, PV FROM Cube_PV WHERE ArtNoId = $1 ORDER BY Site`,
      [req.params.id]
    );

    const fournisseurs = await pool.query(`
      SELECT f.CODE AS CODEFOU, fa.RAISONSOCIALE AS NOM_FOU,
             f.REFERENCE, f.EAN13, f.ITF,
             f.QTEUA, f.PCB, f.SPCB, f.DELAI, f.SECURITE, f.PREFERENCE, f.SUSPENDU,
             t.PRIXACHAT, t.REMISE_PROMOTION
      FROM ARTFOU1 f
      LEFT JOIN FOUADR1 fa ON fa.CODE = f.CODE AND fa.SIT_CODE = '000'
      LEFT JOIN ARTFOU2 t ON t.IDARTFOU1 = f.NO_ID
      WHERE f.ART_NO_ID = $1
      ORDER BY f.PREFERENCE
    `, [req.params.id]);

    res.json({
      article: article.rows[0],
      gtins: gtins.rows,
      stock: stock.rows,
      prix: prix.rows,
      fournisseurs: fournisseurs.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/articles/:id/referentiel - Fiche complète
router.get('/:id/referentiel', async (req, res) => {
  try {
    const pool = getPool();
    const id = req.params.id;

    const article = await pool.query(`
      SELECT
        a.NO_ID, a.CODEIN, a.LIBELLE1, a.LIBELLE2, a.LIB_TICKET,
        a.TAX_CODE, a.ACTIF, a.SUSPENDU, a.UTILISABLE,
        a.SUIVIDATECREATION, a.SUIVIDATEMODIF,
        ai.PRIX_VENTE_MINI, ai.PRIX_VENTE_MAXI,
        ai.ECO_HT, ai.ECO_TTC, ai.ON_WEB,
        ai.INTERDIT_REMISE, ai.NOMPHOTO,
        ai.DATEDEBVENTE, ai.DATEFINVENTE,
        ai.PV_CONSEILLE, ai.FRAISLOGISTIC,
        ai.COMMENTAIRE,
        n.CODE AS NOM_CODE, n.LIBELLE AS NOM_LIBELLE, n.NIVEAU AS NOM_NIVEAU,
        n.CHEMIN_PERE AS NOM_CHEMIN_PERE
      FROM ARTICLES a
      LEFT JOIN ARTICLE_INFOSUP ai ON ai.ARTNOID = a.NO_ID
      LEFT JOIN NOMENCLATURE n ON n.NO_ID = a.NOM_NO_ID
      WHERE a.NO_ID = $1
    `, [id]);

    if (!article.rows.length)
      return res.status(404).json({ error: 'Article introuvable' });

    const gtins = await pool.query(
      `SELECT GTIN, PREFERENTIEL FROM ART_GTIN WHERE IDARTICLE = $1 ORDER BY PREFERENTIEL DESC`,
      [id]
    );

    const gammes = await pool.query(`
      SELECT g.CODE AS GAMME_CODE, g.LIBELLE AS GAMME_LIBELLE,
             s.CODE AS SAISON_CODE, s.LIBELLE AS SAISON_LIBELLE,
             ags.IDGAMME, ags.IDSAISON
      FROM ART_GAMME_SAISON ags
      JOIN GAMMES g ON g.NO_ID = ags.IDGAMME
      LEFT JOIN SAISONS s ON s.NO_ID = ags.IDSAISON
      WHERE ags.ARTNOID = $1
    `, [id]);

    const stock = await pool.query(`
      SELECT Site, QTE, Prmp, ValStock, PV, StockDispo,
             DerniereVente, DerniereReception, StockMort
      FROM Cube_Stock WHERE ArtNoId = $1 ORDER BY Site
    `, [id]);

    const prixAchat = await pool.query(
      `SELECT ArtNoId, PA FROM Cube_PA WHERE ArtNoId = $1`,
      [id]
    );

    const prixVente = await pool.query(
      `SELECT Site, PV FROM Cube_PV WHERE ArtNoId = $1 ORDER BY Site`,
      [id]
    );

    const fournisseurs = await pool.query(`
      SELECT f.CODE AS CODEFOU, fa.RAISONSOCIALE AS NOM_FOU,
             f.REFERENCE AS REF_FOU, f.EAN13, f.ITF,
             f.QTEUA AS PCB, f.SPCB, f.DELAI, f.SECURITE,
             f.PREFERENCE, f.SUSPENDU,
             t.PRIXACHAT, t.REMISE_PROMOTION
      FROM ARTFOU1 f
      LEFT JOIN FOUADR1 fa ON fa.CODE = f.CODE AND fa.SIT_CODE = '000'
      LEFT JOIN ARTFOU2 t ON t.IDARTFOU1 = f.NO_ID
      WHERE f.ART_NO_ID = $1
      ORDER BY f.PREFERENCE
    `, [id]);

    const mvtAgreges = await pool.query(`
      SELECT
        MAX(CASE WHEN GenreMvt = 1 THEN DatMvt END) AS DERNIERE_ENTREE,
        MAX(CASE WHEN GenreMvt = 3 THEN DatMvt END) AS DERNIERE_VENTE,
        SUM(CASE WHEN GenreMvt = 3 THEN QteMvt ELSE 0 END) AS QTE_TOTALE_VENDUE,
        SUM(CASE WHEN GenreMvt = 3 THEN MntMvtTTC ELSE 0 END) AS CA_TTC_TOTAL,
        SUM(CASE WHEN GenreMvt = 3 THEN MargeMvt ELSE 0 END) AS MARGE_TOTALE
      FROM MvtArt WHERE ArtNoId = $1
    `, [id]);

    let stockRemballe = [];
    try {
      const remb = await pool.query(
        `SELECT Site, QTE AS QTE_REMBALLE, DateSaisie FROM InvRemballe WHERE ArtNoId = $1 ORDER BY Site`,
        [id]
      );
      stockRemballe = remb.rows;
    } catch (e) { /* Table optionnelle */ }

    let notes = [];
    try {
      const notesRes = await pool.query(
        `SELECT Note, DateSaisie, Auteur FROM NoteArticles WHERE ArtNoId = $1 ORDER BY DateSaisie DESC`,
        [id]
      );
      notes = notesRes.rows;
    } catch (e) { /* Table optionnelle */ }

    res.json({
      article: article.rows[0],
      gtins: gtins.rows,
      gammes: gammes.rows,
      stock: stock.rows,
      stock_remballe: stockRemballe,
      prix: {
        achat: prixAchat.rows[0]?.pa ?? null,
        vente_par_site: prixVente.rows,
      },
      fournisseurs: fournisseurs.rows,
      performance: mvtAgreges.rows[0],
      notes,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/articles/:id/mouvements?dateDebut=&dateFin=&site=&page=&limit=
router.get('/:id/mouvements', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '', dateFin = '', site = '', page = 1, limit = 100 } = req.query;
    const pageNum  = Math.max(1, parseInt(page)   || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 100, 1000));
    const offsetNum = (pageNum - 1) * limitNum;

    const result = await pool.query(`
      SELECT DatMvt, Site, LibMvt, GenreMvt, QteMvt, ValMvt,
             MntMvtHt, MntMvtTTC, MargeMvt, QteStock, Prmp, CODEFOU
      FROM MvtArt
      WHERE ArtNoId = $1
        AND DatMvt BETWEEN $2 AND $3
        AND Site LIKE $4
      ORDER BY DatMvt DESC
      LIMIT ${limitNum} OFFSET ${offsetNum}
    `, [req.params.id, dateDebut || '2000-01-01', dateFin || '2099-12-31', `%${site}%`]);

    res.json({ page: pageNum, limit: limitNum, mouvements: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/articles/:id/mensuel?dateDebut=&dateFin=&site=
// Ventes + stock fin de mois, mois par mois, par site
router.get('/:id/mensuel', async (req, res) => {
  try {
    const pool = getPool();
    const { dateDebut = '2024-01-01', dateFin = '2099-12-31', site = '' } = req.query;

    // Ventes par mois et par site
    const ventes = await pool.query(`
      SELECT
        TO_CHAR(DatMvt, 'YYYY-MM') AS mois,
        Site,
        COUNT(*)           AS nb_passages,
        SUM(QteMvt)        AS qte_vendue,
        SUM(MntMvtHt)      AS ca_ht,
        SUM(MntMvtTTC)     AS ca_ttc,
        SUM(MargeMvt)      AS marge,
        CASE WHEN SUM(MntMvtTTC) > 0
          THEN ROUND(SUM(MargeMvt) / SUM(MntMvtTTC) * 100, 2)
          ELSE 0 END       AS taux_marge
      FROM MvtArt
      WHERE ArtNoId = $1
        AND GenreMvt = 3
        AND DatMvt BETWEEN $2 AND $3
        AND Site LIKE $4
      GROUP BY TO_CHAR(DatMvt, 'YYYY-MM'), Site
      ORDER BY mois DESC, Site
    `, [req.params.id, dateDebut, dateFin, `%${site}%`]);

    // Réceptions par mois et par site
    const receptions = await pool.query(`
      SELECT
        TO_CHAR(DatMvt, 'YYYY-MM') AS mois,
        Site,
        COUNT(*)           AS nb_receptions,
        SUM(QteMvt)        AS qte_recue
      FROM MvtArt
      WHERE ArtNoId = $1
        AND GenreMvt = 1
        AND DatMvt BETWEEN $2 AND $3
        AND Site LIKE $4
      GROUP BY TO_CHAR(DatMvt, 'YYYY-MM'), Site
      ORDER BY mois DESC, Site
    `, [req.params.id, dateDebut, dateFin, `%${site}%`]);

    // Stock fin de mois : dernier QteStock de chaque mois par site
    const stocks = await pool.query(`
      SELECT DISTINCT ON (TO_CHAR(DatMvt, 'YYYY-MM'), Site)
        TO_CHAR(DatMvt, 'YYYY-MM') AS mois,
        Site,
        QteStock AS stock_fin_mois,
        Prmp     AS prmp_fin_mois
      FROM MvtArt
      WHERE ArtNoId = $1
        AND DatMvt BETWEEN $2 AND $3
        AND Site LIKE $4
      ORDER BY TO_CHAR(DatMvt, 'YYYY-MM'), Site, DatMvt DESC
    `, [req.params.id, dateDebut, dateFin, `%${site}%`]);

    // Assembler par mois+site
    const key = (mois, site) => `${mois}|${site}`;
    const byKey = (rows) => Object.fromEntries(rows.map(r => [key(r.mois, r.site), r]));

    const sv = byKey(ventes.rows);
    const sr = byKey(receptions.rows);
    const ss = byKey(stocks.rows);

    const keys = [...new Set([
      ...Object.keys(sv), ...Object.keys(sr), ...Object.keys(ss),
    ])].sort().reverse();

    const data = keys.map(k => {
      const [mois, site] = k.split('|');
      return {
        mois,
        site,
        stock_fin_mois: ss[k]?.stock_fin_mois ?? null,
        prmp_fin_mois:  ss[k]?.prmp_fin_mois  ?? null,
        ventes:         sv[k] ? {
          nb_passages: sv[k].nb_passages,
          qte_vendue:  sv[k].qte_vendue,
          ca_ht:       sv[k].ca_ht,
          ca_ttc:      sv[k].ca_ttc,
          marge:       sv[k].marge,
          taux_marge:  sv[k].taux_marge,
        } : null,
        receptions: sr[k] ? {
          nb_receptions: sr[k].nb_receptions,
          qte_recue:     sr[k].qte_recue,
        } : null,
      };
    });

    res.json({ artNoId: req.params.id, dateDebut, dateFin, data });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
