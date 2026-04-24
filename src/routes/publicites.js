const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/publicites?search=&site=&dateDebut=&dateFin=&statut=en_cours|passees|futures|toutes&page=&limit=
router.get('/', async (req, res) => {
  try {
    const pool = getPool();
    const {
      search = '',
      site = '',
      dateDebut = '',
      dateFin = '',
      statut = '',
      page = 1,
      limit = 50,
    } = req.query;

    const pageNum  = Math.max(1, parseInt(page) || 1);
    const limitNum = Math.max(1, Math.min(parseInt(limit) || 50, 500));
    const offset   = (pageNum - 1) * limitNum;

    // Filtre statut
    let statutFilter = '';
    if      (statut === 'en_cours') statutFilter = `AND CURRENT_DATE BETWEEN e.tcrd_datedeb AND e.tcrd_datefin`;
    else if (statut === 'passees')  statutFilter = `AND e.tcrd_datefin < CURRENT_DATE`;
    else if (statut === 'futures')  statutFilter = `AND e.tcrd_datedeb > CURRENT_DATE`;

    const deb = dateDebut || '2000-01-01';
    const fin = dateFin   || '2099-12-31';

    // Synthèse par pub (agrégat multi-sites)
    const result = await pool.query(`
      SELECT
        e.tcr_code,
        e.tcr_libelle                                   AS intitule,
        e.tcrd_datedeb                                  AS date_debut,
        e.tcrd_datefin                                  AS date_fin,
        e.datecalcul,
        e.site,
        e.ca_pub_periode_pub,
        e.qte_vendue_pub,
        e.ca_total_periode_pub,
        ROUND(e.pourc_capub_catotal::NUMERIC, 2)        AS pourc_capub_catotal,
        e.client_pub_periode,
        e.client_total_periode,
        e.stock_datedebut,
        e.stock_datefin,
        e.ca_pub_30_jours,
        e.ca_pub_60_jours,
        e.ca_pub_90_jours,
        e.ca_pub_180_jours,
        e.ca_depuis_finpub,
        ROUND(e.taux_sortie::NUMERIC, 2)                AS taux_sortie,
        ROUND(e.marge::NUMERIC, 4)                      AS marge,
        ROUND(e.taux_marge::NUMERIC, 2)                 AS taux_marge,
        CASE
          WHEN CURRENT_DATE BETWEEN e.tcrd_datedeb AND e.tcrd_datefin THEN 'en_cours'
          WHEN e.tcrd_datefin < CURRENT_DATE                           THEN 'passee'
          ELSE 'future'
        END                                             AS statut,
        (SELECT COUNT(DISTINCT d.artnoid)
         FROM pub_ecoulement_detail d
         WHERE d.tcr_code = e.tcr_code AND d.site = e.site)  AS nb_articles
      FROM pub_ecoulement e
      WHERE ($1 = '' OR e.tcr_libelle ILIKE '%' || $1 || '%' OR e.tcr_code ILIKE '%' || $1 || '%')
        AND ($2 = '' OR e.site ILIKE '%' || $2 || '%')
        AND e.tcrd_datedeb >= $3
        AND e.tcrd_datedeb <= $4
        ${statutFilter}
      ORDER BY e.tcrd_datedeb DESC, e.tcr_code, e.site
      LIMIT $5 OFFSET $6
    `, [search, site, deb, fin, limitNum, offset]);

    res.json({
      page: pageNum,
      limit: limitNum,
      publicites: result.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/publicites/:code — synthèse par site + articles
router.get('/:code', async (req, res) => {
  try {
    const pool = getPool();
    const { code } = req.params;
    const { site = '' } = req.query;

    // Synthèse par site
    const synthese = await pool.query(`
      SELECT
        e.tcr_code,
        e.tcr_libelle                                   AS intitule,
        e.tcrd_datedeb                                  AS date_debut,
        e.tcrd_datefin                                  AS date_fin,
        e.datecalcul,
        e.site,
        e.ca_pub_periode_pub,
        e.qte_vendue_pub,
        e.ca_total_periode_pub,
        ROUND(e.pourc_capub_catotal::NUMERIC, 2)        AS pourc_capub_catotal,
        e.client_pub_periode,
        e.client_total_periode,
        e.stock_datedebut,
        e.stock_datefin,
        e.ca_pub_30_jours,
        e.ca_pub_60_jours,
        e.ca_pub_90_jours,
        e.ca_pub_180_jours,
        e.ca_depuis_finpub,
        ROUND(e.taux_sortie::NUMERIC, 2)                AS taux_sortie,
        ROUND(e.marge::NUMERIC, 4)                      AS marge,
        ROUND(e.taux_marge::NUMERIC, 2)                 AS taux_marge
      FROM pub_ecoulement e
      WHERE e.tcr_code = $1
        AND ($2 = '' OR e.site ILIKE '%' || $2 || '%')
      ORDER BY e.site
    `, [code, site]);

    if (!synthese.rows.length) {
      return res.status(404).json({ error: 'Publication introuvable' });
    }

    // Articles de la pub
    const articles = await pool.query(`
      SELECT
        d.site,
        d.artnoid,
        d.codein,
        d.libelle,
        d.prmp,
        d.pa,
        d.pv,
        d.prixpub,
        d.qte_vendue_pub,
        ROUND(d.ca_pub_periode_pub::NUMERIC, 4)  AS ca_pub_periode_pub,
        d.stock_datedebut,
        d.stock_datefin,
        d.ca_pub_30_jours,
        d.ca_pub_60_jours,
        d.ca_pub_90_jours,
        d.ca_pub_180_jours,
        d.ca_depuis_finpub,
        ROUND(d.taux_sortie::NUMERIC, 2)          AS taux_sortie,
        ROUND(d.marge::NUMERIC, 4)                AS marge,
        ROUND(d.taux_marge::NUMERIC, 2)           AS taux_marge
      FROM pub_ecoulement_detail d
      WHERE d.tcr_code = $1
        AND ($2 = '' OR d.site ILIKE '%' || $2 || '%')
      ORDER BY d.site, d.ca_pub_periode_pub DESC
    `, [code, site]);

    res.json({
      tcr_code:   synthese.rows[0].tcr_code,
      intitule:   synthese.rows[0].intitule,
      date_debut: synthese.rows[0].date_debut,
      date_fin:   synthese.rows[0].date_fin,
      datecalcul: synthese.rows[0].datecalcul,
      synthese_par_site: synthese.rows,
      articles: articles.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
