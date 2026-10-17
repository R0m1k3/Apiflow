const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { parseText, parseEnum, parseCode, parsePagination, parseDateRange, likeContains } = require('../utils/params');
const { SqlParams, dateRange, where } = require('../utils/sql');
const { HttpError } = require('../utils/errors');

const STATUTS = ['en_cours', 'passees', 'futures', 'toutes'];

// Filtre de statut sur les colonnes début/fin données
function statutFilter(statut, deb, fin) {
  if (statut === 'en_cours') return `CURRENT_DATE BETWEEN ${deb} AND ${fin}`;
  if (statut === 'passees')  return `${fin} < CURRENT_DATE`;
  if (statut === 'futures')  return `${deb} > CURRENT_DATE`;
  return null;
}

// GET /api/publicites?search=&site=&dateDebut=&dateFin=&statut=en_cours|passees|futures|toutes&page=&limit=
router.get('/', async (req, res) => {
  const q = req.query;
  const search = parseText(q.search, 'search', { max: 100 });
  const site   = parseText(q.site, 'site', { max: 20 });
  const statut = parseEnum(q.statut, 'statut', STATUTS, { def: '' });
  const { debut, fin } = parseDateRange(q, { defDebut: '2000-01-01', defFin: '2099-12-31' });
  const { page, limit, offset } = parsePagination(q, { defLimit: 50, maxLimit: 500 });

  const p = new SqlParams();
  const conds = [dateRange('e.tcrd_datedeb', p, debut, fin)];
  if (search) {
    const s = p.add(likeContains(search));
    conds.push(`(e.tcr_libelle ILIKE ${s} OR e.tcr_code ILIKE ${s})`);
  }
  if (site) conds.push(`e.site = ${p.add(site)}`);
  conds.push(statutFilter(statut, 'e.tcrd_datedeb', 'e.tcrd_datefin'));
  const whereSql = where(conds);
  const pool = getPool();

  const countValues = [...p.values];
  const limitPh = p.add(limit);
  const offsetPh = p.add(offset);

  const [countResult, result] = await Promise.all([
    // Total (pour la pagination)
    pool.query(`SELECT COUNT(*) AS total FROM pub_ecoulement e ${whereSql}`, countValues),
    // Synthèse par pub (une ligne par pub × site)
    pool.query(`
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
      ${whereSql}
      ORDER BY e.tcrd_datedeb DESC, e.tcr_code, e.site
      LIMIT ${limitPh} OFFSET ${offsetPh}
    `, p.values),
  ]);

  const total = parseInt(countResult.rows[0].total, 10);
  res.json({
    page,
    limit,
    total,
    pages: Math.ceil(total / limit),
    publicites: result.rows,
  });
});

// GET /api/publicites/historique?search=&dateDebut=&dateFin=&statut=
// Liste complète depuis pub_entetes (inclut les pubs sans données d'écoulement)
router.get('/historique', async (req, res) => {
  const q = req.query;
  const search = parseText(q.search, 'search', { max: 100 });
  const statut = parseEnum(q.statut, 'statut', STATUTS, { def: '' });
  const { debut, fin } = parseDateRange(q, { defDebut: '2000-01-01', defFin: '2099-12-31' });

  const p = new SqlParams();
  const conds = [dateRange('e.ent_datdeb', p, debut, fin)];
  if (search) {
    const s = p.add(likeContains(search));
    conds.push(`(e.ent_titre ILIKE ${s} OR e.ent_npub ILIKE ${s})`);
  }
  conds.push(statutFilter(statut, 'e.ent_datdeb', 'e.ent_datfin'));

  const result = await getPool().query(`
      SELECT
        e.ent_npub                                          AS tcr_code,
        e.ent_titre                                         AS intitule,
        e.ent_datdeb                                        AS date_debut,
        e.ent_datfin                                        AS date_fin,
        CASE
          WHEN CURRENT_DATE BETWEEN e.ent_datdeb AND e.ent_datfin THEN 'en_cours'
          WHEN e.ent_datfin < CURRENT_DATE                         THEN 'passee'
          ELSE 'future'
        END                                                 AS statut,
        COUNT(DISTINCT ec.site)                             AS nb_sites,
        SUM(ec.ca_pub_periode_pub)                          AS ca_total,
        SUM(ec.qte_vendue_pub)                              AS qte_totale
      FROM pub_entetes e
      LEFT JOIN pub_ecoulement ec ON ec.tcr_code = e.ent_npub
      ${where(conds)}
      GROUP BY e.ent_npub, e.ent_titre, e.ent_datdeb, e.ent_datfin
      ORDER BY e.ent_datdeb DESC
    `, p.values);

  res.json({ total: result.rows.length, publicites: result.rows });
});

// GET /api/publicites/:code — synthèse par site + articles
router.get('/:code', async (req, res) => {
  const code = parseCode(req.params.code, 'code');
  const site = parseText(req.query.site, 'site', { max: 20 });
  const pool = getPool();

  const p = new SqlParams([code]);
  const siteFilter = site ? `AND site = ${p.add(site)}` : '';

  const [synthese, articles] = await Promise.all([
    // Synthèse par site
    pool.query(`
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
      WHERE e.tcr_code = $1 ${siteFilter}
      ORDER BY e.site
    `, p.values),
    // Articles de la pub
    pool.query(`
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
      WHERE d.tcr_code = $1 ${siteFilter}
      ORDER BY d.site, d.ca_pub_periode_pub DESC NULLS LAST
    `, p.values),
  ]);

  if (!synthese.rows.length) throw new HttpError(404, 'Publication introuvable');

  const s0 = synthese.rows[0];
  res.json({
    tcr_code:   s0.tcr_code,
    intitule:   s0.intitule,
    date_debut: s0.date_debut,
    date_fin:   s0.date_fin,
    datecalcul: s0.datecalcul,
    synthese_par_site: synthese.rows,
    articles: articles.rows,
  });
});

module.exports = router;
