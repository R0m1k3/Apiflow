// Prix de vente (historique décodé de LIG_TC_*) et promotions caisse (TC_REMISE*).
const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { parseId, parseDate, parsePositiveInt, parseText, parsePagination, likeContains } = require('../utils/params');
const { SqlParams, where } = require('../utils/sql');
const { HttpError } = require('../utils/errors');
const { todayParis, addDays } = require('../utils/dates');

// GET /api/prix/article/:id — historique des prix par site (000 = centrale) et tarif
router.get('/article/:id', async (req, res) => {
  const id = parseId(req.params.id);
  const { rows } = await getPool().query(`
    SELECT h.site, h.tarif_client_no_id, tc.tc_code, tc.tc_nom, h.date_effet, h.prix,
           LEAD(h.date_effet) OVER w - 1 AS date_fin,
           h.prix - LAG(h.prix) OVER w   AS variation
    FROM prix_vente_historique h
    LEFT JOIN tarif_client tc ON tc.no_id = h.tarif_client_no_id
    WHERE h.article_no_id = $1
    WINDOW w AS (PARTITION BY h.site, h.tarif_client_no_id ORDER BY h.date_effet)
    ORDER BY h.site, h.tarif_client_no_id, h.date_effet`, [id]);
  if (!rows.length) throw new HttpError(404, 'Aucun historique de prix pour cet article');
  res.json({ article: id, historique: rows });
});

// GET /api/prix/changements?depuis=YYYY-MM-DD&site=292&sens=hausse|baisse&page&limit
// Articles dont le prix a changé depuis une date (ancien → nouveau prix)
router.get('/changements', async (req, res) => {
  const depuis = parseDate(req.query.depuis, 'depuis', { def: addDays(todayParis(), -7) });
  const site = parseText(req.query.site, 'site', { max: 10 }) || '292';
  const sens = parseText(req.query.sens, 'sens', { max: 10 });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });
  const p = new SqlParams([depuis, site]);
  const conds = ['x.date_effet >= $1::date', 'x.ancien_prix IS NOT NULL', 'x.prix IS DISTINCT FROM x.ancien_prix'];
  if (sens === 'hausse') conds.push('x.prix > x.ancien_prix');
  if (sens === 'baisse') conds.push('x.prix < x.ancien_prix');
  const { rows } = await getPool().query(`
    SELECT x.article_no_id, a.codein, a.libelle1, x.date_effet, x.ancien_prix, x.prix AS nouveau_prix,
           ROUND((x.prix - x.ancien_prix) / NULLIF(x.ancien_prix, 0) * 100, 1) AS variation_pct
    FROM (
      SELECT h.*, LAG(h.prix) OVER (PARTITION BY h.article_no_id, h.site, h.tarif_client_no_id ORDER BY h.date_effet) AS ancien_prix
      FROM prix_vente_historique h
      WHERE h.site = $2
    ) x
    LEFT JOIN articles a ON a.no_id = x.article_no_id
    ${where(conds)}
    ORDER BY x.date_effet DESC, a.libelle1
    LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}`, p.values);
  res.json({ depuis, site, page, limit, changements: rows });
});

// GET /api/prix/etiquettes?traite=0|1&page&limit — changements de prix à étiqueter (Etiquettes_Auto_PV)
router.get('/etiquettes', async (req, res) => {
  const traite = parseText(req.query.traite, 'traite', { max: 1 });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });
  const p = new SqlParams();
  const conds = [];
  if (traite === '0' || traite === '1') conds.push(`COALESCE(e.traite, 0) = ${p.add(Number(traite))}`);
  const { rows } = await getPool().query(`
    SELECT e.no_id, e.article_no_id, e.codin, a.libelle1, e.gtin, e.code_fourn, e.prixach, e.lig_prix AS prix_vente,
           e.pub, e.colisage, e.qte_stock, e.nb_etiqu_u, e.nb_etiqu_g, e.dateetiq, e.traite, e.datetraitement
    FROM etiquettes_auto_pv e
    LEFT JOIN articles a ON a.no_id = e.article_no_id
    ${where(conds)}
    ORDER BY e.dateetiq DESC NULLS LAST
    LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}`, p.values);
  res.json({ page, limit, etiquettes: rows });
});

// GET /api/prix/promotions?date=YYYY-MM-DD&site=292&search=
// Opérations de remise actives à une date (défaut : aujourd'hui)
router.get('/promotions', async (req, res) => {
  const date = parseDate(req.query.date, 'date', { def: todayParis() });
  const site = parseText(req.query.site, 'site', { max: 10 });
  const search = parseText(req.query.search, 'search', { max: 100 });
  const p = new SqlParams([date]);
  const conds = ['d.datedeb < $1::date + 1', '(d.datefin IS NULL OR d.datefin >= $1::date)'];
  if (search) conds.push(`(r.tcr_code ILIKE ${p.add(likeContains(search))} OR r.tcr_libelle ILIKE $${p.values.length})`);
  if (site) conds.push(`EXISTS (SELECT 1 FROM remise_site rs WHERE rs.tc_remise_date_no_id = d.no_id AND TRIM(rs.sitecode) = ${p.add(site)})`);
  const { rows } = await getPool().query(`
    SELECT r.no_id AS remise_id, r.tcr_code, r.tcr_libelle, r.tcr_nature, r.tcr_type,
           d.no_id AS periode_id, d.datedeb, d.datefin, d.regime,
           (SELECT COUNT(*) FROM tc_remise_filtre f WHERE f.tc_remise_date_no_id = d.no_id AND f.tcrf_type = 4) AS nb_articles,
           (SELECT array_agg(DISTINCT TRIM(rs.sitecode)) FROM remise_site rs WHERE rs.tc_remise_date_no_id = d.no_id) AS sites
    FROM tc_remise_date d
    JOIN tc_remise r ON r.no_id = d.tc_remise_no_id
    ${where(conds)}
    ORDER BY d.datedeb DESC, r.tcr_code`, p.values);
  res.json({ date, site: site || null, promotions: rows });
});

// GET /api/prix/promotions/:periodeId/articles — articles d'une période de remise + ventes pendant la période
router.get('/promotions/:periodeId/articles', async (req, res) => {
  const id = parseId(req.params.periodeId, 'periodeId');
  const limit = parsePositiveInt(req.query.limit, 'limit', { def: 500, max: 5000, clamp: true });
  const head = await getPool().query(`
    SELECT d.no_id, d.datedeb, d.datefin, r.tcr_code, r.tcr_libelle
    FROM tc_remise_date d JOIN tc_remise r ON r.no_id = d.tc_remise_no_id WHERE d.no_id = $1`, [id]);
  if (!head.rows.length) throw new HttpError(404, 'Période de remise introuvable');
  const { rows } = await getPool().query(`
    SELECT f.tcrf_noid AS artnoid, a.codein, a.libelle1, f.tcrf_remise AS remise, f.remise_pct,
           ra.quantite_cons, ra.pos_page, ra.reassortiment,
           v.qte_vendue, v.ca_ttc
    FROM tc_remise_filtre f
    JOIN tc_remise_date d ON d.no_id = f.tc_remise_date_no_id
    LEFT JOIN articles a ON a.no_id = f.tcrf_noid
    LEFT JOIN LATERAL (SELECT * FROM remise_ref_article r WHERE r.tc_remise_filtre_no_id = f.no_id LIMIT 1) ra ON true
    LEFT JOIN LATERAL (
      SELECT ROUND(SUM(-m.qtemvt), 0) AS qte_vendue, ROUND(SUM(-m.mntmvtttc), 2) AS ca_ttc
      FROM mvtart m
      WHERE m.artnoid = f.tcrf_noid AND m.mvttypnoid IN (11, 12)
        AND m.datmvt >= d.datedeb::date AND m.datmvt < COALESCE(d.datefin::date, CURRENT_DATE) + 1
    ) v ON true
    WHERE f.tc_remise_date_no_id = $1 AND f.tcrf_type = 4
    ORDER BY v.ca_ttc DESC NULLS LAST
    LIMIT $2`, [id, limit]);
  res.json({ ...head.rows[0], articles: rows });
});

module.exports = router;
