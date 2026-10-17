const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const {
  parseText, parseId, parseDate, parseDateRange, parsePagination, likeContains,
} = require('../utils/params');
const { SqlParams, dateRange, where } = require('../utils/sql');
const { todayParis, firstDayOfMonth } = require('../utils/dates');

// Colonne réelle : nbjoursdernierrereception (double « r », cf. init.sql).
// Alias pour conserver la clé JSON attendue (cohérente avec nbjoursdernierevente).
const NBJ_RECEPTION = 's.nbjoursdernierrereception AS nbjoursdernierereception';

// GET /api/stock?site=&page=&limit= - Stock global
router.get('/', async (req, res) => {
  const site = parseText(req.query.site, 'site', { max: 20 });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });

  const p = new SqlParams();
  const conds = site ? [`s.site = ${p.add(site)}`] : [];

  const result = await getPool().query(`
      SELECT
        a.codein, a.libelle1,
        s.site, s.qte, s.prmp, s.valstock, s.pv,
        s.stockdispo, s.stockmort, s.stockcolis,
        s.dernierevente, s.dernierereception, s.premierevente,
        s.nbjoursderniermouvement, s.nbjoursdernierevente,
        ${NBJ_RECEPTION}, s.interditachat
      FROM cube_stock s
      JOIN articles a ON a.no_id = s.artnoid
      ${where(conds)}
      ORDER BY a.libelle1, s.site, s.artnoid
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  res.json({ page, limit, stock: result.rows });
});

// GET /api/stock/article/:id - Stock d'un article par site
router.get('/article/:id', async (req, res) => {
  const id = parseId(req.params.id);

  const result = await getPool().query(`
      SELECT
        s.site, s.qte, s.prmp, s.valstock, s.pv,
        s.stockdispo, s.stockmort, s.stockcolis,
        s.dernierevente, s.dernierereception, s.premierevente,
        s.nbjoursderniermouvement, s.nbjoursdernierevente,
        ${NBJ_RECEPTION}, s.interditachat, s.codefou
      FROM cube_stock s
      WHERE s.artnoid = $1
      ORDER BY s.site
    `, [id]);

  res.json({ artNoId: id, stock: result.rows });
});

// GET /api/stock/site/:site - Tous les articles d'un site
router.get('/site/:site', async (req, res) => {
  const site = parseText(req.params.site, 'site', { max: 20 });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });

  const result = await getPool().query(`
      SELECT
        a.no_id AS art_no_id, a.codein, a.libelle1,
        s.qte, s.prmp, s.valstock, s.pv,
        s.stockdispo, s.stockmort, s.dernierevente, s.dernierereception,
        s.nbjoursdernierevente, ${NBJ_RECEPTION}
      FROM cube_stock s
      JOIN articles a ON a.no_id = s.artnoid
      WHERE s.site = $1
      ORDER BY s.qte DESC NULLS LAST, s.artnoid
      LIMIT $2 OFFSET $3
    `, [site, limit, offset]);

  res.json({ site, page, limit, stock: result.rows });
});

// GET /api/stock/article/:id/historique?date=&site= - Stock d'un article à une date donnée
// Utilise qtestock de mvtart (stock enregistré au moment de chaque mouvement)
router.get('/article/:id/historique', async (req, res) => {
  const id = parseId(req.params.id);
  const date = parseDate(req.query.date, 'date', { def: todayParis() });
  const site = parseText(req.query.site, 'site', { max: 20 });

  const p = new SqlParams([id, date]);
  // Filtre partiel sur le site (documenté)
  const siteFilter = site ? `AND site LIKE ${p.add(likeContains(site))}` : '';

  const result = await getPool().query(`
      SELECT DISTINCT ON (site)
        site,
        qtestock AS qte,
        prmp,
        datmvt AS date_dernier_mouvement,
        libmvt AS libelle_dernier_mouvement
      FROM mvtart
      WHERE artnoid = $1
        AND datmvt < $2::date + 1
        ${siteFilter}
      ORDER BY site, datmvt DESC
    `, p.values);

  res.json({ artNoId: id, date, stock: result.rows });
});

// GET /api/stock/article/:id/periode?dateDebut=&dateFin=&site= - Stock + ventes sur une période
router.get('/article/:id/periode', async (req, res) => {
  const id = parseId(req.params.id);
  const today = todayParis();
  const { debut, fin } = parseDateRange(req.query, { defDebut: firstDayOfMonth(today), defFin: today });
  const site = parseText(req.query.site, 'site', { max: 20 });
  const pool = getPool();

  // Filtre site partiel (documenté) — même placeholder ($2) dans toutes les requêtes
  const siteFilter = site ? 'AND site LIKE $2' : '';
  const base = site ? [id, likeContains(site)] : [id];

  // Stock au début de la période (dernier mouvement avant dateDebut)
  const pDebut = new SqlParams(base);
  const qStockDebut = pool.query(`
      SELECT DISTINCT ON (site)
        site, qtestock AS qte, prmp, datmvt AS date_mouvement
      FROM mvtart
      WHERE artnoid = $1 ${siteFilter}
        AND datmvt < ${pDebut.add(debut)}::date
      ORDER BY site, datmvt DESC
    `, pDebut.values);

  // Stock à la fin de la période (dernier mouvement <= dateFin, journée incluse)
  const pFin = new SqlParams(base);
  const qStockFin = pool.query(`
      SELECT DISTINCT ON (site)
        site, qtestock AS qte, prmp, datmvt AS date_mouvement
      FROM mvtart
      WHERE artnoid = $1 ${siteFilter}
        AND datmvt < ${pFin.add(fin)}::date + 1
      ORDER BY site, datmvt DESC
    `, pFin.values);

  // Ventes sur la période (genremvt = 3)
  const pV = new SqlParams(base);
  const qVentes = pool.query(`
      SELECT
        site,
        COUNT(*) AS nb_passages,
        SUM(qtemvt) AS qte_vendue,
        SUM(mntmvtht) AS ca_ht,
        SUM(mntmvtttc) AS ca_ttc,
        SUM(margemvt) AS marge,
        CASE WHEN SUM(mntmvtht) != 0
          THEN ROUND(SUM(margemvt) / ABS(SUM(mntmvtht)) * 100, 2)
          ELSE 0 END AS taux_marge,
        MIN(datmvt) AS premiere_vente,
        MAX(datmvt) AS derniere_vente
      FROM mvtart
      WHERE artnoid = $1 ${siteFilter}
        AND genremvt = 3
        AND ${dateRange('datmvt', pV, debut, fin)}
      GROUP BY site
    `, pV.values);

  // Réceptions sur la période (genremvt = 1)
  const pR = new SqlParams(base);
  const qReceptions = pool.query(`
      SELECT
        site,
        COUNT(*) AS nb_receptions,
        SUM(qtemvt) AS qte_recue
      FROM mvtart
      WHERE artnoid = $1 ${siteFilter}
        AND genremvt = 1
        AND ${dateRange('datmvt', pR, debut, fin)}
      GROUP BY site
    `, pR.values);

  const [stockDebut, stockFin, ventes, receptions] =
    await Promise.all([qStockDebut, qStockFin, qVentes, qReceptions]);

  // Indexation par site pour l'assemblage
  const bysite = (rows) => Object.fromEntries(rows.map(r => [r.site, r]));
  const sd = bysite(stockDebut.rows);
  const sf = bysite(stockFin.rows);
  const sv = bysite(ventes.rows);
  const sr = bysite(receptions.rows);

  const sites = [...new Set([
    ...Object.keys(sd), ...Object.keys(sf),
    ...Object.keys(sv), ...Object.keys(sr),
  ])].sort();

  const data = sites.map(s => ({
    site: s,
    stock_debut: sd[s] ? { qte: sd[s].qte, prmp: sd[s].prmp, au: sd[s].date_mouvement } : null,
    stock_fin:   sf[s] ? { qte: sf[s].qte, prmp: sf[s].prmp, au: sf[s].date_mouvement } : null,
    ventes:      sv[s] ?? null,
    receptions:  sr[s] ?? null,
  }));

  res.json({ artNoId: id, dateDebut: debut, dateFin: fin, data });
});

// GET /api/stock/valorisation - Valorisation du stock par site
router.get('/valorisation', async (req, res) => {
  const result = await getPool().query(`
      SELECT
        site,
        COUNT(*) AS nb_articles,
        SUM(qte) AS qte_totale,
        SUM(valstock) AS val_stock,
        SUM(qte * pv) AS val_pv_theorique,
        AVG(prmp) AS prmp_moyen
      FROM cube_stock
      WHERE qte > 0
      GROUP BY site
      ORDER BY val_stock DESC NULLS LAST
    `);

  res.json({ valorisation: result.rows });
});

module.exports = router;
