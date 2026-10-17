const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const {
  parseText, parseId, parsePositiveInt, parsePagination, parseDateRange, likeContains,
} = require('../utils/params');
const { SqlParams, dateRange, where } = require('../utils/sql');

// Conditions communes : période + site (code exact) + article optionnel
function communes(p, alias, q, { debut, fin }) {
  const c = (col) => (alias ? `${alias}.${col}` : col);
  const conds = [dateRange(c('datmvt'), p, debut, fin)];
  const site = parseText(q.site, 'site', { max: 20 });
  if (site) conds.push(`${c('site')} = ${p.add(site)}`);
  return conds;
}

// GET /api/mouvements/types - Codes GenreMvt distincts avec total
// Pour GenreMvt=3 (ventes), LibMvt contient les numéros de ticket → libellé générique.
// Une seule passe sur mvtart (libellé représentatif = MIN(libmvt)).
router.get('/types', async (req, res) => {
  const result = await getPool().query(`
      SELECT genremvt, COUNT(*) AS nb_occurrences, MIN(libmvt) AS libmvt
      FROM mvtart
      GROUP BY genremvt
      ORDER BY genremvt
    `);

  res.json({
    types: result.rows.map(r => ({
      genremvt: r.genremvt,
      libmvt: r.genremvt == 3 ? '(numéros de ticket)' : (r.libmvt ?? null),
      nb_occurrences: r.nb_occurrences,
    })),
  });
});

// GET /api/mouvements/articles?dateDebut=&dateFin=&site=&codefou=&genremvt=&page=&limit=
router.get('/articles', async (req, res) => {
  const q = req.query;
  const range = parseDateRange(q, { defDebut: '2024-01-01', defFin: '2099-12-31' });
  const codefou = parseText(q.codefou, 'codefou', { max: 50 });
  const genremvt = parsePositiveInt(q.genremvt, 'genremvt', { def: null, min: 0, max: 9999 });
  const { page, limit, offset } = parsePagination(q, { defLimit: 100, maxLimit: 1000 });

  const p = new SqlParams();
  const conds = communes(p, 'm', q, range);
  // Filtre fournisseur uniquement s'il est fourni (sinon les CODEFOU NULL étaient exclus)
  if (codefou) conds.push(`m.codefou LIKE ${p.add(likeContains(codefou))}`);
  if (genremvt !== null) conds.push(`m.genremvt = ${p.add(genremvt)}`);

  const result = await getPool().query(`
      SELECT
        m.datmvt, m.site,
        a.codein, a.libelle1,
        m.libmvt, m.genremvt, m.qtemvt, m.valmvt,
        m.mntmvtht, m.mntmvtttc, m.margemvt,
        m.qtestock, m.prmp, m.valstock,
        m.codefou AS codefou_mouvement,
        -- Pour les ventes (genremvt=3), le vrai fournisseur = dernière entrée avant cette vente
        CASE WHEN m.genremvt = 3 THEN (
          SELECT TRIM(e.codefou) FROM mvtart e
          WHERE e.artnoid = m.artnoid AND e.site = m.site
            AND e.genremvt = 1 AND e.codefou IS NOT NULL AND TRIM(e.codefou) != ''
            AND e.datmvt <= m.datmvt
          ORDER BY e.datmvt DESC LIMIT 1
        ) ELSE TRIM(m.codefou) END AS codefou_reel,
        CASE WHEN m.genremvt = 3 THEN (
          SELECT fi.nom FROM mvtart e
          LEFT JOIN fouident fi ON TRIM(fi.code) = TRIM(e.codefou)
          WHERE e.artnoid = m.artnoid AND e.site = m.site
            AND e.genremvt = 1 AND e.codefou IS NOT NULL AND TRIM(e.codefou) != ''
            AND e.datmvt <= m.datmvt
          ORDER BY e.datmvt DESC LIMIT 1
        ) ELSE (
          SELECT fi2.nom FROM fouident fi2
          WHERE TRIM(fi2.code) = TRIM(m.codefou)
          LIMIT 1
        ) END AS nom_fournisseur_reel
      FROM mvtart m
      JOIN articles a ON a.no_id = m.artnoid
      ${where(conds)}
      ORDER BY m.datmvt DESC, m.no_id DESC
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  res.json({ page, limit, mouvements: result.rows });
});

// GET /api/mouvements/entrees?dateDebut=&dateFin=&site=&artNoId=&page=&limit=
// Entrées en stock uniquement (GenreMvt = 1) avec date de création article
router.get('/entrees', async (req, res) => {
  const q = req.query;
  const range = parseDateRange(q, { defDebut: '2024-01-01', defFin: '2099-12-31' });
  const { page, limit, offset } = parsePagination(q, { defLimit: 100, maxLimit: 1000 });

  const p = new SqlParams();
  const conds = ['m.genremvt = 1', ...communes(p, 'm', q, range)];
  if (q.artNoId !== undefined && q.artNoId !== '') {
    conds.push(`m.artnoid = ${p.add(parseId(q.artNoId, 'artNoId'))}`);
  }

  const result = await getPool().query(`
      SELECT
        m.datmvt AS date_entree,
        m.site,
        a.no_id AS artnoid,
        a.codein,
        a.libelle1,
        a.suividatecreation AS date_creation_article,
        m.libmvt AS libelle_mouvement,
        m.qtemvt AS qte_entree,
        m.prmp AS prmp,
        m.qtestock AS stock_apres_entree,
        m.codefou AS codefou
      FROM mvtart m
      JOIN articles a ON a.no_id = m.artnoid
      ${where(conds)}
      ORDER BY m.datmvt DESC, m.no_id DESC
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  res.json({ page, limit, entrees: result.rows });
});

// GET /api/mouvements/regularisations?dateDebut=&dateFin=&site=&artNoId=&page=&limit=
// Mouvements hors ventes (GenreMvt=3) et hors entrées standard (GenreMvt=1)
// = inventaires, corrections, démarques, transferts, etc.
router.get('/regularisations', async (req, res) => {
  const q = req.query;
  const range = parseDateRange(q, { defDebut: '2024-01-01', defFin: '2099-12-31' });
  const { page, limit, offset } = parsePagination(q, { defLimit: 100, maxLimit: 1000 });

  const p = new SqlParams();
  const conds = ['m.genremvt NOT IN (1, 3)', ...communes(p, 'm', q, range)];
  if (q.artNoId !== undefined && q.artNoId !== '') {
    conds.push(`m.artnoid = ${p.add(parseId(q.artNoId, 'artNoId'))}`);
  }

  const result = await getPool().query(`
      SELECT
        m.datmvt AS date_mouvement,
        m.site,
        a.no_id AS artnoid,
        a.codein,
        a.libelle1,
        m.genremvt,
        m.libmvt AS libelle_mouvement,
        m.qtemvt AS qte,
        m.valmvt AS valeur,
        m.qtestock AS stock_apres,
        m.prmp AS prmp
      FROM mvtart m
      JOIN articles a ON a.no_id = m.artnoid
      ${where(conds)}
      ORDER BY m.datmvt DESC, m.no_id DESC
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  res.json({ page, limit, regularisations: result.rows });
});

// GET /api/mouvements/reglements?dateDebut=&dateFin=&page=&limit=
router.get('/reglements', async (req, res) => {
  const range = parseDateRange(req.query, { defDebut: '2024-01-01', defFin: '2099-12-31' });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });

  const p = new SqlParams();
  const conds = [dateRange('datmvt', p, range.debut, range.fin)];

  const result = await getPool().query(`
      SELECT
        datmvt, codtick, codcartecli, coddev,
        mntreg, mntregdev, clientnom, echeance,
        reference, typereg, suividatecreation
      FROM mvtreg
      ${where(conds)}
      ORDER BY datmvt DESC
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  res.json({ page, limit, reglements: result.rows });
});

// GET /api/mouvements/synthese?dateDebut=&dateFin=&site=&page=&limit= - Synthèse par jour/site
// Paginé (limit 1000 par défaut, max 5000).
router.get('/synthese', async (req, res) => {
  const q = req.query;
  const range = parseDateRange(q, { defDebut: '2024-01-01', defFin: '2099-12-31' });
  const { page, limit, offset } = parsePagination(q, { defLimit: 1000, maxLimit: 5000 });

  const p = new SqlParams();
  const conds = communes(p, null, q, range);

  const result = await getPool().query(`
      SELECT
        -- Jour en texte YYYY-MM-DD (évite le décalage de fuseau du type DATE côté Node)
        TO_CHAR(datmvt, 'YYYY-MM-DD') AS jour,
        site,
        genremvt,
        COUNT(*) AS nb_mvt,
        SUM(qtemvt) AS qte_totale,
        SUM(mntmvtht) AS mnt_ht,
        SUM(mntmvtttc) AS mnt_ttc,
        SUM(margemvt) AS marge
      FROM mvtart
      ${where(conds)}
      GROUP BY TO_CHAR(datmvt, 'YYYY-MM-DD'), site, genremvt
      ORDER BY jour DESC, site, genremvt
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  res.json({ page, limit, synthese: result.rows });
});

module.exports = router;
