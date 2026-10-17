const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { parseText, parseId, parsePositiveInt, likeContains } = require('../utils/params');
const { SqlParams, where } = require('../utils/sql');

// GET /api/ranking?gencod=&codein=&site=&foucentrale=&limit=
router.get('/', async (req, res) => {
  const q = req.query;
  const gencod      = parseText(q.gencod, 'gencod', { max: 20 });
  const codein      = parseText(q.codein, 'codein', { max: 50 });
  const site        = parseText(q.site, 'site', { max: 20 });
  const foucentrale = parseText(q.foucentrale, 'foucentrale', { max: 50 });
  const limit = parsePositiveInt(q.limit, 'limit', { def: 50, min: 1, max: 500, clamp: true });

  const p = new SqlParams();
  const conds = [];
  if (gencod) conds.push(`r.gencod = ${p.add(gencod)}`);
  if (codein) conds.push(`a.codein = ${p.add(codein)}`);
  // site et foucentrale : filtres partiels (documentés)
  if (site) conds.push(`r.site LIKE ${p.add(likeContains(site))}`);
  if (foucentrale) conds.push(`r.foucentrale LIKE ${p.add(likeContains(foucentrale))}`);

  const result = await getPool().query(`
      SELECT
        r.gencod, r.site, r.libelle,
        r.foucentrale, r.nomfoucentrale,
        r.ranking_ca, r.ranking_qte,
        r.ranking_mag_ca, r.ranking_mag_qte, r.ranking_mag_marge,
        r.pv_calcule, r.pv_mag, r.pv_cen,
        r.codefamille, r.libellefamille,
        r.date_maj, r.date_calcul_mag,
        a.no_id AS art_no_id, a.codein
      FROM ranking r
      -- Un GTIN peut être rattaché à plusieurs articles : un seul retenu (pas de doublon)
      LEFT JOIN LATERAL (
        SELECT g.idarticle FROM art_gtin g
        WHERE g.gtin = r.gencod
        ORDER BY g.preferentiel DESC NULLS LAST, g.idarticle
        LIMIT 1
      ) g ON true
      LEFT JOIN articles a ON a.no_id = g.idarticle
      ${where(conds)}
      ORDER BY r.ranking_ca ASC NULLS LAST, r.gencod, r.site
      LIMIT ${p.add(limit)}
    `, p.values);

  res.json({ count: result.rows.length, ranking: result.rows });
});

// GET /api/ranking/article/:id - Ranking pour un article (par no_id)
router.get('/article/:id', async (req, res) => {
  const artNoId = parseId(req.params.id);

  const result = await getPool().query(`
      SELECT
        r.gencod, r.site, r.libelle,
        r.foucentrale, r.nomfoucentrale,
        r.ranking_ca, r.ranking_qte,
        r.ranking_mag_ca, r.ranking_mag_qte, r.ranking_mag_marge,
        r.pv_calcule, r.pv_mag, r.pv_cen,
        r.codefamille, r.libellefamille,
        r.date_maj, r.date_calcul_mag
      FROM ranking r
      JOIN art_gtin g ON g.gtin = r.gencod
      WHERE g.idarticle = $1
      ORDER BY r.site
    `, [artNoId]);

  res.json({ artNoId, ranking: result.rows });
});

module.exports = router;
