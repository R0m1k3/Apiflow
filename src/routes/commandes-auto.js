const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { parseText, parseCode, likeContains } = require('../utils/params');
const { SqlParams, where } = require('../utils/sql');
const { HttpError } = require('../utils/errors');
const { francoPortCte, FRANCO_EXPR } = require('../sql/franco');

// Résumé par site × fournisseur, comparé au franco de port.
// La condition de fraîcheur de foucad est dans le ON : sinon le LEFT JOIN
// devenait une jointure interne et masquait les fournisseurs sans foucad récent.
function resumeSql(conditions) {
  return `
      WITH ${francoPortCte()}
      SELECT
        q.codesite                                 AS site,
        q.fou_code                                 AS codefou,
        fi.nom                                     AS nom_fou,
        ${FRANCO_EXPR}                             AS franco_ht,
        COUNT(q.art_no_id)                         AS nb_articles,
        SUM(q.qtepropo)                            AS qte_totale,
        ROUND(SUM(q.qtepropo * cp.pa), 2)          AS montant_propo_ht,
        CASE
          WHEN ${FRANCO_EXPR} IS NULL OR ${FRANCO_EXPR} = 0 THEN 'N/A'
          WHEN SUM(q.qtepropo * cp.pa) >= ${FRANCO_EXPR}    THEN 'OUI'
          ELSE 'NON'
        END                                        AS franco_atteint,
        CASE
          WHEN ${FRANCO_EXPR} IS NULL OR ${FRANCO_EXPR} = 0 THEN NULL
          ELSE ROUND(${FRANCO_EXPR} - SUM(q.qtepropo * cp.pa), 2)
        END                                        AS ecart_franco
      FROM commande_auto_qtepropo q
      LEFT JOIN fouident    fi ON TRIM(fi.code)    = TRIM(q.fou_code)
      LEFT JOIN foucad      fc ON TRIM(fc.foucode) = TRIM(q.fou_code)
                              AND fc.datecdeauto >= CURRENT_DATE - INTERVAL '7 days'
      LEFT JOIN franco_port fp ON fp.code          = TRIM(q.fou_code)
      LEFT JOIN cube_pa     cp ON cp.artnoid       = q.art_no_id
      ${where(conditions)}
      GROUP BY q.codesite, q.fou_code, fi.nom, fc.franco, fp.seuil
  `;
}

// GET /api/commandes-auto?site=&codefou=
// Propositions de commandes automatiques par site et fournisseur, avec comparaison au franco
router.get('/', async (req, res) => {
  const site    = parseText(req.query.site, 'site', { max: 20 });
  const codefou = parseText(req.query.codefou, 'codefou', { max: 50 });

  const p = new SqlParams();
  const conds = ['q.qtepropo > 0'];
  if (site)    conds.push(`q.codesite = ${p.add(site)}`);
  if (codefou) conds.push(`TRIM(q.fou_code) ILIKE ${p.add(likeContains(codefou))}`);

  const result = await getPool().query(`
      ${resumeSql(conds)}
      ORDER BY q.codesite, montant_propo_ht DESC NULLS LAST
    `, p.values);

  res.json({ count: result.rows.length, propositions: result.rows });
});

// GET /api/commandes-auto/:codefou?site=
// Détail des articles proposés pour un fournisseur donné
router.get('/:codefou', async (req, res) => {
  const codefou = parseCode(req.params.codefou, 'codefou');
  const site    = parseText(req.query.site, 'site', { max: 20 });
  const pool = getPool();

  // Conditions communes (code fournisseur exact, insensible à la casse et aux espaces)
  const p = new SqlParams([codefou]);
  const conds = ['q.qtepropo > 0', 'UPPER(TRIM(q.fou_code)) = UPPER($1)'];
  if (site) conds.push(`q.codesite = ${p.add(site)}`);

  const [resume, lignes] = await Promise.all([
    pool.query(`${resumeSql(conds)} ORDER BY q.codesite`, p.values),
    pool.query(`
      SELECT
        q.codesite                       AS site,
        q.art_no_id                      AS no_id,
        a.codein,
        a.libelle1,
        af.reference                     AS ref_fou,
        af.pcb,
        q.qtepropo,
        cp.pa,
        ROUND(q.qtepropo * cp.pa, 2)     AS montant_ht,
        cs.qte                           AS stock_actuel,
        cs.dernierevente
      FROM commande_auto_qtepropo q
      LEFT JOIN articles  a   ON a.no_id    = q.art_no_id
      LEFT JOIN LATERAL (
        SELECT f.reference, f.pcb
        FROM artfou1 f
        WHERE f.art_no_id = q.art_no_id AND TRIM(f.code) = TRIM(q.fou_code)
        ORDER BY f.preference NULLS LAST, f.no_id
        LIMIT 1
      ) af ON true
      LEFT JOIN cube_pa   cp  ON cp.artnoid  = q.art_no_id
      LEFT JOIN cube_stock cs ON cs.artnoid  = q.art_no_id AND cs.site = q.codesite
      ${where(conds)}
      ORDER BY q.codesite, montant_ht DESC NULLS LAST
    `, p.values),
  ]);

  if (resume.rows.length === 0) {
    throw new HttpError(404, 'Aucune proposition pour ce fournisseur');
  }

  res.json({ resume: resume.rows, lignes: lignes.rows });
});

module.exports = router;
