const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

// GET /api/commandes-auto?site=&codefou=
// Propositions de commandes automatiques par site et fournisseur, avec comparaison au franco
router.get('/', async (req, res) => {
  try {
    const pool = getPool();
    const { site = '', codefou = '' } = req.query;

    const result = await pool.query(`
      WITH franco_port AS (
        SELECT fp.code, MAX(fp2.seuil) AS seuil
        FROM fouport fp
        JOIN fouport2 fp2 ON fp2.fou_no_id = fp.no_id
        GROUP BY fp.code
      )
      SELECT
        q.codesite                                               AS site,
        q.fou_code                                               AS codefou,
        fi.nom                                                   AS nom_fou,
        COALESCE(fp.seuil, fc.franco)                            AS franco_ht,
        COUNT(q.art_no_id)                                       AS nb_articles,
        SUM(q.qtepropo)                                          AS qte_totale,
        ROUND(SUM(q.qtepropo * cp.pa), 2)                       AS montant_propo_ht,
        CASE
          WHEN COALESCE(fp.seuil, fc.franco) IS NULL OR COALESCE(fp.seuil, fc.franco) = 0 THEN 'N/A'
          WHEN SUM(q.qtepropo * cp.pa) >= COALESCE(fp.seuil, fc.franco)                    THEN 'OUI'
          ELSE 'NON'
        END                                                      AS franco_atteint,
        CASE
          WHEN COALESCE(fp.seuil, fc.franco) IS NULL OR COALESCE(fp.seuil, fc.franco) = 0 THEN NULL
          ELSE ROUND(COALESCE(fp.seuil, fc.franco) - SUM(q.qtepropo * cp.pa), 2)
        END                                                      AS ecart_franco
      FROM commande_auto_qtepropo q
      LEFT JOIN fouident   fi ON fi.code    = q.fou_code
      LEFT JOIN foucad     fc ON fc.foucode = q.fou_code
      LEFT JOIN franco_port fp ON fp.code   = q.fou_code
      LEFT JOIN cube_pa    cp ON cp.artnoid = q.art_no_id
      WHERE q.qtepropo > 0
        AND ($1 = '' OR q.codesite LIKE $1)
        AND ($2 = '' OR q.fou_code ILIKE $2)
      GROUP BY q.codesite, q.fou_code, fi.nom, fc.franco, fp.seuil
      ORDER BY q.codesite, montant_propo_ht DESC
    `, [`%${site}%`, `%${codefou}%`]);

    res.json({ count: result.rows.length, propositions: result.rows });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// GET /api/commandes-auto/:codefou?site=
// Détail des articles proposés pour un fournisseur donné
router.get('/:codefou', async (req, res) => {
  try {
    const pool = getPool();
    const { site = '' } = req.query;
    const { codefou } = req.params;

    // Résumé par site
    const resume = await pool.query(`
      WITH franco_port AS (
        SELECT fp.code, MAX(fp2.seuil) AS seuil
        FROM fouport fp
        JOIN fouport2 fp2 ON fp2.fou_no_id = fp.no_id
        GROUP BY fp.code
      )
      SELECT
        q.codesite                                               AS site,
        q.fou_code                                               AS codefou,
        fi.nom                                                   AS nom_fou,
        COALESCE(fp.seuil, fc.franco)                            AS franco_ht,
        COUNT(q.art_no_id)                                       AS nb_articles,
        SUM(q.qtepropo)                                          AS qte_totale,
        ROUND(SUM(q.qtepropo * cp.pa), 2)                       AS montant_propo_ht,
        CASE
          WHEN COALESCE(fp.seuil, fc.franco) IS NULL OR COALESCE(fp.seuil, fc.franco) = 0 THEN 'N/A'
          WHEN SUM(q.qtepropo * cp.pa) >= COALESCE(fp.seuil, fc.franco)                    THEN 'OUI'
          ELSE 'NON'
        END                                                      AS franco_atteint,
        CASE
          WHEN COALESCE(fp.seuil, fc.franco) IS NULL OR COALESCE(fp.seuil, fc.franco) = 0 THEN NULL
          ELSE ROUND(COALESCE(fp.seuil, fc.franco) - SUM(q.qtepropo * cp.pa), 2)
        END                                                      AS ecart_franco
      FROM commande_auto_qtepropo q
      LEFT JOIN fouident   fi ON fi.code    = q.fou_code
      LEFT JOIN foucad     fc ON fc.foucode = q.fou_code
      LEFT JOIN franco_port fp ON fp.code   = q.fou_code
      LEFT JOIN cube_pa    cp ON cp.artnoid = q.art_no_id
      WHERE q.qtepropo > 0
        AND q.fou_code ILIKE $1
        AND ($2 = '' OR q.codesite LIKE $2)
      GROUP BY q.codesite, q.fou_code, fi.nom, fc.franco, fp.seuil
      ORDER BY q.codesite
    `, [codefou, `%${site}%`]);

    if (resume.rows.length === 0) {
      return res.status(404).json({ error: 'Aucune proposition pour ce fournisseur' });
    }

    // Détail des articles
    const lignes = await pool.query(`
      SELECT
        q.codesite                                          AS site,
        q.art_no_id                                         AS no_id,
        a.codein,
        a.libelle1,
        af.reference                                        AS ref_fou,
        af.pcb,
        q.qtepropo,
        cp.pa,
        ROUND(q.qtepropo * cp.pa, 2)                       AS montant_ht,
        cs.qte                                              AS stock_actuel,
        cs.dernierevente
      FROM commande_auto_qtepropo q
      LEFT JOIN articles  a   ON a.no_id    = q.art_no_id
      LEFT JOIN artfou1   af  ON af.fou_code = q.fou_code AND af.art_no_id = q.art_no_id AND af.preference = 1
      LEFT JOIN cube_pa   cp  ON cp.artnoid  = q.art_no_id
      LEFT JOIN cube_stock cs ON cs.artnoid  = q.art_no_id AND cs.site = q.codesite
      WHERE q.qtepropo > 0
        AND q.fou_code ILIKE $1
        AND ($2 = '' OR q.codesite LIKE $2)
      ORDER BY q.codesite, montant_ht DESC
    `, [codefou, `%${site}%`]);

    res.json({
      resume: resume.rows,
      lignes: lignes.rows,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
