const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { parseText, parseCode, parsePagination, parseDateRange, likeContains } = require('../utils/params');
const { SqlParams, dateRange, where } = require('../utils/sql');
const { HttpError } = require('../utils/errors');
const { francoPortCte, FRANCO_EXPR } = require('../sql/franco');

// GET /api/fournisseurs?search=&page=&limit= - Liste des fournisseurs distincts
// Pagination ajoutée (limit 500 par défaut, max 5000) ; `count` = lignes de la page, `total` = total.
router.get('/', async (req, res) => {
  const search = parseText(req.query.search, 'search', { max: 100 });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 500, maxLimit: 5000 });

  const p = new SqlParams();
  const conds = [];
  if (search) {
    const s = p.add(likeContains(search));
    conds.push(`(TRIM(f.code) ILIKE ${s} OR fi.nom ILIKE ${s})`);
  }

  const result = await getPool().query(`
      SELECT
        TRIM(f.code) AS codefou,
        MAX(fi.nom) AS nom,
        MAX(fa.adrligne1) AS adresse,
        MAX(fa.telephone) AS telephone,
        MAX(fa.email) AS email,
        COUNT(DISTINCT f.art_no_id) AS nb_articles,
        MIN(f.suividatecreation) AS premiere_entree,
        MAX(COALESCE(f.suividatemodif, f.suividatecreation)) AS derniere_modif,
        SUM(CASE WHEN f.suspendu IS NULL THEN 1 ELSE 0 END) AS nb_actifs,
        SUM(CASE WHEN f.suspendu IS NOT NULL THEN 1 ELSE 0 END) AS nb_suspendus,
        COUNT(*) OVER () AS total_count
      FROM artfou1 f
      LEFT JOIN fouident fi ON TRIM(fi.code) = TRIM(f.code)
      LEFT JOIN fouadr1 fa ON TRIM(fa.code) = TRIM(f.code) AND fa.sit_code = '000'
      ${where(conds)}
      GROUP BY TRIM(f.code)
      ORDER BY nb_articles DESC, codefou
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  const total = result.rows.length ? Number(result.rows[0].total_count) : 0;
  const fournisseurs = result.rows.map(({ total_count, ...r }) => r);
  res.json({ count: fournisseurs.length, total, page, limit, fournisseurs });
});

// GET /api/fournisseurs/:code/articles?page=&limit=
router.get('/:code/articles', async (req, res) => {
  const code = parseCode(req.params.code, 'code');
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 50, maxLimit: 500 });

  const result = await getPool().query(`
      SELECT
        a.no_id, a.codein, a.libelle1, a.libelle2,
        f.reference AS ref_fou, f.ean13, f.itf,
        f.pcb, f.spcb, f.delai, f.securite, f.suspendu, f.preference,
        t2.prixachat, t2.remise_promotion,
        pa.pa,
        pv.pv AS pv_central
      FROM artfou1 f
      JOIN articles a ON a.no_id = f.art_no_id
      LEFT JOIN artfou2 t2 ON t2.idartfou1 = f.no_id
      LEFT JOIN cube_pa pa ON pa.artnoid = a.no_id
      LEFT JOIN cube_pv pv ON pv.artnoid = a.no_id AND pv.site = '000'
      WHERE TRIM(f.code) = $1
      ORDER BY a.libelle1, a.no_id
      LIMIT $2 OFFSET $3
    `, [code, limit, offset]);

  res.json({ code, page, limit, articles: result.rows });
});

// GET /api/fournisseurs/:code/commandes?dateDebut=&dateFin=&page=&limit=
router.get('/:code/commandes', async (req, res) => {
  const code = parseCode(req.params.code, 'code');
  const { debut, fin } = parseDateRange(req.query, { defDebut: '2020-01-01', defFin: '2099-12-31' });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });

  const p = new SqlParams([code]);
  const conds = ['TRIM(c.fouident_code) = $1', dateRange('c.cdedate', p, debut, fin)];

  const result = await getPool().query(`
      SELECT
        c.no_id         AS no_commande,
        c.cdenum        AS num_commande,
        c.cdeetat       AS etat,
        a.codein        AS codein,
        a.libelle1      AS libelle,
        f.reference     AS ref_fou,
        f.ean13         AS ean13,
        cl.qtecde       AS qte_cde,
        cl.prixbrut     AS prix_brut,
        cl.remise       AS remise,
        cl.prixnet      AS prix_net,
        cl.montant      AS montant,
        cl.qteacc       AS qte_acceptee,
        cl.qteann       AS qte_annulee,
        cl.qteatt       AS qte_attente,
        cl.cdeligtard   AS date_livraison,
        c.cdedate       AS date_commande
      FROM cdefou_ligne cl
      JOIN commande_fou c  ON c.no_id = cl.commande_fou_no_id
      LEFT JOIN artfou1 f  ON f.no_id = cl.artfou1_no_id
      LEFT JOIN articles a ON a.no_id = f.art_no_id
      ${where(conds)}
      ORDER BY c.cdedate DESC, c.no_id, cl.no_id
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  res.json({ code, page, limit, commandes: result.rows });
});

// GET /api/fournisseurs/:code/franco
// Franco de port du fournisseur (fouport2.seuil en priorité, sinon foucad.franco)
router.get('/:code/franco', async (req, res) => {
  const code = parseCode(req.params.code, 'code');

  const result = await getPool().query(`
      WITH ${francoPortCte('$1')}
      SELECT
        fi.code,
        fi.nom,
        ${FRANCO_EXPR}  AS franco_ht,
        fp.seuil        AS franco_port,
        fc.franco       AS franco_general,
        fc.actif,
        fc.duree,
        fc.unite
      FROM fouident fi
      LEFT JOIN foucad      fc ON TRIM(fc.foucode) = TRIM(fi.code)
      LEFT JOIN franco_port fp ON fp.code          = TRIM(fi.code)
      WHERE TRIM(fi.code) = $1
    `, [code]);

  if (!result.rows.length) throw new HttpError(404, 'Fournisseur introuvable');

  res.json(result.rows[0]);
});

module.exports = router;
