const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { parseText, parseId, parsePagination, parseDateRange, likeContains } = require('../utils/params');
const { SqlParams, dateRange, where } = require('../utils/sql');
const { HttpError } = require('../utils/errors');

// GET /api/commandes?dateDebut=&dateFin=&codefou=&page=&limit=
router.get('/', async (req, res) => {
  const { debut, fin } = parseDateRange(req.query, { defDebut: '2020-01-01', defFin: '2099-12-31' });
  const codefou = parseText(req.query.codefou, 'codefou', { max: 50 });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });

  const p = new SqlParams();
  const conds = [dateRange('c.cdedate', p, debut, fin)];
  if (codefou) conds.push(`TRIM(c.fouident_code) ILIKE ${p.add(likeContains(codefou))}`);
  const etat = parseText(req.query.etat, 'etat', { max: 10 });
  if (etat) conds.push(`c.cdeetat = ${p.add(etat)}`);

  const result = await getPool().query(`
      SELECT
        c.no_id                      AS no_commande,
        c.cdenum                     AS num_commande,
        TRIM(c.fouident_code)        AS codefou,
        c.cdeetat                    AS etat,
        a.codein                     AS codein,
        a.libelle1                   AS libelle,
        f.reference                  AS ref_fou,
        f.ean13                      AS ean13,
        f.itf                        AS itf,
        cl.qtecde                    AS qte_cde,
        cl.prixbrut                  AS prix_brut,
        cl.remise                    AS remise,
        cl.prixnet                   AS prix_net,
        cl.montant                   AS montant,
        pv.pv                        AS prix_vente,
        cl.qteacc                    AS qte_acceptee,
        cl.qteann                    AS qte_annulee,
        cl.qteatt                    AS qte_attente,
        cl.qterel                    AS qte_reliquat,
        cl.cdeligtard                AS date_livraison_cible,
        cl.cdeligtot                 AS date_livraison_tot,
        NULL::text                   AS commentaire,
        c.cdedate                    AS date_commande
      -- CDEFOU_VIVANT n'est plus alimentée côté SQL Server (oct. 2026) :
      -- lignes de commande = cdefou_ligne + en-tête commande_fou
      FROM cdefou_ligne cl
      JOIN commande_fou c   ON c.no_id = cl.commande_fou_no_id
      LEFT JOIN artfou1 f   ON f.no_id = cl.artfou1_no_id
      LEFT JOIN articles a  ON a.no_id = f.art_no_id
      LEFT JOIN cube_pv pv  ON pv.artnoid = f.art_no_id AND pv.site = c.sit_cod_desti
      ${where(conds)}
      ORDER BY c.cdedate DESC, c.no_id, cl.no_id
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  res.json({ page, limit, commandes: result.rows });
});

// GET /api/commandes/:noCommande - Détail d'une commande
router.get('/:noCommande', async (req, res) => {
  const noCommande = parseId(req.params.noCommande, 'noCommande');

  const result = await getPool().query(`
      SELECT
        c.no_id                      AS no_commande,
        c.cdenum                     AS num_commande,
        TRIM(c.fouident_code)        AS codefou,
        c.cdeetat                    AS etat,
        a.codein                     AS codein,
        a.libelle1                   AS libelle,
        a.libelle2                   AS libelle2,
        f.reference                  AS ref_fou,
        f.ean13                      AS ean13,
        f.itf                        AS itf,
        cl.qtecde                    AS qte_cde,
        cl.prixbrut                  AS prix_brut,
        cl.remise                    AS remise,
        NULL::numeric                AS remise2,
        NULL::numeric                AS remise3,
        cl.prixnet                   AS prix_net,
        cl.montant                   AS montant,
        pv.pv                        AS prix_vente,
        cl.gratuit                   AS qte_gratuite,
        cl.qteacc                    AS qte_acceptee,
        cl.qteann                    AS qte_annulee,
        cl.qteatt                    AS qte_attente,
        cl.qterel                    AS qte_reliquat,
        cl.cdeligtard                AS date_livraison_cible,
        cl.cdeligtot                 AS date_livraison_tot,
        NULL::text                   AS commentaire,
        c.cdetot                     AS total_commande,
        c.cdeportmontant             AS montant_port,
        c.cdedate                    AS date_commande
      -- CDEFOU_VIVANT n'est plus alimentée côté SQL Server (oct. 2026) :
      -- lignes de commande = cdefou_ligne + en-tête commande_fou
      FROM cdefou_ligne cl
      JOIN commande_fou c   ON c.no_id = cl.commande_fou_no_id
      LEFT JOIN artfou1 f   ON f.no_id = cl.artfou1_no_id
      LEFT JOIN articles a  ON a.no_id = f.art_no_id
      LEFT JOIN cube_pv pv  ON pv.artnoid = f.art_no_id AND pv.site = c.sit_cod_desti
      WHERE c.no_id = $1
      ORDER BY a.libelle1
    `, [noCommande]);

  if (!result.rows.length) throw new HttpError(404, 'Commande introuvable');

  // NUMERIC est renvoyé en chaîne par pg : conversion avant la somme
  const total = result.rows.reduce((s, l) => s + (Number(l.montant) || 0), 0);

  res.json({
    noCommande,
    nbLignes: result.rows.length,
    montantTotal: Math.round(total * 10000) / 10000,
    lignes: result.rows,
  });
});

// GET /api/commandes/receptions/liste?dateDebut=&dateFin=&page=&limit=
router.get('/receptions/liste', async (req, res) => {
  const { debut, fin } = parseDateRange(req.query, { defDebut: '2020-01-01', defFin: '2099-12-31' });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });

  const p = new SqlParams();
  const conds = [dateRange('rl.suividatecreation', p, debut, fin)];

  const result = await getPool().query(`
      SELECT
        rl.no_id, rl.cdefou_reception_no_id,
        rl.qtebl, rl.qterec, rl.qteacc, rl.qteref, rl.qteaff,
        rl.pribrut, rl.remise, rl.prirec, rl.mntrec,
        rl.motifrefus, rl.recpb, rl.recpbok,
        f.code AS codefou,
        f.reference AS ref_fou, f.ean13,
        a.codein, a.libelle1,
        rl.suividatecreation AS date_reception
      FROM cdefou_receplig rl
      JOIN artfou1 f ON f.no_id = rl.artfou1_no_id
      JOIN articles a ON a.no_id = f.art_no_id
      ${where(conds)}
      ORDER BY rl.suividatecreation DESC, rl.no_id
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  res.json({ page, limit, receptions: result.rows });
});

module.exports = router;
