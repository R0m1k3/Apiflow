// Approvisionnement : suivi des commandes fournisseurs (containers, RDV dépôt),
// litiges de réception, conditions fournisseurs, rotation / surstock, fiche réassort article.
const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { parseId, parseCode, parseText, parseEnum, parsePagination } = require('../utils/params');
const { SqlParams, dateRange, where } = require('../utils/sql');
const { periode, site: parseSite } = require('./_periode');

// GET /api/appro/suivi-commandes?etat=&codefou=&site=&enCours=true&page&limit
// Dernier état connu de chaque commande (COMMANDE_FOU_SUIVI_ETAT) : container, incoterm, RDV dépôt
router.get('/suivi-commandes', async (req, res) => {
  const codefou = parseText(req.query.codefou, 'codefou', { max: 50 });
  const etat = parseText(req.query.etat, 'etat', { max: 50 });
  const site = parseSite(req.query);
  const enCours = parseEnum(req.query.enCours, 'enCours', ['true', 'false'], { def: 'true' }) === 'true';
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });
  const p = new SqlParams();
  const conds = [];
  if (codefou) conds.push(`TRIM(c.fouident_code) = ${p.add(codefou)}`);
  if (site) conds.push(`c.sit_cod_desti = ${p.add(site)}`);
  if (etat) conds.push(`(ec.etat = ${p.add(etat)} OR ec.libelle ILIKE $${p.values.length})`);
  if (enCours) conds.push(`c.cdedate >= CURRENT_DATE - 365 AND COALESCE(c.cdeannulation, '0') IN ('0', 'false', 'N')`);
  const { rows } = await getPool().query(`
    SELECT c.no_id AS no_commande, c.cdenum, TRIM(c.fouident_code) AS codefou, fi.nom AS fournisseur,
           c.sit_cod_desti AS site, c.cdedate, c.cdetard, c.cdetot, c.cdeetat,
           se.dateetat, ec.etat, ec.libelle AS etat_libelle, ec.couleur,
           se.numero_container, se.code_incoterm, se.ville_incoterm, se.date_depart_incoterm,
           se.port_arrivee_incoterm, se.numero_dossier_incoterm, se.date_rdv_depot, se.heure_rdv_depot,
           se.reception_auto, se.date_reception_auto
    FROM commande_fou c
    LEFT JOIN LATERAL (
      SELECT * FROM commande_fou_suivi_etat s WHERE s.commande_fou_no_id = c.no_id
      ORDER BY s.dateetat DESC NULLS LAST, s.no_id DESC LIMIT 1
    ) se ON true
    LEFT JOIN etat_commande ec ON ec.no_id = se.etat_commande_no_id
    LEFT JOIN fouident fi ON TRIM(fi.code) = TRIM(c.fouident_code)
    ${where(conds)}
    ORDER BY COALESCE(se.date_rdv_depot, c.cdetard) ASC NULLS LAST, c.no_id
    LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}`, p.values);
  res.json({ page, limit, commandes: rows });
});

// GET /api/appro/litiges?dateDebut&dateFin&codefou&groupBy=fournisseur|motif|detail
// Quantités refusées à réception (RECEPLIG_REFUS) et taux de litige par fournisseur
router.get('/litiges', async (req, res) => {
  const groupBy = parseEnum(req.query.groupBy, 'groupBy', ['fournisseur', 'motif', 'detail'], { def: 'fournisseur' });
  const codefou = parseText(req.query.codefou, 'codefou', { max: 50 });
  const { debut, fin } = periode(req.query, { defJours: 90 });
  const p = new SqlParams();
  const conds = [dateRange('COALESCE(r.recdate, rl.suividatecreation)', p, debut, fin)];
  if (codefou) conds.push(`TRIM(r.fouident_code) = ${p.add(codefou)}`);

  if (groupBy === 'detail') {
    const { rows } = await getPool().query(`
      SELECT r.recnum, r.recdate, TRIM(r.fouident_code) AS codefou, a.codein, a.libelle1,
             rl.qtebl, rl.qterec, rr.qterefus, mr.libelle AS motif, rl.motifrefus
      FROM receplig_refus rr
      JOIN cdefou_receplig rl ON rl.no_id = rr.cdefou_receplig_no_id
      LEFT JOIN cdefou_reception r ON r.no_id = rl.cdefou_reception_no_id
      LEFT JOIN artfou1 f ON f.no_id = rl.artfou1_no_id
      LEFT JOIN articles a ON a.no_id = f.art_no_id
      LEFT JOIN motifs_refus mr ON mr.no_id = rr.motif_refus_no_id
      ${where(conds)}
      ORDER BY r.recdate DESC NULLS LAST LIMIT 2000`, p.values);
    return res.json({ periode: { debut, fin }, groupBy, lignes: rows });
  }

  const key = groupBy === 'motif'
    ? { sel: `COALESCE(mr.libelle, 'Sans motif') AS motif`, grp: `COALESCE(mr.libelle, 'Sans motif')` }
    : { sel: `TRIM(r.fouident_code) AS codefou, MAX(fi.nom) AS fournisseur`, grp: `TRIM(r.fouident_code)` };
  const { rows } = await getPool().query(`
    SELECT ${key.sel},
           COUNT(*)                                AS lignes_recues,
           COUNT(*) FILTER (WHERE rr.qte > 0)      AS lignes_en_litige,
           ROUND(SUM(rl.qterec), 0)                AS qte_recue,
           ROUND(SUM(rr.qte), 0)                   AS qte_refusee,
           ROUND(COUNT(*) FILTER (WHERE rr.qte > 0)::numeric / NULLIF(COUNT(*), 0) * 100, 2) AS taux_litige_pct
    FROM cdefou_receplig rl
    LEFT JOIN cdefou_reception r ON r.no_id = rl.cdefou_reception_no_id
    LEFT JOIN LATERAL (
      SELECT SUM(x.qterefus) AS qte, MIN(x.motif_refus_no_id) AS motif
      FROM receplig_refus x WHERE x.cdefou_receplig_no_id = rl.no_id
    ) rr ON true
    LEFT JOIN motifs_refus mr ON mr.no_id = rr.motif
    LEFT JOIN fouident fi ON TRIM(fi.code) = TRIM(r.fouident_code)
    ${where(conds)}
    GROUP BY ${key.grp}
    ${groupBy === 'motif' ? 'HAVING SUM(rr.qte) > 0' : ''}
    ORDER BY qte_refusee DESC NULLS LAST
    LIMIT 500`, p.values);
  res.json({ periode: { debut, fin }, groupBy, lignes: rows });
});

// GET /api/appro/fournisseurs/:code/conditions — règlement, escompte, délais, franco
router.get('/fournisseurs/:code/conditions', async (req, res) => {
  const code = parseCode(req.params.code, 'code');
  const [reg, delais, franco, reel] = await Promise.all([
    getPool().query(`SELECT * FROM foureg WHERE TRIM(code) = $1 ORDER BY actif DESC NULLS LAST, plagedeb DESC NULLS LAST`, [code]),
    getPool().query(`SELECT sit_code, delainormal, delaisecurite, gesreliquat FROM fouregle WHERE TRIM(fou_code) = $1 ORDER BY sit_code`, [code]),
    getPool().query(`SELECT franco, actif, duree, unite, montantcdeauto, datecdeauto FROM foucad WHERE TRIM(foucode) = $1`, [code]),
    // Délai réel constaté : commande → première réception, sur 12 mois
    getPool().query(`
      SELECT COUNT(*) AS nb_commandes,
             ROUND(AVG(x.delai), 1) AS delai_moyen_jours,
             percentile_cont(0.5) WITHIN GROUP (ORDER BY x.delai) AS delai_median_jours
      FROM (
        SELECT c.no_id, MIN(r.recdate)::date - c.cdedate::date AS delai
        FROM commande_fou c
        JOIN cdefou_reception r ON r.com_no_id = c.no_id
        WHERE TRIM(c.fouident_code) = $1 AND c.cdedate >= CURRENT_DATE - 365 AND r.recdate IS NOT NULL
        GROUP BY c.no_id, c.cdedate
      ) x WHERE x.delai >= 0`, [code]),
  ]);
  res.json({ code, reglement: reg.rows, delais: delais.rows, franco: franco.rows[0] || null, delai_reel_12_mois: reel.rows[0] });
});

// GET /api/appro/rotation?site=292&filtre=surstock|sans_vente|tout&page&limit
// STOCKAGE_ROTATION : stock vs ventes, ratio, dernières entrée/vente
router.get('/rotation', async (req, res) => {
  const site = parseSite(req.query) || '292';
  const filtre = parseEnum(req.query.filtre, 'filtre', ['surstock', 'sans_vente', 'tout'], { def: 'surstock' });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });
  const p = new SqlParams([site]);
  const conds = ['r.codesite = $1', 'r.stock > 0'];
  if (filtre === 'sans_vente') conds.push('COALESCE(r.vente, 0) = 0');
  const order = filtre === 'surstock' ? 'r.ratio DESC NULLS LAST' : 'r.stock * COALESCE(r.prmp, 0) DESC';
  const { rows } = await getPool().query(`
    SELECT r.artnoid, r.codein, r.libticket, r.fourncode, r.fournnom, r.nomenlib, r.gammelib, r.saisonlib,
           r.stock, r.vente, r.ratio, r.prmp, r.prixvente,
           ROUND(r.stock * COALESCE(r.prmp, 0), 2) AS valeur_stock,
           r.derentree, r.dervente, r.derinventaire
    FROM stockage_rotation r
    ${where(conds)}
    ORDER BY ${order}
    LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}`, p.values);
  res.json({ site, filtre, page, limit, articles: rows });
});

// GET /api/appro/article/:id — paramètres de réassort et historique d'assortiment
router.get('/article/:id', async (req, res) => {
  const id = parseId(req.params.id);
  const q = (sql) => getPool().query(sql, [id]).then((r) => r.rows);
  const [artsite, notes, gammes, lots, stat, helios, semaines, web] = await Promise.all([
    q(`SELECT site, qtemini, qtemaxi, saisonnier, modereappro FROM artsite WHERE artnoid = $1 ORDER BY site`),
    q(`SELECT site, colisage, datederentree, qtederentree, totqte, nbjour, note FROM note_articles WHERE artnoid = $1`),
    q(`SELECT h.date, h.oldidgamme, go.libelle AS ancienne_gamme, h.newidgamme, gn.libelle AS nouvelle_gamme, h.util
       FROM histo_art_gamme h LEFT JOIN gammes go ON go.no_id = h.oldidgamme LEFT JOIN gammes gn ON gn.no_id = h.newidgamme
       WHERE h.artnoid = $1 ORDER BY h.date DESC LIMIT 50`),
    q(`SELECT l.art_lot_noid, a.codein, a.libelle1, l.quantite, l.pourc_marge
       FROM article_lot l LEFT JOIN articles a ON a.no_id = l.art_lot_noid WHERE l.artnoid = $1`),
    q(`SELECT s.site, s.dispoperm, s.stock, s.ventesn_4, s.ventesn1_4, s.encours, s.proposition, s.gamme, s.saison, s.dateactualisation
       FROM statpermtemp s JOIN articles a ON a.codein = s.codein WHERE a.no_id = $1`),
    q(`SELECT site, ranking, saisonnier FROM ranking_helios WHERE artnoid = $1`),
    q(`SELECT annee, numsem, type, codesite, valeurstk, valeurvte FROM commande_auto_detail_semaine
       WHERE artnoid = $1 ORDER BY annee DESC, numsem DESC LIMIT 104`),
    q(`SELECT w.idmag, w.qtestock, w.qteencde, w.pvmag, w.interditvte, w.datemaj
       FROM art_web_stock w JOIN articles a ON a.codein = w.idarticle OR a.no_id::text = w.idarticle WHERE a.no_id = $1`),
  ]);
  res.json({ article: id, reassort: artsite, notes, historique_gammes: gammes, lots,
    stat_permanent: stat, ranking_helios: helios, historique_semaines: semaines, stock_web: web });
});

module.exports = router;
