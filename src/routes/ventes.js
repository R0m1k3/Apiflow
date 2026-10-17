// Analyses de ventes au ticket (données ajoutées oct. 2026 : heure du ticket,
// type de mouvement, règlements, motifs de remise caisse).
const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { parseId, parsePositiveInt, parseEnum } = require('../utils/params');
const { SqlParams, dateRange, where } = require('../utils/sql');
const { VENTES, periode, site: parseSite } = require('./_periode');

function base(q, alias = 'm', col = 'datmvt') {
  const { debut, fin } = periode(q);
  const site = parseSite(q);
  const p = new SqlParams();
  const conds = [VENTES(alias), dateRange(`${alias}.${col}`, p, debut, fin)];
  if (site) conds.push(`${alias}.site = ${p.add(site)}`);
  return { p, conds, debut, fin, site };
}

// GET /api/ventes/horaire?dateDebut&dateFin&site — CA, tickets et panier par heure
router.get('/horaire', async (req, res) => {
  const { p, conds, debut, fin, site } = base(req.query);
  conds.push('m.ticket_ts IS NOT NULL');
  const { rows } = await getPool().query(`
    SELECT extract(hour FROM m.ticket_ts)::int AS heure,
           ROUND(SUM(-m.mntmvtttc), 2)        AS ca_ttc,
           ROUND(SUM(-m.mntmvtht), 2)         AS ca_ht,
           ROUND(SUM(m.margemvt), 2)          AS marge,
           COUNT(DISTINCT m.libmvt)           AS nb_tickets,
           ROUND(SUM(-m.qtemvt), 0)           AS nb_articles,
           ROUND(SUM(-m.mntmvtttc) / NULLIF(COUNT(DISTINCT m.libmvt), 0), 2) AS panier_moyen_ttc
    FROM mvtart m
    ${where(conds)}
    GROUP BY 1 ORDER BY 1`, p.values);
  const total = rows.reduce((s, r) => s + Number(r.ca_ttc || 0), 0);
  res.json({
    periode: { debut, fin }, site: site || null,
    heures: rows.map((r) => ({ ...r, part_ca_pct: total ? Math.round(Number(r.ca_ttc) / total * 1000) / 10 : 0 })),
  });
});

// GET /api/ventes/jours?dateDebut&dateFin&site — par jour de semaine (1 = lundi)
router.get('/jours', async (req, res) => {
  const { p, conds, debut, fin, site } = base(req.query);
  const { rows } = await getPool().query(`
    SELECT extract(isodow FROM m.datmvt)::int AS jour_semaine,
           COUNT(DISTINCT m.datmvt::date)     AS nb_jours,
           ROUND(SUM(-m.mntmvtttc), 2)        AS ca_ttc,
           COUNT(DISTINCT m.libmvt)           AS nb_tickets,
           ROUND(SUM(-m.mntmvtttc) / NULLIF(COUNT(DISTINCT m.datmvt::date), 0), 2) AS ca_moyen_jour
    FROM mvtart m
    ${where(conds)}
    GROUP BY 1 ORDER BY 1`, p.values);
  res.json({ periode: { debut, fin }, site: site || null, jours: rows });
});

// GET /api/ventes/paniers?dateDebut&dateFin&site — indicateurs de panier + répartition
router.get('/paniers', async (req, res) => {
  const { p, conds, debut, fin, site } = base(req.query);
  const { rows } = await getPool().query(`
    WITH t AS (
      SELECT m.libmvt, SUM(-m.mntmvtttc) AS ttc, SUM(-m.qtemvt) AS qte, COUNT(*) AS lignes
      FROM mvtart m
      ${where(conds)}
      GROUP BY m.libmvt
      HAVING SUM(-m.mntmvtttc) > 0
    )
    SELECT COUNT(*)                                   AS nb_tickets,
           ROUND(AVG(ttc), 2)                         AS panier_moyen_ttc,
           ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY ttc)::numeric, 2) AS panier_median_ttc,
           ROUND(AVG(qte), 2)                         AS articles_par_ticket,
           ROUND(AVG(lignes), 2)                      AS lignes_par_ticket,
           COUNT(*) FILTER (WHERE ttc < 10)           AS tranche_moins_10,
           COUNT(*) FILTER (WHERE ttc >= 10 AND ttc < 20)  AS tranche_10_20,
           COUNT(*) FILTER (WHERE ttc >= 20 AND ttc < 50)  AS tranche_20_50,
           COUNT(*) FILTER (WHERE ttc >= 50 AND ttc < 100) AS tranche_50_100,
           COUNT(*) FILTER (WHERE ttc >= 100)         AS tranche_100_plus
    FROM t`, p.values);
  res.json({ periode: { debut, fin }, site: site || null, ...rows[0] });
});

// GET /api/ventes/associations/:id?dateDebut&dateFin&limit — articles achetés avec l'article :id
router.get('/associations/:id', async (req, res) => {
  const artId = parseId(req.params.id);
  const limit = parsePositiveInt(req.query.limit, 'limit', { def: 20, max: 100, clamp: true });
  const { p, conds, debut, fin } = base(req.query, 'm', 'datmvt');
  const pArt = p.add(artId);
  const { rows } = await getPool().query(`
    WITH tickets AS (
      SELECT DISTINCT m.libmvt FROM mvtart m
      ${where([...conds, `m.artnoid = ${pArt}`])}
    ), nb AS (SELECT COUNT(*)::int AS n FROM tickets)
    SELECT o.artnoid, a.codein, a.libelle1,
           COUNT(DISTINCT o.libmvt) AS nb_tickets_communs,
           ROUND(COUNT(DISTINCT o.libmvt)::numeric / NULLIF(MAX(nb.n), 0) * 100, 1) AS pct_tickets,
           MAX(nb.n) AS nb_tickets_article
    FROM mvtart o
    JOIN tickets t ON t.libmvt = o.libmvt
    CROSS JOIN nb
    LEFT JOIN articles a ON a.no_id = o.artnoid
    WHERE ${VENTES('o')} AND o.artnoid <> ${pArt}
      AND ${dateRange('o.datmvt', p, debut, fin)}
    GROUP BY o.artnoid, a.codein, a.libelle1
    ORDER BY nb_tickets_communs DESC
    LIMIT ${p.add(limit)}`, p.values);
  res.json({
    article: artId, periode: { debut, fin },
    nb_tickets_article: rows[0]?.nb_tickets_article ?? 0,
    associations: rows.map(({ nb_tickets_article, ...r }) => r),
  });
});

// GET /api/ventes/reglements?dateDebut&dateFin — encaissements par mode de règlement
router.get('/reglements', async (req, res) => {
  const { debut, fin } = periode(req.query);
  const p = new SqlParams();
  const { rows } = await getPool().query(`
    SELECT mr.codreg, COALESCE(mr.libreg, 'Inconnu') AS mode,
           COUNT(*) AS nb_reglements, COUNT(DISTINCT r.codtick) AS nb_tickets,
           ROUND(SUM(r.mntreg), 2) AS montant
    FROM mvtreg r
    LEFT JOIN mode_reglement mr ON mr.no_id = r.regnoid
    WHERE ${dateRange('r.datmvt', p, debut, fin)}
    GROUP BY mr.codreg, mr.libreg
    ORDER BY montant DESC NULLS LAST`, p.values);
  const total = rows.reduce((s, r) => s + Number(r.montant || 0), 0);
  res.json({
    periode: { debut, fin },
    total: Math.round(total * 100) / 100,
    modes: rows.map((r) => ({ ...r, part_pct: total ? Math.round(Number(r.montant) / total * 1000) / 10 : 0 })),
  });
});

// GET /api/ventes/remises?dateDebut&dateFin&site&groupBy=motif|valeur|article
// Remises caisse : écart prix d'origine × qté – montant encaissé, par motif (PUB, OPLIQ…, TARIF_CLIENT)
router.get('/remises', async (req, res) => {
  const groupBy = parseEnum(req.query.groupBy, 'groupBy', ['motif', 'valeur', 'article'], { def: 'motif' });
  const limit = parsePositiveInt(req.query.limit, 'limit', { def: 100, max: 1000, clamp: true });
  const { p, conds, debut, fin, site } = base(req.query);
  conds.push('mm.prix_orig IS NOT NULL');
  const keys = {
    motif:   { sel: `COALESCE(mm.motif, '(aucun)') AS motif`, grp: `COALESCE(mm.motif, '(aucun)')` },
    valeur:  { sel: `COALESCE(mm.motif, '(aucun)') AS motif, mm.motif_valeur`, grp: `COALESCE(mm.motif, '(aucun)'), mm.motif_valeur` },
    article: { sel: `m.artnoid, a.codein, a.libelle1`, grp: `m.artnoid, a.codein, a.libelle1` },
  }[groupBy];
  const { rows } = await getPool().query(`
    SELECT ${keys.sel},
           COUNT(*)                                            AS nb_lignes,
           ROUND(SUM(-m.qtemvt), 0)                            AS qte,
           ROUND(SUM(mm.prix_orig * -m.qtemvt), 2)             AS montant_prix_origine,
           ROUND(SUM(-m.mntmvtttc), 2)                         AS ca_ttc,
           ROUND(SUM(mm.prix_orig * -m.qtemvt) - SUM(-m.mntmvtttc), 2) AS remise_ttc
    FROM mvtart m
    JOIN mvtart_motif mm ON mm.mvtart_no_id = m.no_id
    ${groupBy === 'article' ? 'LEFT JOIN articles a ON a.no_id = m.artnoid' : ''}
    ${where(conds)}
    GROUP BY ${keys.grp}
    ORDER BY remise_ttc DESC NULLS LAST
    LIMIT ${p.add(limit)}`, p.values);
  res.json({ periode: { debut, fin }, site: site || null, groupBy, lignes: rows });
});

module.exports = router;
