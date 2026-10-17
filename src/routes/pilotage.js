// Pilotage magasin : objectifs (BUDGET_PILOTAGE), météo, démarque, fidélité.
const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { parsePositiveInt, parseEnum, parseText } = require('../utils/params');
const { SqlParams, dateRange, where } = require('../utils/sql');
const { periode, site: parseSite, VENTES } = require('./_periode');
const { todayParis } = require('../utils/dates');

// GET /api/pilotage/objectifs?annee=2026&site=292&niveau=1|2&nomenclature=<no_id N1>
// Budgets / objectifs calculés par Helios (BUDGET_PILOTAGE) : annuel + 12 mois d'exercice.
router.get('/objectifs', async (req, res) => {
  const annee = parsePositiveInt(req.query.annee, 'annee', { def: Number(todayParis().slice(0, 4)), min: 2000, max: 2100 });
  const site = parseSite(req.query);
  const niveau = parseEnum(req.query.niveau, 'niveau', ['1', '2'], { def: '1' });
  const n1 = parseText(req.query.nomenclature, 'nomenclature', { max: 20 });

  const p = new SqlParams();
  const conds = [`extract(year FROM b.datefiscale) = ${p.add(annee)}`];
  if (site) conds.push(`b.site = ${p.add(site)}`);
  conds.push(niveau === '1' ? 'b.nomennoid_n2 = 0' : 'b.nomennoid_n2 <> 0');
  if (n1) conds.push(`b.nomennoid_n1 = ${p.add(n1)}::bigint`);

  const [annuel, mois] = await Promise.all([
    getPool().query(`
      SELECT b.site, b.datefiscale, b.nomennoid_n1, n1.libelle AS libelle_n1,
             b.nomennoid_n2, n2.libelle AS libelle_n2, b.datecalcul,
             b.ca_ht_an2, b.ca_ht_an1, b.ca_ht_objectif, b.ca_ht_an, b.ca_coeff,
             b.stk_an1, b.stk_objectif, b.stk_an,
             b.marge_valeur_an1, b.marge_taux_an1, b.marge_taux, b.marge_valeur_an, b.marge_taux_an,
             b.demarque_connue_an1, b.demarque_inconnue_an1, b.demarque_connue, b.demarque_taux, b.demarque_taux_connue,
             b.lineaire_unite,
             CASE WHEN b.ca_ht_objectif > 0 THEN ROUND(b.ca_ht_an / b.ca_ht_objectif * 100, 1) END AS realisation_ca_pct
      FROM budget_pilotage b
      LEFT JOIN nomenclature n1 ON n1.no_id = b.nomennoid_n1
      LEFT JOIN nomenclature n2 ON n2.no_id = NULLIF(b.nomennoid_n2, 0)
      ${where(conds)}
      ORDER BY b.site, b.ca_ht_objectif DESC NULLS LAST`, p.values),
    getPool().query(`
      SELECT b.site, b.mois,
             ROUND(SUM(b.ca_ht), 2) AS ca_ht, ROUND(SUM(b.ca_ht_exercice), 2) AS ca_ht_exercice,
             ROUND(SUM(b.stk), 2) AS stock, ROUND(SUM(b.marge_exercice), 2) AS marge_exercice,
             ROUND(SUM(b.marge_n1), 2) AS marge_n1
      FROM budget_pilotage_mois b
      ${where(conds)}
      GROUP BY b.site, b.mois ORDER BY b.site, b.mois`, p.values),
  ]);
  res.json({
    annee, site: site || null, niveau: Number(niveau),
    note: 'Valeurs calculées par Helios (BUDGET_PILOTAGE). mois = mois de l\'exercice fiscal (1 = premier mois).',
    lignes: annuel.rows, mensuel: mois.rows,
  });
});

// GET /api/pilotage/meteo?dateDebut&dateFin&site — météo du jour × CA et trafic
router.get('/meteo', async (req, res) => {
  const { debut, fin } = periode(req.query, { defJours: 90 });
  const site = parseSite(req.query) || '292';
  const p = new SqlParams([site]);
  const { rows } = await getPool().query(`
    SELECT d.jour::date AS date, extract(isodow FROM d.jour)::int AS jour_semaine,
           mt.libelle AS temps, me.temperature, me.commentaire,
           s.mnt AS ca_ttc, s.nbticket AS nb_tickets,
           ROUND(s.mnt / NULLIF(s.nbticket, 0), 2) AS panier_moyen
    FROM generate_series(${p.add(debut)}::date, ${p.add(fin)}::date, interval '1 day') d(jour)
    LEFT JOIN meteo me      ON me.site = $1 AND me.date = d.jour::date
    LEFT JOIN meteo_temps mt ON mt.no_id = me.temps_no_id
    LEFT JOIN statopca s    ON s.site = $1 AND s.datmvt = d.jour::date
    ORDER BY 1`, p.values);
  // Synthèse par type de temps (jours ouvrés avec CA)
  const parTemps = {};
  for (const r of rows) {
    if (!r.temps || r.ca_ttc == null) continue;
    const t = (parTemps[r.temps] ||= { temps: r.temps, nb_jours: 0, ca_total: 0, tickets_total: 0 });
    t.nb_jours += 1; t.ca_total += Number(r.ca_ttc); t.tickets_total += Number(r.nb_tickets || 0);
  }
  res.json({
    site, periode: { debut, fin },
    synthese: Object.values(parTemps).map((t) => ({
      temps: t.temps, nb_jours: t.nb_jours,
      ca_moyen_jour: Math.round(t.ca_total / t.nb_jours * 100) / 100,
      tickets_moyen_jour: Math.round(t.tickets_total / t.nb_jours),
    })),
    jours: rows,
  });
});

// GET /api/pilotage/meteo/previsions?site
router.get('/meteo/previsions', async (req, res) => {
  const site = parseSite(req.query);
  const p = new SqlParams();
  const conds = ['date_meteo >= CURRENT_DATE'];
  if (site) conds.push(`site = ${p.add(site)}`);
  const { rows } = await getPool().query(`
    SELECT site, date_meteo, weather_code, temperature_min, temperature_max, temperature_moy,
           precipitation_mm, neige_cm, rafale_max_kmh, date_import
    FROM meteo_prevision ${where(conds)} ORDER BY site, date_meteo`, p.values);
  res.json({ previsions: rows });
});

// GET /api/pilotage/demarque?dateDebut&dateFin&site&groupBy=type|nomenclature|article
// Démarque connue (vol, casse, périmé, inventaire…) à partir des types de mouvement
router.get('/demarque', async (req, res) => {
  const groupBy = parseEnum(req.query.groupBy, 'groupBy', ['type', 'nomenclature', 'article'], { def: 'type' });
  const limit = parsePositiveInt(req.query.limit, 'limit', { def: 100, max: 1000, clamp: true });
  const { debut, fin } = periode(req.query);
  const site = parseSite(req.query);
  const p = new SqlParams();
  // groupes 4 = démarque, 5 = surmarque, 7 = inventaire (table mvtgrp)
  const conds = ['t.mvtgrpnoid IN (4, 5, 7)', dateRange('m.datmvt', p, debut, fin)];
  if (site) conds.push(`m.site = ${p.add(site)}`);
  const g = {
    type:         { sel: 't.no_id AS type_id, t.libellemvt AS type, g.libellegrpmvt AS groupe', grp: 't.no_id, t.libellemvt, g.libellegrpmvt' },
    nomenclature: { sel: 'm.nomnoid, n.libelle AS nomenclature', grp: 'm.nomnoid, n.libelle' },
    article:      { sel: 'm.artnoid, a.codein, a.libelle1', grp: 'm.artnoid, a.codein, a.libelle1' },
  }[groupBy];
  const { rows } = await getPool().query(`
    SELECT ${g.sel},
           COUNT(*) AS nb_mouvements,
           ROUND(SUM(m.qtemvt), 2)                 AS qte,
           ROUND(SUM(m.qtemvt * m.prmp), 2)        AS valeur_prmp,
           ROUND(SUM(m.mntmvtttc), 2)              AS valeur_ttc
    FROM mvtart m
    JOIN mvttype t ON t.no_id = m.mvttypnoid
    LEFT JOIN mvtgrp g ON g.no_id = t.mvtgrpnoid
    ${groupBy === 'nomenclature' ? 'LEFT JOIN nomenclature n ON n.no_id = m.nomnoid' : ''}
    ${groupBy === 'article' ? 'LEFT JOIN articles a ON a.no_id = m.artnoid' : ''}
    ${where(conds)}
    GROUP BY ${g.grp}
    ORDER BY valeur_prmp ASC NULLS LAST
    LIMIT ${p.add(limit)}`, p.values);
  res.json({
    periode: { debut, fin }, site: site || null, groupBy,
    note: 'Quantités/valeurs signées : négatif = sortie de stock (démarque), positif = surmarque.',
    lignes: rows,
  });
});

// GET /api/pilotage/fidelite?dateDebut&dateFin&site — part de l'activité réalisée avec carte
router.get('/fidelite', async (req, res) => {
  const { debut, fin } = periode(req.query);
  const site = parseSite(req.query) || '292';
  const p = new SqlParams([site]);
  const { rows } = await getPool().query(`
    SELECT d.jour::date AS date,
           f.nb_tickets AS tickets_fidelite, f.nb_cartes, ROUND(f.ca_achat, 2) AS ca_fidelite, f.points,
           s.nbticket AS tickets_total, ROUND(s.mnt, 2) AS ca_total,
           ROUND(f.nb_tickets::numeric / NULLIF(s.nbticket, 0) * 100, 1) AS part_tickets_pct,
           ROUND(f.ca_achat / NULLIF(s.mnt, 0) * 100, 1) AS part_ca_pct
    FROM generate_series(${p.add(debut)}::date, ${p.add(fin)}::date, interval '1 day') d(jour)
    LEFT JOIN fidelite_jour f ON f.site = $1 AND f.date = d.jour::date
    LEFT JOIN statopca s      ON s.site = $1 AND s.datmvt = d.jour::date
    ORDER BY 1`, p.values);
  const sum = (k) => rows.reduce((s, r) => s + Number(r[k] || 0), 0);
  res.json({
    site, periode: { debut, fin },
    total: {
      tickets_fidelite: sum('tickets_fidelite'), tickets_total: sum('tickets_total'),
      ca_fidelite: Math.round(sum('ca_fidelite') * 100) / 100, ca_total: Math.round(sum('ca_total') * 100) / 100,
      part_ca_pct: sum('ca_total') ? Math.round(sum('ca_fidelite') / sum('ca_total') * 1000) / 10 : null,
    },
    jours: rows,
  });
});

// GET /api/pilotage/ca-jour?dateDebut&dateFin&site — CA/tickets journaliers recalculés depuis les tickets
router.get('/ca-jour', async (req, res) => {
  const { debut, fin } = periode(req.query);
  const site = parseSite(req.query);
  const p = new SqlParams();
  const conds = [VENTES('m'), dateRange('m.datmvt', p, debut, fin)];
  if (site) conds.push(`m.site = ${p.add(site)}`);
  const { rows } = await getPool().query(`
    SELECT m.datmvt::date AS date, m.site,
           ROUND(SUM(-m.mntmvtttc), 2) AS ca_ttc, ROUND(SUM(-m.mntmvtht), 2) AS ca_ht,
           ROUND(SUM(m.margemvt), 2) AS marge, COUNT(DISTINCT m.libmvt) AS nb_tickets
    FROM mvtart m ${where(conds)}
    GROUP BY 1, 2 ORDER BY 1, 2`, p.values);
  res.json({ periode: { debut, fin }, site: site || null, jours: rows });
});

module.exports = router;
