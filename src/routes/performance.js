const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const {
  parseText, parseEnum, parseDate, parsePositiveInt, checkRange, likeContains,
} = require('../utils/params');
const { SqlParams, dateRange, sameDay, where } = require('../utils/sql');
const { todayParis, addDays } = require('../utils/dates');
const { caSql, tauxMarge } = require('../utils/ca');
const { fouPrincipalLateral } = require('../sql/fournisseur');

// Période max autorisée sur les agrégats de performance (≈ 3 ans)
const MAX_DAYS = 3 * 366;
// Période par défaut : 30 derniers jours (aujourd'hui inclus, heure de Paris)
const DEFAULT_DAYS = 30;

// Lit dateDebut/dateFin avec les défauts « 30 derniers jours » et le plafond de 3 ans
function periode(q) {
  let fin   = parseDate(q.dateFin, 'dateFin', { def: null });
  let debut = parseDate(q.dateDebut, 'dateDebut', { def: null });
  if (!fin) fin = todayParis();
  if (!debut) debut = addDays(fin, -(DEFAULT_DAYS - 1));
  checkRange(debut, fin, { maxDays: MAX_DAYS });
  return { debut, fin };
}

// Conditions communes : ventes + période + site (code site exact)
function baseConditions(p, ca, alias, debut, fin, site) {
  const conds = [ca.filtreVentes, dateRange(`${alias}.datmvt`, p, debut, fin)];
  if (site) conds.push(`${alias}.site = ${p.add(site)}`);
  return conds;
}

// GET /api/performance/ca?dateDebut=&dateFin=&site=&groupBy=jour|mois
router.get('/ca', async (req, res) => {
  const { debut, fin } = periode(req.query);
  const site = parseText(req.query.site, 'site', { max: 20 });
  const groupBy = parseEnum(req.query.groupBy, 'groupBy', ['jour', 'mois'], { def: 'jour' });

  const ca = caSql('m');
  const p = new SqlParams();
  const conds = baseConditions(p, ca, 'm', debut, fin, site);
  // Période renvoyée en texte (YYYY-MM ou YYYY-MM-DD) : pas de décalage de fuseau à la sérialisation
  const groupExpr = groupBy === 'mois' ? "TO_CHAR(m.datmvt, 'YYYY-MM')" : "TO_CHAR(m.datmvt, 'YYYY-MM-DD')";

  const result = await getPool().query(`
      SELECT
        ${groupExpr} AS periode,
        m.site,
        COUNT(*) AS nb_ventes,
        ${ca.qteVendue}  AS qte_vendue,
        ${ca.caHt}       AS ca_ht,
        ${ca.caTtc}      AS ca_ttc,
        ${ca.retoursTtc} AS retours_ttc,
        ${ca.marge}      AS marge,
        ${ca.tauxMarge}  AS taux_marge
      FROM mvtart m
      ${where(conds)}
      GROUP BY ${groupExpr}, m.site
      ORDER BY periode DESC, m.site
    `, p.values);

  res.json({
    groupBy,
    dateDebut: debut,
    dateFin: fin,
    site: site || 'tous',
    ca: result.rows,
  });
});

// GET /api/performance/hitparade?dateDebut=&dateFin=&site=&limit=&groupBy=qte|ca|marge&codefou=
// Agrégation par article (no_id) et site, puis fournisseur principal (1 seul par article).
router.get('/hitparade', async (req, res) => {
  const { debut, fin } = periode(req.query);
  const site    = parseText(req.query.site, 'site', { max: 20 });
  const codefou = parseText(req.query.codefou, 'codefou', { max: 50 });
  const groupBy = parseEnum(req.query.groupBy, 'groupBy', ['ca', 'qte', 'marge'], { def: 'ca' });
  const limit   = parsePositiveInt(req.query.limit, 'limit', { def: 50, min: 1, max: 500, clamp: true });

  const orderCol = { qte: 'qte_vendue', marge: 'marge', ca: 'ca_ht' }[groupBy];
  const ca = caSql('m');
  const p = new SqlParams();
  const conds = baseConditions(p, ca, 'm', debut, fin, site);

  // Sans filtre fournisseur, on peut tronquer au top N avant les jointures
  const limitPh = p.add(limit);
  const preLimit = codefou ? '' : `ORDER BY ${orderCol} DESC NULLS LAST LIMIT ${limitPh}`;
  const fouFilter = codefou ? `WHERE fp.code ILIKE ${p.add(likeContains(codefou))}` : '';

  const result = await getPool().query(`
      WITH ventes AS (
        SELECT
          m.artnoid, m.site,
          COUNT(*)         AS nb_passages,
          ${ca.qteVendue}  AS qte_vendue,
          ${ca.caHt}       AS ca_ht,
          ${ca.caTtc}      AS ca_ttc,
          ${ca.marge}      AS marge,
          ${ca.tauxMarge}  AS taux_marge,
          MAX(m.datmvt)    AS derniere_vente
        FROM mvtart m
        ${where(conds)}
        GROUP BY m.artnoid, m.site
        ${preLimit}
      )
      SELECT
        a.no_id,
        a.codein,
        a.libelle1,
        v.site,
        fp.code                     AS code_fournisseur,
        COALESCE(fi.nom, fp.code)   AS fournisseur,
        v.nb_passages, v.qte_vendue, v.ca_ht, v.ca_ttc, v.marge, v.taux_marge,
        v.derniere_vente
      FROM ventes v
      JOIN articles a ON a.no_id = v.artnoid
      ${fouPrincipalLateral('v.artnoid', 'fp')}
      LEFT JOIN fouident fi ON TRIM(fi.code) = fp.code
      ${fouFilter}
      ORDER BY v.${orderCol} DESC NULLS LAST, a.no_id
      LIMIT ${limitPh}
    `, p.values);

  res.json({
    dateDebut: debut,
    dateFin: fin,
    site: site || 'tous',
    codefou: codefou || null,
    classementPar: groupBy,
    hitparade: result.rows,
  });
});

// GET /api/performance/ca/nomenclature?dateDebut=&dateFin=&site=&niveau=
// Les articles sont rattachés au niveau 3. Pour niveau=1 ou 2, on remonte via
// chemin_pere (format "noid_niv1.noid_niv2").
router.get('/ca/nomenclature', async (req, res) => {
  const { debut, fin } = periode(req.query);
  const site = parseText(req.query.site, 'site', { max: 20 });
  const niv = parsePositiveInt(req.query.niveau, 'niveau', { def: 1, min: 1, max: 3 });

  const joinNom = niv === 3
    ? 'JOIN nomenclature n ON n.no_id = a.nom_no_id'
    : `JOIN nomenclature n3 ON n3.no_id = a.nom_no_id
       JOIN nomenclature n  ON n.no_id = NULLIF(SPLIT_PART(n3.chemin_pere, '.', ${niv}), '')::bigint`;

  const ca = caSql('m');
  const p = new SqlParams();
  const conds = baseConditions(p, ca, 'm', debut, fin, site);

  const result = await getPool().query(`
      SELECT
        n.code                    AS code_nomen,
        n.libelle,
        ${niv}                    AS niveau,
        COUNT(DISTINCT m.artnoid) AS nb_articles,
        ${ca.qteVendue}           AS qte_vendue,
        ${ca.caHt}                AS ca_ht,
        ${ca.caTtc}               AS ca_ttc,
        ${ca.marge}               AS marge,
        ${ca.tauxMarge}           AS taux_marge
      FROM mvtart m
      JOIN articles a ON a.no_id = m.artnoid
      ${joinNom}
      ${where(conds)}
      GROUP BY n.code, n.libelle
      ORDER BY ca_ttc DESC
    `, p.values);

  res.json({ dateDebut: debut, dateFin: fin, niveau: niv, ca_par_nomenclature: result.rows });
});

// GET /api/performance/ca/fournisseur?dateDebut=&dateFin=&site=
// Agrégation par article puis rattachement au fournisseur principal (pas de double comptage).
router.get('/ca/fournisseur', async (req, res) => {
  const { debut, fin } = periode(req.query);
  const site = parseText(req.query.site, 'site', { max: 20 });

  const ca = caSql('m');
  const p = new SqlParams();
  const conds = baseConditions(p, ca, 'm', debut, fin, site);

  const result = await getPool().query(`
      WITH ventes AS (
        SELECT
          m.artnoid,
          ${ca.qteVendue} AS qte_vendue,
          ${ca.caHt}      AS ca_ht,
          ${ca.caTtc}     AS ca_ttc,
          ${ca.marge}     AS marge,
          ${ca.mntHtNet}  AS mnt_ht_net
        FROM mvtart m
        ${where(conds)}
        GROUP BY m.artnoid
      )
      SELECT
        COALESCE(fi.nom, fp.code, 'Inconnu') AS fournisseur,
        fp.code                              AS code_fournisseur,
        COUNT(*)                             AS nb_articles,
        SUM(v.qte_vendue)                    AS qte_vendue,
        SUM(v.ca_ht)                         AS ca_ht,
        SUM(v.ca_ttc)                        AS ca_ttc,
        SUM(v.marge)                         AS marge,
        ${tauxMarge('SUM(v.marge)', 'SUM(v.mnt_ht_net)')} AS taux_marge
      FROM ventes v
      ${fouPrincipalLateral('v.artnoid', 'fp')}
      LEFT JOIN fouident fi ON TRIM(fi.code) = fp.code
      GROUP BY fi.nom, fp.code
      ORDER BY ca_ttc DESC
    `, p.values);

  res.json({ dateDebut: debut, dateFin: fin, site: site || 'tous', ca_par_fournisseur: result.rows });
});

// GET /api/performance/ca/gamme?dateDebut=&dateFin=&site=
// Agrégation par article d'abord : un article présent dans plusieurs saisons
// d'une même gamme n'est compté qu'une fois dans cette gamme.
router.get('/ca/gamme', async (req, res) => {
  const { debut, fin } = periode(req.query);
  const site = parseText(req.query.site, 'site', { max: 20 });

  const ca = caSql('m');
  const p = new SqlParams();
  const conds = baseConditions(p, ca, 'm', debut, fin, site);

  const result = await getPool().query(`
      WITH ventes AS (
        SELECT
          m.artnoid,
          ${ca.qteVendue} AS qte_vendue,
          ${ca.caHt}      AS ca_ht,
          ${ca.caTtc}     AS ca_ttc,
          ${ca.marge}     AS marge
        FROM mvtart m
        ${where(conds)}
        GROUP BY m.artnoid
      ),
      art_gamme AS (
        SELECT DISTINCT artnoid, idgamme FROM art_gamme_saison
      )
      SELECT
        g.code            AS gamme,
        g.libelle         AS libelle_gamme,
        COUNT(*)          AS nb_articles,
        SUM(v.qte_vendue) AS qte_vendue,
        SUM(v.ca_ht)      AS ca_ht,
        SUM(v.ca_ttc)     AS ca_ttc,
        SUM(v.marge)      AS marge
      FROM ventes v
      JOIN art_gamme ag ON ag.artnoid = v.artnoid
      JOIN gammes g ON g.no_id = ag.idgamme
      GROUP BY g.code, g.libelle
      ORDER BY ca_ttc DESC
    `, p.values);

  res.json({ dateDebut: debut, dateFin: fin, ca_par_gamme: result.rows });
});

// GET /api/performance/dashboard?date=2026-03-20&site=
// CA + trafic du jour vs N-1 (même jour de la semaine, J-364) + top 10 CA / qté / marge
router.get('/dashboard', async (req, res) => {
  // Défaut : hier, heure de Paris
  const targetDate = parseDate(req.query.date, 'date', { def: addDays(todayParis(), -1) });
  const site = parseText(req.query.site, 'site', { max: 20 });
  // 364 jours = 52 semaines : même jour de la semaine, pas de problème de 29/02
  const n1DateStr = addDays(targetDate, -364);
  const pool = getPool();

  // CA + trafic N et N-1 (statopca, colonne DATE)
  const pCa = new SqlParams([targetDate, n1DateStr]);
  const caSiteFilter = site ? `AND site = ${pCa.add(site)}` : '';

  // Top 10 : une seule agrégation par article/site, trois classements
  const ca = caSql('m');
  const pTop = new SqlParams();
  const topConds = [ca.filtreVentes, sameDay('m.datmvt', pTop, targetDate)];
  if (site) topConds.push(`m.site = ${pTop.add(site)}`);

  const [caRows, tops] = await Promise.all([
    pool.query(`
      SELECT site, TO_CHAR(datmvt, 'YYYY-MM-DD') AS date, mnt AS ca_ttc, nbticket AS trafic
      FROM statopca
      WHERE datmvt IN ($1::date, $2::date) ${caSiteFilter}
      ORDER BY site, datmvt
    `, pCa.values),
    pool.query(`
      WITH v AS (
        SELECT m.artnoid, m.site,
               ${ca.caTtc}     AS ca_ttc,
               ${ca.qteVendue} AS qte_vendue,
               ${ca.marge}     AS marge
        FROM mvtart m
        ${where(topConds)}
        GROUP BY m.artnoid, m.site
      ),
      r AS (
        SELECT v.*,
               RANK() OVER (PARTITION BY site ORDER BY ca_ttc DESC)            AS rk_ca,
               RANK() OVER (PARTITION BY site ORDER BY qte_vendue DESC)        AS rk_qte,
               RANK() OVER (PARTITION BY site ORDER BY marge DESC NULLS LAST)  AS rk_marge
        FROM v
      )
      SELECT a.codein, a.libelle1, r.site, r.ca_ttc, r.qte_vendue, r.marge,
             r.rk_ca, r.rk_qte, r.rk_marge
      FROM r
      JOIN articles a ON a.no_id = r.artnoid
      WHERE r.rk_ca <= 10 OR r.rk_qte <= 10 OR r.rk_marge <= 10
    `, pTop.values),
  ]);

  const top = (rkKey) => tops.rows
    .filter(r => Number(r[rkKey]) <= 10)
    .sort((x, y) => (x.site < y.site ? -1 : x.site > y.site ? 1 : Number(x[rkKey]) - Number(y[rkKey])))
    .map(r => ({
      codein: r.codein, libelle1: r.libelle1, site: r.site,
      ca_ttc: r.ca_ttc, qte_vendue: r.qte_vendue, marge: r.marge,
    }));

  // Comparaison N vs N-1 par site
  const byDate = {};
  for (const row of caRows.rows) {
    if (!byDate[row.site]) byDate[row.site] = {};
    byDate[row.site][row.date] = { ca_ttc: parseFloat(row.ca_ttc) || 0, trafic: row.trafic || 0 };
  }

  const evol = (n, n1) => {
    if (!(n1 > 0)) return null;
    const v = Math.round((n / n1 - 1) * 1000) / 10;
    return `${v > 0 ? '+' : ''}${v}%`;
  };

  const sites = Object.keys(byDate).sort().map(s => {
    const n  = byDate[s][targetDate] || { ca_ttc: 0, trafic: 0 };
    const n1 = byDate[s][n1DateStr]  || { ca_ttc: 0, trafic: 0 };
    return {
      site:        s,
      ca_ttc:      n.ca_ttc,   ca_ttc_n1: n1.ca_ttc,
      evol_ca:     evol(n.ca_ttc, n1.ca_ttc),
      trafic:      n.trafic,   trafic_n1: n1.trafic,
      evol_trafic: evol(n.trafic, n1.trafic),
    };
  });

  res.json({
    date:        targetDate,
    date_n1:     n1DateStr,
    sites,
    top10_ca:    top('rk_ca'),
    top10_qte:   top('rk_qte'),
    top10_marge: top('rk_marge'),
  });
});

module.exports = router;
