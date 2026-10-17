const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const {
  parseText, parseEnum, parseId, parsePagination, parseDateRange, likeContains,
} = require('../utils/params');
const { SqlParams, dateRange, where } = require('../utils/sql');
const { HttpError } = require('../utils/errors');
const { photoCode, photoUrls, resolvePhotoUrl, proxyImage, PHOTO_SIZES } = require('../services/photos');

// Code centrale LaFoir'Fouille conforme : préfixe 1000x sur 11 chiffres.
// COALESCE car les articles non référencés valent '' ou NULL.
const CENTRALE_CONFORME = `(TRIM(COALESCE(a.artcentrale, '')) LIKE '1000%'
          AND LENGTH(TRIM(COALESCE(a.artcentrale, ''))) = 11)`;

// GET /api/articles?search=&codein=&ean=&actif=&codefou=&artcentrale=&has_artcentrale=&page=&limit=
router.get('/', async (req, res) => {
  const q = req.query;
  const search      = parseText(q.search, 'search');
  const codein      = parseText(q.codein, 'codein', { max: 50 });
  const ean         = parseText(q.ean, 'ean', { max: 50 });
  const codefou     = parseText(q.codefou, 'codefou', { max: 50 });
  const artcentrale = parseText(q.artcentrale, 'artcentrale', { max: 50 });
  const actif       = parseEnum(q.actif, 'actif', ['1', '0'], { def: '' });
  const hasCentrale = parseEnum(q.has_artcentrale, 'has_artcentrale', ['1', '0'], { def: '' });
  const { page, limit, offset } = parsePagination(q, { defLimit: 50, maxLimit: 500 });

  const p = new SqlParams();
  const conds = [];
  if (search) {
    const s = p.add(likeContains(search));
    conds.push(`(a.libelle1 ILIKE ${s} OR a.libelle2 ILIKE ${s})`);
  }
  if (codein) conds.push(`a.codein ILIKE ${p.add(likeContains(codein))}`);
  // Recherche sur n'importe quel GTIN de l'article (pas seulement le préférentiel)
  if (ean) {
    conds.push(`EXISTS (SELECT 1 FROM art_gtin ge
          WHERE ge.idarticle = a.no_id AND ge.gtin LIKE ${p.add(likeContains(ean))})`);
  }
  if (actif === '1') conds.push('a.actif IS NOT NULL AND a.suspendu IS NULL');
  if (actif === '0') conds.push('a.suspendu IS NOT NULL');
  if (codefou) {
    conds.push(`EXISTS (SELECT 1 FROM artfou1 f
          WHERE f.art_no_id = a.no_id AND TRIM(f.code) ILIKE ${p.add(likeContains(codefou))})`);
  }
  if (artcentrale) conds.push(`TRIM(a.artcentrale) LIKE ${p.add(likeContains(artcentrale))}`);
  if (hasCentrale === '1') conds.push(CENTRALE_CONFORME);
  if (hasCentrale === '0') conds.push(`NOT ${CENTRALE_CONFORME}`);

  const result = await getPool().query(`
      SELECT
        a.no_id, a.codein, a.libelle1, a.libelle2, a.lib_ticket,
        a.tax_code, a.ach_code, a.utilisable, a.actif, a.suspendu,
        a.suividatecreation, a.suividatemodif,
        NULLIF(TRIM(a.artcentrale), '') AS artcentrale,
        ai.prix_vente_mini, ai.prix_vente_maxi, ai.eco_ttc,
        ai.on_web, ai.interdit_remise, ai.nomphoto,
        ai.datedebvente, ai.datefinvente,
        pa.pa,
        g.gtin,
        fpref.code    AS codefou_principal,
        fpref.nom_fou AS nom_fou_principal,
        fpref.ref     AS ref_fou_principale,
        fpref.pcb     AS pcb_principal
      FROM articles a
      LEFT JOIN article_infosup ai ON ai.artnoid = a.no_id
      LEFT JOIN cube_pa pa ON pa.artnoid = a.no_id
      LEFT JOIN LATERAL (
        SELECT gp.gtin FROM art_gtin gp
        WHERE gp.idarticle = a.no_id AND gp.preferentiel = 1
        ORDER BY gp.gtin
        LIMIT 1
      ) g ON true
      LEFT JOIN LATERAL (
        SELECT f.code, f.reference AS ref, f.qteua AS pcb, fi.nom AS nom_fou
        FROM artfou1 f
        LEFT JOIN fouident fi ON TRIM(fi.code) = TRIM(f.code)
        WHERE f.art_no_id = a.no_id
        ORDER BY f.preference NULLS LAST, f.no_id
        LIMIT 1
      ) fpref ON true
      ${where(conds)}
      ORDER BY a.libelle1, a.no_id
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  const articles = result.rows.map(a => ({ ...a, ...photoUrls(a.no_id, a.nomphoto) }));
  res.json({ page, limit, articles });
});

// GET /api/articles/:id - Détail complet d'un article
router.get('/:id', async (req, res) => {
  const id = parseId(req.params.id);
  const pool = getPool();

  const [article, gtins, stock, prix, fournisseurs] = await Promise.all([
    pool.query(`
      SELECT
        a.*, ai.prix_vente_mini, ai.prix_vente_maxi,
        ai.eco_ht, ai.eco_ttc, ai.on_web, ai.interdit_remise,
        ai.nomphoto, ai.datedebvente, ai.datefinvente,
        ai.datevaliditeachat, ai.commentaire, ai.pv_conseille,
        ai.fraislogistic, ai.transit, ai.distribution,
        pa.pa
      FROM articles a
      LEFT JOIN article_infosup ai ON ai.artnoid = a.no_id
      LEFT JOIN cube_pa pa ON pa.artnoid = a.no_id
      WHERE a.no_id = $1
    `, [id]),
    pool.query(
      'SELECT gtin, preferentiel FROM art_gtin WHERE idarticle = $1 ORDER BY preferentiel DESC',
      [id]
    ),
    pool.query(`
      SELECT site, qte, prmp, valstock, pv, stockdispo,
             dernierevente, dernierereception, stockmort
      FROM cube_stock WHERE artnoid = $1 ORDER BY site
    `, [id]),
    pool.query('SELECT site, pv FROM cube_pv WHERE artnoid = $1 ORDER BY site', [id]),
    pool.query(`
      SELECT f.code AS codefou, fa.raisonsociale AS nom_fou,
             f.reference, f.ean13, f.itf,
             f.qteua, f.pcb, f.spcb, f.delai, f.securite, f.preference, f.suspendu,
             t.prixachat, t.remise_promotion
      FROM artfou1 f
      LEFT JOIN fouadr1 fa ON TRIM(fa.code) = TRIM(f.code) AND fa.sit_code = '000'
      LEFT JOIN artfou2 t ON t.idartfou1 = f.no_id
      WHERE f.art_no_id = $1
      ORDER BY f.preference
    `, [id]),
  ]);

  if (!article.rows.length) throw new HttpError(404, 'Article introuvable');

  const art = article.rows[0];
  res.json({
    article: {
      ...art,
      artcentrale: art.artcentrale?.trim() || null,
      ...photoUrls(id, art.nomphoto),
    },
    gtins: gtins.rows,
    stock: stock.rows,
    prix: prix.rows,
    fournisseurs: fournisseurs.rows,
  });
});

// Notes article (table note_articles alimentée par la sync).
// Tolère l'absence de la table pendant le déploiement (42P01 = undefined_table).
async function fetchNotes(pool, id) {
  try {
    const r = await pool.query(`
      SELECT site, colisage, datederentree, qtederentree, totqte, nbjour, note
      FROM note_articles WHERE artnoid = $1 ORDER BY site
    `, [id]);
    return r.rows;
  } catch (err) {
    if (err.code === '42P01') {
      console.warn('[articles] table note_articles absente — notes vides');
      return [];
    }
    throw err;
  }
}

// GET /api/articles/:id/referentiel - Fiche complète
router.get('/:id/referentiel', async (req, res) => {
  const id = parseId(req.params.id);
  const pool = getPool();

  // Requêtes indépendantes exécutées en parallèle
  const [article, gtins, gammes, stock, prixAchat, prixVente, fournisseurs, mvtAgreges, notes, ranking] =
    await Promise.all([
      pool.query(`
        SELECT
          a.no_id, a.codein, a.libelle1, a.libelle2, a.lib_ticket,
          a.tax_code, a.actif, a.suspendu, a.utilisable,
          a.suividatecreation, a.suividatemodif,
          ai.prix_vente_mini, ai.prix_vente_maxi,
          ai.eco_ht, ai.eco_ttc, ai.on_web,
          ai.interdit_remise, ai.nomphoto,
          ai.datedebvente, ai.datefinvente,
          ai.pv_conseille, ai.fraislogistic,
          ai.commentaire,
          n.code AS nom_code, n.libelle AS nom_libelle, n.niveau AS nom_niveau,
          n.chemin_pere AS nom_chemin_pere
        FROM articles a
        LEFT JOIN article_infosup ai ON ai.artnoid = a.no_id
        LEFT JOIN nomenclature n ON n.no_id = a.nom_no_id
        WHERE a.no_id = $1
      `, [id]),
      pool.query(
        'SELECT gtin, preferentiel FROM art_gtin WHERE idarticle = $1 ORDER BY preferentiel DESC',
        [id]
      ),
      pool.query(`
        SELECT g.code AS gamme_code, g.libelle AS gamme_libelle,
               s.code AS saison_code, s.libelle AS saison_libelle,
               ags.idgamme, ags.idsaison
        FROM art_gamme_saison ags
        JOIN gammes g ON g.no_id = ags.idgamme
        LEFT JOIN saisons s ON s.no_id = ags.idsaison
        WHERE ags.artnoid = $1
      `, [id]),
      pool.query(`
        SELECT site, qte, prmp, valstock, pv, stockdispo,
               dernierevente, dernierereception, stockmort
        FROM cube_stock WHERE artnoid = $1 ORDER BY site
      `, [id]),
      pool.query('SELECT artnoid, pa FROM cube_pa WHERE artnoid = $1', [id]),
      pool.query('SELECT site, pv FROM cube_pv WHERE artnoid = $1 ORDER BY site', [id]),
      pool.query(`
        SELECT f.code AS codefou, fi.nom AS nom_fou,
               f.reference AS ref_fou, f.ean13, f.itf,
               f.qteua AS pcb, f.spcb, f.delai, f.securite,
               f.preference, f.suspendu,
               t.prixachat, t.remise_promotion
        FROM artfou1 f
        LEFT JOIN fouident fi ON TRIM(fi.code) = TRIM(f.code)
        LEFT JOIN artfou2 t ON t.idartfou1 = f.no_id
        WHERE f.art_no_id = $1
        ORDER BY f.preference
      `, [id]),
      pool.query(`
        SELECT
          MAX(CASE WHEN genremvt = 1 THEN datmvt END) AS derniere_entree,
          MAX(CASE WHEN genremvt = 3 THEN datmvt END) AS derniere_vente,
          SUM(CASE WHEN genremvt = 3 THEN qtemvt ELSE 0 END) AS qte_totale_vendue,
          SUM(CASE WHEN genremvt = 3 THEN mntmvtttc ELSE 0 END) AS ca_ttc_total,
          SUM(CASE WHEN genremvt = 3 THEN margemvt ELSE 0 END) AS marge_totale
        FROM mvtart WHERE artnoid = $1
      `, [id]),
      fetchNotes(pool, id),
      pool.query(`
        SELECT r.gencod, r.site, r.ranking_ca, r.ranking_qte,
               r.ranking_mag_ca, r.ranking_mag_qte, r.ranking_mag_marge,
               r.foucentrale, r.nomfoucentrale,
               r.pv_calcule, r.pv_mag, r.pv_cen,
               r.date_maj, r.date_calcul_mag
        FROM ranking r
        JOIN art_gtin g ON g.gtin = r.gencod
        WHERE g.idarticle = $1
        ORDER BY r.site
      `, [id]),
    ]);

  if (!article.rows.length) throw new HttpError(404, 'Article introuvable');

  const artRef = article.rows[0];
  res.json({
    article: { ...artRef, ...photoUrls(id, artRef.nomphoto) },
    gtins: gtins.rows,
    gammes: gammes.rows,
    ranking: ranking.rows,
    stock: stock.rows,
    // Pas de données de remballe dans PostgreSQL : champ conservé pour compatibilité
    stock_remballe: null,
    prix: {
      achat: prixAchat.rows[0]?.pa ?? null,
      vente_par_site: prixVente.rows,
    },
    fournisseurs: fournisseurs.rows,
    performance: mvtAgreges.rows[0],
    notes,
  });
});

// GET /api/articles/:id/photo?size=large — proxy photo depuis lafoirfouille.fr (route publique)
router.get('/:id/photo', async (req, res) => {
  const id = parseId(req.params.id);
  const size = req.query.size === 'large' ? PHOTO_SIZES.large : PHOTO_SIZES.small;
  const pool = getPool();

  const result = await pool.query(`
      SELECT ai.nomphoto FROM articles a
      LEFT JOIN article_infosup ai ON ai.artnoid = a.no_id
      WHERE a.no_id = $1
    `, [id]);

  if (!result.rows.length) throw new HttpError(404, 'Article introuvable');

  const code = photoCode(result.rows[0].nomphoto);
  if (!code) throw new HttpError(404, 'Pas de photo pour cet article');
  if (!/^\d{9,13}$/.test(code)) throw new HttpError(404, 'Format photo non supporté');

  const imageUrl = await resolvePhotoUrl(pool, code, size);
  if (!imageUrl) throw new HttpError(404, 'Photo non trouvée sur lafoirfouille.fr');

  // Proxy de l'image (évite les problèmes CORS côté client)
  await proxyImage(res, imageUrl);
});

// GET /api/articles/:id/dernier-fournisseur?site=
// Fournisseur réel (dernière entrée en stock GenreMvt=1) par site.
// C'est le seul moyen fiable d'identifier qui a livré le produit actuellement en rayon.
router.get('/:id/dernier-fournisseur', async (req, res) => {
  const id = parseId(req.params.id);
  const site = parseText(req.query.site, 'site', { max: 20 });

  const p = new SqlParams([id]);
  const result = await getPool().query(`
      SELECT DISTINCT ON (m.site)
        m.site,
        m.datmvt       AS date_derniere_entree,
        m.codefou      AS codefou,
        fi.nom         AS nom_fournisseur,
        m.qtemvt       AS qte_entree,
        m.prmp         AS prmp
      FROM mvtart m
      LEFT JOIN fouident fi ON TRIM(fi.code) = TRIM(m.codefou)
      WHERE m.artnoid = $1
        AND m.genremvt = 1
        AND m.codefou IS NOT NULL
        AND TRIM(m.codefou) != ''
        ${site ? `AND m.site LIKE ${p.add(likeContains(site))}` : ''}
      ORDER BY m.site, m.datmvt DESC
    `, p.values);

  res.json({ artnoid: id, fournisseurs_par_site: result.rows });
});

// GET /api/articles/:id/mouvements?dateDebut=&dateFin=&site=&page=&limit=
router.get('/:id/mouvements', async (req, res) => {
  const id = parseId(req.params.id);
  const site = parseText(req.query.site, 'site', { max: 20 });
  const { debut, fin } = parseDateRange(req.query, { defDebut: '2000-01-01', defFin: '2099-12-31' });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });

  const p = new SqlParams([id]);
  const conds = ['artnoid = $1', dateRange('datmvt', p, debut, fin)];
  // Filtre partiel sur le site (documenté), appliqué seulement s'il est fourni
  if (site) conds.push(`site LIKE ${p.add(likeContains(site))}`);

  const result = await getPool().query(`
      SELECT datmvt, site, libmvt, genremvt, qtemvt, valmvt,
             mntmvtht, mntmvtttc, margemvt, qtestock, prmp, codefou
      FROM mvtart
      ${where(conds)}
      ORDER BY datmvt DESC
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}
    `, p.values);

  res.json({ page, limit, mouvements: result.rows });
});

// GET /api/articles/:id/mensuel?dateDebut=&dateFin=&site=
// Ventes + stock fin de mois, mois par mois, par site
router.get('/:id/mensuel', async (req, res) => {
  const id = parseId(req.params.id);
  const site = parseText(req.query.site, 'site', { max: 20 });
  const { debut, fin } = parseDateRange(req.query, { defDebut: '2024-01-01', defFin: '2099-12-31' });

  // Construit les conditions communes (mêmes paramètres pour les 3 requêtes)
  const p = new SqlParams([id]);
  const conds = ['artnoid = $1', dateRange('datmvt', p, debut, fin)];
  if (site) conds.push(`site = ${p.add(site)}`);
  const baseWhere = conds.join(' AND ');
  const pool = getPool();

  const [ventes, receptions, stocks] = await Promise.all([
    // Ventes par mois et par site
    pool.query(`
      SELECT
        TO_CHAR(datmvt, 'YYYY-MM') AS mois,
        site,
        COUNT(*)           AS nb_passages,
        SUM(qtemvt)        AS qte_vendue,
        SUM(mntmvtht)      AS ca_ht,
        SUM(mntmvtttc)     AS ca_ttc,
        SUM(margemvt)      AS marge,
        CASE WHEN SUM(mntmvtht) != 0
          THEN ROUND(SUM(margemvt) / ABS(SUM(mntmvtht)) * 100, 2)
          ELSE 0 END       AS taux_marge
      FROM mvtart
      WHERE ${baseWhere} AND genremvt = 3
      GROUP BY TO_CHAR(datmvt, 'YYYY-MM'), site
      ORDER BY mois DESC, site
    `, p.values),
    // Réceptions par mois et par site
    pool.query(`
      SELECT
        TO_CHAR(datmvt, 'YYYY-MM') AS mois,
        site,
        COUNT(*)           AS nb_receptions,
        SUM(qtemvt)        AS qte_recue
      FROM mvtart
      WHERE ${baseWhere} AND genremvt = 1
      GROUP BY TO_CHAR(datmvt, 'YYYY-MM'), site
      ORDER BY mois DESC, site
    `, p.values),
    // Stock fin de mois : dernier qtestock de chaque mois par site
    pool.query(`
      SELECT DISTINCT ON (TO_CHAR(datmvt, 'YYYY-MM'), site)
        TO_CHAR(datmvt, 'YYYY-MM') AS mois,
        site,
        qtestock AS stock_fin_mois,
        prmp     AS prmp_fin_mois
      FROM mvtart
      WHERE ${baseWhere}
      ORDER BY TO_CHAR(datmvt, 'YYYY-MM'), site, datmvt DESC
    `, p.values),
  ]);

  // Assemblage par mois + site
  const key = (mois, s) => `${mois}|${s}`;
  const byKey = (rows) => Object.fromEntries(rows.map(r => [key(r.mois, r.site), r]));

  const sv = byKey(ventes.rows);
  const sr = byKey(receptions.rows);
  const ss = byKey(stocks.rows);

  const keys = [...new Set([
    ...Object.keys(sv), ...Object.keys(sr), ...Object.keys(ss),
  ])].sort().reverse();

  const data = keys.map(k => {
    const [mois, s] = k.split('|');
    return {
      mois,
      site: s,
      stock_fin_mois: ss[k]?.stock_fin_mois ?? null,
      prmp_fin_mois:  ss[k]?.prmp_fin_mois  ?? null,
      ventes: sv[k] ? {
        nb_passages: sv[k].nb_passages,
        qte_vendue:  sv[k].qte_vendue,
        ca_ht:       sv[k].ca_ht,
        ca_ttc:      sv[k].ca_ttc,
        marge:       sv[k].marge,
        taux_marge:  sv[k].taux_marge,
      } : null,
      receptions: sr[k] ? {
        nb_receptions: sr[k].nb_receptions,
        qte_recue:     sr[k].qte_recue,
      } : null,
    };
  });

  res.json({ artNoId: id, dateDebut: debut, dateFin: fin, data });
});

module.exports = router;
