// Référentiels (petites tables de correspondance) + commandes clients et web (sans données nominatives).
const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { parseId, parseText, parsePagination } = require('../utils/params');
const { SqlParams, dateRange, where } = require('../utils/sql');
const { HttpError } = require('../utils/errors');
const { periode } = require('./_periode');

const REFS = {
  'types-mouvement':  `SELECT t.no_id, t.codemvt, t.libellemvt, t.sensmvt, t.genremvt, t.cumul, t.mvtgrpnoid, g.libellegrpmvt AS groupe
                       FROM mvttype t LEFT JOIN mvtgrp g ON g.no_id = t.mvtgrpnoid ORDER BY t.codemvt`,
  'modes-reglement':  `SELECT no_id, codreg, libreg, codcai, compta FROM mode_reglement ORDER BY codreg`,
  sites:              `SELECT code, nom, sit_code, niveau, est_base, is_depot FROM sites ORDER BY code`,
  tarifs:             `SELECT no_id, tc_code, tc_nom, devise_code FROM tarif_client ORDER BY tc_code`,
  'etats-commande':   `SELECT no_id, etat, libelle, couleur FROM etat_commande ORDER BY no_id`,
  'motifs-refus':     `SELECT no_id, code, libelle FROM motifs_refus ORDER BY code`,
  'temps-meteo':      `SELECT no_id, libelle FROM meteo_temps ORDER BY no_id`,
};

// GET /api/referentiels — liste ; GET /api/referentiels/:nom — contenu
router.get('/', (req, res) => res.json({ referentiels: Object.keys(REFS) }));
router.get('/:nom', async (req, res) => {
  const sql = REFS[req.params.nom];
  if (!sql) throw new HttpError(404, `Référentiel inconnu (disponibles : ${Object.keys(REFS).join(', ')})`);
  const { rows } = await getPool().query(sql);
  res.json({ referentiel: req.params.nom, lignes: rows });
});

// GET /api/referentiels/inventaires/liste — campagnes d'inventaire
router.get('/inventaires/liste', async (req, res) => {
  const { rows } = await getPool().query(`
    SELECT no_id, codesite, libelle, typeinventaire, datedebut, datefin, datevalidation, datemvt, ventesjourinv
    FROM inv_suivi ORDER BY datedebut DESC NULLS LAST`);
  res.json({ inventaires: rows });
});

// ── Commandes clients (COMMANDECLIENT / CDECLILIGNE) ─────────
const clients = express.Router();

// GET /api/clients/commandes?dateDebut&dateFin&etat&page&limit
clients.get('/commandes', async (req, res) => {
  const { debut, fin } = periode(req.query, { defJours: 365 });
  const etat = parseText(req.query.etat, 'etat', { max: 5 });
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });
  const p = new SqlParams();
  const conds = [dateRange('c.datcdecli', p, debut, fin)];
  if (etat) conds.push(`c.etatcdecli = ${p.add(etat)}::smallint`);
  const [list, synth] = await Promise.all([
    getPool().query(`
      SELECT c.idcdecli, c.numcdecli, c.datcdecli, c.etatcdecli, c.etat_second, c.typcdecli, c.typliv,
             c.mntttc, c.mnt_encaisse, c.mnt_a_encaisser, c.acompte_pc, c.datlivsou, c.ddisposition,
             (SELECT COUNT(*) FROM commande_client_ligne l WHERE l.idcdecli = c.idcdecli) AS nb_lignes
      FROM commande_client c ${where(conds)}
      ORDER BY c.datcdecli DESC
      LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}`, p.values),
    getPool().query(`
      SELECT c.etatcdecli, COUNT(*) AS nb, ROUND(SUM(c.mntttc), 2) AS montant_ttc,
             ROUND(SUM(c.mnt_a_encaisser), 2) AS reste_a_encaisser
      FROM commande_client c ${where(conds)} GROUP BY c.etatcdecli ORDER BY c.etatcdecli`, p.values.slice(0, p.values.length - 2)),
  ]);
  res.json({ periode: { debut, fin }, page, limit, synthese: synth.rows, commandes: list.rows });
});

// GET /api/clients/commandes/:id — lignes d'une commande client
clients.get('/commandes/:id', async (req, res) => {
  const id = parseId(req.params.id);
  const [head, lignes] = await Promise.all([
    getPool().query(`SELECT * FROM commande_client WHERE idcdecli = $1`, [id]),
    getPool().query(`
      SELECT l.idcdeclilig, l.nlcdeclilig, l.idarticle, a.codein, COALESCE(l.lib1cdeclilig, a.libelle1) AS libelle,
             l.eancdeclilig, l.qtecdeclilig, l.puttcnetcdeclilig, l.mntttcnetcdeclilig, l.etatclilig,
             l.dpaiement, l.dlivraison, l.dateclientinforme
      FROM commande_client_ligne l LEFT JOIN articles a ON a.no_id = l.idarticle
      WHERE l.idcdecli = $1 ORDER BY l.nlcdeclilig`, [id]),
  ]);
  if (!head.rows.length) throw new HttpError(404, 'Commande client introuvable');
  res.json({ ...head.rows[0], lignes: lignes.rows });
});

// GET /api/clients/web/commandes?dateDebut&dateFin — commandes web / click & collect (statuts)
clients.get('/web/commandes', async (req, res) => {
  const { debut, fin } = periode(req.query, { defJours: 90 });
  const p = new SqlParams();
  const cond = dateRange('w.daterecup', p, debut, fin);
  const [synth, list] = await Promise.all([
    getPool().query(`SELECT COALESCE(w.status, '(vide)') AS status, COUNT(*) AS nb,
                       ROUND(AVG(EXTRACT(EPOCH FROM (w.datetraitement - w.daterecup)) / 3600)::numeric, 1) AS delai_traitement_h
                     FROM web_commande w WHERE ${cond} GROUP BY 1 ORDER BY 2 DESC`, p.values),
    getPool().query(`SELECT w.numcommande, w.daterecup, w.datetraitement, w.deliver, w.status
                     FROM web_commande w WHERE ${cond} ORDER BY w.daterecup DESC LIMIT 1000`, p.values),
  ]);
  res.json({ periode: { debut, fin }, synthese: synth.rows, commandes: list.rows });
});

// GET /api/clients/web/stock?article=<codein|id>&interdit=true — stock exposé au site web
clients.get('/web/stock', async (req, res) => {
  const article = parseText(req.query.article, 'article', { max: 30 });
  const interdit = req.query.interdit === 'true';
  const { page, limit, offset } = parsePagination(req.query, { defLimit: 100, maxLimit: 1000 });
  const p = new SqlParams();
  const conds = [];
  if (article) conds.push(`w.idarticle = ${p.add(article)}`);
  if (interdit) conds.push('w.interditvte = true');
  const { rows } = await getPool().query(`
    SELECT w.idarticle, w.idmag, w.qtestock, w.qteencde, w.pvmag, w.interditvte, w.datemaj
    FROM art_web_stock w ${where(conds)}
    ORDER BY w.datemaj DESC NULLS LAST
    LIMIT ${p.add(limit)} OFFSET ${p.add(offset)}`, p.values);
  res.json({ page, limit, stock: rows });
});

module.exports = router;
module.exports.clients = clients;
