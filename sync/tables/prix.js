/**
 * Historique des prix de vente (LIG_TC_MAGASIN + LIG_TC_CENTRALE).
 *
 * Le prix est stocké côté SQL Server sous forme d'historique texte :
 *   "[01/01/1901,4.99:26/09/2022,7.99:08/01/2025,9.99]"
 * → une ligne par (article, site, tarif, date d'effet).
 *
 * Delta par SUIVIDATEMODIF (−2 jours de recouvrement) ; rechargement complet
 * à la première sync, en mode force, ou si le dernier complet a plus de 7 jours
 * (rattrape les lignes supprimées côté source).
 */
'use strict';
const { getPg } = require('../db');
const { getLastSync, logSync } = require('../utils');
const { runSpec } = require('../engine');

const COLS = ['article_no_id', 'site', 'tarif_client_no_id', 'date_effet', 'prix'];
const PK   = ['article_no_id', 'site', 'tarif_client_no_id', 'date_effet'];

/** "[01/01/1901,4.99:26/09/2022,7.99]" → [{date_effet:'1901-01-01', prix:4.99}, …] */
function parseHistorique(txt) {
  if (txt == null) return [];
  const out = new Map();
  const body = String(txt).trim().replace(/^\[/, '').replace(/\]$/, '');
  for (const part of body.split(':')) {
    const m = part.trim().match(/^(\d{2})\/(\d{2})\/(\d{4}),\s*(-?\d+(?:[.,]\d+)?)$/);
    if (!m) continue;
    out.set(`${m[3]}-${m[2]}-${m[1]}`, Number(m[4].replace(',', '.')));
  }
  return [...out.entries()].map(([date_effet, prix]) => ({ date_effet, prix }));
}

function expandRow(r, site) {
  const article = Number(r.ARTICLE_NO_ID);
  const tarif   = Number(r.TARIF_CLIENT_NO_ID);
  if (!Number.isFinite(article) || !Number.isFinite(tarif)) return [];
  return parseHistorique(r.PRIX).map((h) => ({ article_no_id: article, site, tarif_client_no_id: tarif, ...h }));
}

const SOURCES = [
  { sql: 'SELECT ARTICLE_NO_ID, TARIF_CLIENT_NO_ID, SITE_CODE, LTC_MAG_PRIXVENTE AS PRIX FROM LIG_TC_MAGASIN WITH (NOLOCK)', site: (r) => String(r.SITE_CODE || '').trim() || '???' },
  { sql: 'SELECT ARTICLE_NO_ID, TARIF_CLIENT_NO_ID, LTC_CEN_PRIXVENTE AS PRIX FROM LIG_TC_CENTRALE WITH (NOLOCK)', site: () => '000' },
];

const PRIX_SPEC = {
  name: 'prix_vente_historique',
  mode: 'custom',
  async run({ force }, { streamQuery, insertRows, withTx }) {
    const pg = getPg();
    const lastSync = await getLastSync(pg, 'prix_vente_historique');
    const lastFull = await getLastSync(pg, 'prix_vente_historique:full', 0);
    const fullDue  = force || !lastSync || !lastFull || (Date.now() - new Date(lastFull)) > 7 * 86400e3;

    let total = 0;
    if (fullDue) {
      // Rechargement complet dans une seule transaction
      total = await withTx(async (client) => {
        await client.query('DELETE FROM prix_vente_historique');
        let n = 0;
        for (const src of SOURCES) {
          await streamQuery(src.sql, {}, async (rows) => {
            n += await insertRows(client, 'prix_vente_historique', COLS, rows.flatMap((r) => expandRow(r, src.site(r))), PK);
          });
        }
        return n;
      });
      await logSync(pg, 'prix_vente_historique:full', total, 'ok');
      return total;
    }

    // Delta : on remplace l'historique complet des lignes modifiées
    for (const src of SOURCES) {
      await streamQuery(`${src.sql} WHERE SUIVIDATEMODIF >= @since`, { since: lastSync }, async (rows) => {
        await withTx(async (client) => {
          const keys = rows.map((r) => [Number(r.ARTICLE_NO_ID), src.site(r), Number(r.TARIF_CLIENT_NO_ID)]);
          await client.query(
            `DELETE FROM prix_vente_historique p
             USING unnest($1::bigint[], $2::text[], $3::bigint[]) AS k(a, s, t)
             WHERE p.article_no_id = k.a AND p.site = k.s AND p.tarif_client_no_id = k.t`,
            [keys.map((k) => k[0]), keys.map((k) => k[1]), keys.map((k) => k[2])]
          );
          total += await insertRows(client, 'prix_vente_historique', COLS, rows.flatMap((r) => expandRow(r, src.site(r))), PK);
        });
      });
    }
    return total;
  },
};

async function syncPrix(force) {
  await runSpec(PRIX_SPEC, { force });
}

module.exports = { syncPrix, parseHistorique };
