/**
 * Mouvements de stock / ventes / règlements.
 *
 * Avant (bug) : insert-only sur `DatMvt > MAX(datmvt)`. DatMvt n'ayant pas
 * d'heure, une sync lancée en journée chargeait une journée partielle puis
 * ne la complétait jamais ; mvtreg n'avait pas de clé → doublons à chaque
 * sync forcée ; les suppressions n'étaient jamais propagées.
 *
 * Maintenant : fenêtre glissante de 10 jours remplacée chaque nuit, par mois,
 * en transaction (cf. engine.js), + propagation de mvtart_delete.
 */
'use strict';
const { runSpec, withTx } = require('../engine');
const { getMssql, getPg } = require('../db');
const { logSync } = require('../utils');
const { MVTART, MVTREG, MVTART_MOTIF, STATOPCA, FIDELITE_JOUR } = require('./specs');

/** Supprime côté PG les mouvements supprimés côté SQL Server (table mvtart_delete) */
async function propagateDeletes() {
  const pg = getPg();
  try {
    const ms = await getMssql();
    const { recordset } = await ms.request().query('SELECT NO_ID FROM mvtart_delete WITH (NOLOCK)');
    const ids = recordset.map((r) => Number(r.NO_ID)).filter(Number.isFinite);
    let deleted = 0;
    for (let i = 0; i < ids.length; i += 20000) {
      const part = ids.slice(i, i + 20000);
      deleted += await withTx(async (client) => {
        const a = await client.query('DELETE FROM mvtart WHERE no_id = ANY($1::bigint[])', [part]);
        await client.query('DELETE FROM mvtart_motif WHERE mvtart_no_id = ANY($1::bigint[])', [part]);
        return a.rowCount;
      });
    }
    await logSync(pg, 'mvtart_delete', deleted, 'ok');
    if (deleted) console.log(`[mvtart_delete] ${deleted} mouvements supprimés propagés`);
  } catch (err) {
    console.error(`[mvtart_delete] ERREUR: ${err.message}`);
    await logSync(pg, 'mvtart_delete', 0, 'error', err.message);
  }
}

async function syncMouvements(force) {
  await runSpec(MVTART, { force });
  await runSpec(MVTART_MOTIF, { force });
  await propagateDeletes();
  await runSpec(MVTREG, { force });
  await runSpec(FIDELITE_JOUR, { force });
}

async function syncStatOpCA(force) {
  await runSpec(STATOPCA, { force });
}

module.exports = { syncMouvements, syncStatOpCA };
