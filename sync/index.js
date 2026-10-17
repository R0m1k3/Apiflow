// Toutes les dates manipulées en UTC (mssql lit les datetime SQL Server en UTC)
process.env.TZ = 'UTC';
require('dotenv').config();
const cron = require('node-cron');

const { syncArticles }     = require('./tables/articles');
const { syncFournisseurs } = require('./tables/fournisseurs');
const { syncStock }        = require('./tables/stock');
const { syncMouvements, syncStatOpCA } = require('./tables/mouvements');
const { syncCommandes }    = require('./tables/commandes');
const { syncReferentiel }  = require('./tables/referentiel');
const { syncRanking }      = require('./tables/ranking');
const { syncStatDispoperm } = require('./tables/stat_dispoperm');
const { syncPhenixQuantiteConseille } = require('./tables/phenix_quantite_conseille');
const { syncAppro }              = require('./tables/appro');
const { syncPublicites }         = require('./tables/publicites');
const { syncPrix }               = require('./tables/prix');
const S = require('./tables/specs');
const { runSpecs, runSpec } = require('./engine');
const { applySchema } = require('./schema');
const { getPg, closeAll } = require('./db');
const { logSync } = require('./utils');
const { sendAlert } = require('./alert');
const {
  startDashboard, registerSyncFn,
  setSyncRunning, isSyncRunning,
  setProgress, clearProgress,
} = require('./server');

const forceMode = process.argv.includes('--force');
const CRON      = process.env.SYNC_CRON || '45 6 * * *';
const LOCK_KEY  = 4242001;   // pg_advisory_lock : une seule sync à la fois, tous process confondus

const specs = (list) => (force) => runSpecs(list, { force });

// Groupes dans l'ordre d'exécution
const STEPS = [
  { name: 'referentiel',       fn: syncReferentiel },
  { name: 'referentiels+',     fn: specs(S.REFERENTIELS) },
  { name: 'articles',          fn: syncArticles },
  { name: 'articles+',         fn: specs([...S.ARTICLES_PLUS, S.HISTO_ART_GAMME]) },
  { name: 'prix',              fn: syncPrix },
  { name: 'fournisseurs',      fn: syncFournisseurs },
  { name: 'fournisseurs+',     fn: specs(S.FOURNISSEURS_PLUS) },
  { name: 'stock',             fn: syncStock },
  { name: 'mouvements',        fn: syncMouvements },
  { name: 'statopca',          fn: syncStatOpCA },
  { name: 'commandes',         fn: syncCommandes },
  { name: 'ranking',           fn: syncRanking },
  { name: 'stat_dispoperm',    fn: syncStatDispoperm },
  { name: 'phenix_quantite_conseille', fn: syncPhenixQuantiteConseille },
  { name: 'appro',             fn: syncAppro },
  { name: 'rotation',          fn: specs(S.ROTATION) },
  { name: 'publicites',        fn: syncPublicites },
  { name: 'promotions',        fn: specs(S.PROMOS) },
  { name: 'pilotage',          fn: specs([...S.BUDGETS, ...S.METEO, S.DEMARQUES]) },
  { name: 'inventaires',       fn: specs(S.INVENTAIRES) },
  { name: 'clients_web',       fn: specs(S.CLIENTS_WEB) },
];

async function syncAll(force = false) {
  if (isSyncRunning()) {
    console.log('Sync déjà en cours — ignorée.');
    return;
  }
  setSyncRunning(true);
  clearProgress();
  const pg = getPg();
  const start = Date.now();
  const startedAt = new Date();
  let lockClient = null;

  try {
    // Verrou inter-process (cron + `node index.js --force` lancés en parallèle)
    lockClient = await pg.connect();
    const { rows: [{ ok }] } = await lockClient.query('SELECT pg_try_advisory_lock($1) AS ok', [LOCK_KEY]);
    if (!ok) {
      console.log('Une autre sync tient le verrou PostgreSQL — ignorée.');
      lockClient.release();
      lockClient = null;
      return;
    }

    console.log(`\n=== SYNC START ${startedAt.toISOString()} (force=${force}) ===`);
    // Marqueur lu par la sync Hostinger (elle attend la fin avant de copier)
    await logSync(pg, '_run', 0, 'running');

    try {
      const { applied } = await applySchema();
      await logSync(pg, '_schema', applied.length, 'ok');
    } catch (err) {
      console.error(`[schema] ERREUR: ${err.message}`);
      await logSync(pg, '_schema', 0, 'error', err.message);
    }

    for (let i = 0; i < STEPS.length; i++) {
      const { name, fn } = STEPS[i];
      setProgress(name, i + 1, STEPS.length);
      console.log(`[${i + 1}/${STEPS.length}] Sync ${name}…`);
      try {
        await fn(force);
      } catch (err) {
        console.error(`[${name}] ERREUR NON CATCHÉE: ${err.message}`);
        await logSync(pg, name, 0, 'error', err.message);
      }
    }

    const { rows: errors } = await pg.query(
      `SELECT table_name, error_msg FROM sync_log
       WHERE status = 'error' AND last_sync >= $1 AND table_name <> '_run' ORDER BY table_name`,
      [startedAt]
    );
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    await logSync(pg, '_run', STEPS.length, errors.length ? 'error' : 'ok',
      errors.length ? `${errors.length} table(s) en erreur` : null);
    console.log(`=== SYNC DONE en ${elapsed}s — ${errors.length} erreur(s) ===\n`);
    if (errors.length) {
      await sendAlert(`Sync SQL Server → PG Debian : ${errors.length} table(s) en erreur\n`
        + errors.map((e) => `• ${e.table_name} : ${e.error_msg}`).join('\n'));
    }
  } catch (err) {
    console.error(`[sync] ERREUR FATALE: ${err.message}`);
    await logSync(pg, '_run', 0, 'error', err.message);
    await sendAlert(`Sync SQL Server → PG Debian : erreur fatale — ${err.message}`);
  } finally {
    if (lockClient) {
      await lockClient.query('SELECT pg_advisory_unlock($1)', [LOCK_KEY]).catch(() => {});
      lockClient.release();
    }
    setSyncRunning(false);
    clearProgress();
  }
}

registerSyncFn(syncAll, CRON);

if (forceMode) {
  console.log('Mode force : sync complète en cours...');
  syncAll(true)
    .then(() => closeAll())
    .then(() => process.exit(0))
    .catch(err => {
      console.error('Erreur fatale sync:', err.message);
      process.exit(1);
    });
} else {
  startDashboard();
  console.log(`Service sync démarré — cron: ${CRON} Europe/Paris`);
  cron.schedule(CRON, () => {
    syncAll(false).catch(err => console.error('Erreur cron sync:', err.message));
  }, { timezone: 'Europe/Paris' });
}

module.exports = { syncAll, runSpec };
