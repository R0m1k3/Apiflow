require('dotenv').config();
const cron = require('node-cron');

const { syncArticles }     = require('./tables/articles');
const { syncFournisseurs } = require('./tables/fournisseurs');
const { syncStock }        = require('./tables/stock');
const { syncMouvements }   = require('./tables/mouvements');
const { syncCommandes }    = require('./tables/commandes');
const { syncReferentiel }  = require('./tables/referentiel');
const { syncRanking }      = require('./tables/ranking');
const { closeAll }         = require('./db');
const {
  startDashboard, registerSyncFn,
  setSyncRunning, isSyncRunning,
  setProgress, clearProgress,
} = require('./server');

const forceMode = process.argv.includes('--force');

// Groupes dans l'ordre d'exécution
const STEPS = [
  { name: 'referentiel',  fn: syncReferentiel },
  { name: 'articles',     fn: syncArticles },
  { name: 'fournisseurs', fn: syncFournisseurs },
  { name: 'stock',        fn: syncStock },
  { name: 'mouvements',   fn: syncMouvements },
  { name: 'commandes',    fn: syncCommandes },
  { name: 'ranking',      fn: syncRanking },
];

async function syncAll(force = false) {
  if (isSyncRunning()) {
    console.log('Sync déjà en cours — ignorée.');
    return;
  }
  setSyncRunning(true);
  clearProgress();
  const start = Date.now();
  console.log(`\n=== SYNC START ${new Date().toISOString()} (force=${force}) ===`);

  try {
    for (let i = 0; i < STEPS.length; i++) {
      const { name, fn } = STEPS[i];
      setProgress(name, i + 1);
      console.log(`[${i + 1}/${STEPS.length}] Sync ${name}…`);
      try {
        await fn(force);
      } catch (err) {
        console.error(`[${name}] ERREUR NON CATCHÉE: ${err.message}`);
      }
    }
  } finally {
    setSyncRunning(false);
    clearProgress();
  }

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`=== SYNC DONE en ${elapsed}s ===\n`);
}

registerSyncFn(syncAll);

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
  console.log('Service sync démarré — cron: 0 3 * * * (3h00 chaque nuit)');
  cron.schedule('0 3 * * *', () => {
    syncAll(false).catch(err => console.error('Erreur cron sync:', err.message));
  });
}
