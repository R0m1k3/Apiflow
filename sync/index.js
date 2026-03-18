require('dotenv').config();
const cron = require('node-cron');

const { syncArticles }     = require('./tables/articles');
const { syncFournisseurs } = require('./tables/fournisseurs');
const { syncStock }        = require('./tables/stock');
const { syncMouvements }   = require('./tables/mouvements');
const { syncCommandes }    = require('./tables/commandes');
const { syncReferentiel }  = require('./tables/referentiel');
const { closeAll }         = require('./db');

const forceMode = process.argv.includes('--force');

async function syncAll(force = false) {
  const start = Date.now();
  console.log(`\n=== SYNC START ${new Date().toISOString()} (force=${force}) ===`);

  // Ordre important : référentiel avant articles, articles avant stock/mouvements
  await syncReferentiel(force);
  await syncArticles(force);
  await syncFournisseurs(force);
  await syncStock(force);
  await syncMouvements(force);
  await syncCommandes(force);

  const elapsed = ((Date.now() - start) / 1000).toFixed(1);
  console.log(`=== SYNC DONE en ${elapsed}s ===\n`);
}

if (forceMode) {
  // Lancement manuel : node index.js --force
  console.log('Mode force : sync complète en cours...');
  syncAll(true)
    .then(() => closeAll())
    .then(() => process.exit(0))
    .catch(err => {
      console.error('Erreur fatale sync:', err.message);
      process.exit(1);
    });
} else {
  // Mode daemon : sync nightly à 3h00
  console.log('Service sync démarré — cron: 0 3 * * * (3h00 chaque nuit)');
  cron.schedule('0 3 * * *', () => {
    syncAll(false).catch(err => console.error('Erreur cron sync:', err.message));
  });
}
