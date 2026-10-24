require('dotenv').config({ quiet: true });
const express = require('express');
const compression = require('compression');
const { getPool, closePool } = require('./config/database');

const { requestLogger } = require('./middleware/logger');
const { securityHeaders, cors } = require('./middleware/security');
const { rateLimit, stopRateLimit, RATE_LIMIT } = require('./middleware/rateLimit');
const { authMiddleware, authEnabled, checkAuthConfig, startAuthWarnings, stopAuthWarnings } = require('./middleware/auth');
const { responseCache } = require('./middleware/cache');
const { errorHandler, notFound } = require('./middleware/errorHandler');

const app = express();
app.disable('x-powered-by');
// Derrière nginx (VPS) → frp : l'IP client vient de X-Forwarded-For
app.set('trust proxy', process.env.TRUST_PROXY || 'loopback, linklocal, uniquelocal');

// ── Middlewares globaux (l'ordre compte) ────────────────────────────────────
app.use(requestLogger);
app.use(securityHeaders);
app.use(cors);          // répond aux pré-vols CORS avant l'authentification
app.use(rateLimit);
app.use(compression({
  // Pas de compression pour le proxy photo (images déjà compressées, streaming)
  filter: (req, res) => !/^\/api\/articles\/[^/]+\/photo\/?$/.test(req.path) && compression.filter(req, res),
}));
app.use(express.json({ limit: '100kb' }));

// ── Santé (publique) ────────────────────────────────────────────────────────
const HEALTH_DB_TIMEOUT_MS = 3000;
app.get('/api/health', async (req, res) => {
  let db = 'ok';
  try {
    let timer;
    const timeout = new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error('timeout')), HEALTH_DB_TIMEOUT_MS);
    });
    try {
      await Promise.race([getPool().query('SELECT 1'), timeout]);
    } finally {
      clearTimeout(timer);
    }
  } catch (err) {
    db = 'error';
    console.error(`[health] base inaccessible : ${err.message}`);
  }
  res.set('Cache-Control', 'no-store');
  res.status(db === 'ok' ? 200 : 503).json({
    status: db === 'ok' ? 'ok' : 'error',
    db,
    auth: authEnabled ? 'enabled' : 'disabled',
  });
});

// ── Authentification puis cache de réponses ─────────────────────────────────
app.use(authMiddleware);
app.use(responseCache);

// ── Routes ──────────────────────────────────────────────────────────────────
const ROUTES = [
  ['/api/articles',       './routes/articles', [
    'GET /api/articles[?search=&codein=&ean=&codefou=&actif=&artcentrale=&has_artcentrale=&page=&limit=]',
    'GET /api/articles/:id',
    'GET /api/articles/:id/referentiel',
    'GET /api/articles/:id/photo[?size=large]   (public)',
    'GET /api/articles/:id/dernier-fournisseur[?site=]',
    'GET /api/articles/:id/mouvements[?dateDebut=&dateFin=&site=&page=&limit=]',
    'GET /api/articles/:id/mensuel[?dateDebut=&dateFin=&site=]',
  ]],
  ['/api/fournisseurs',   './routes/fournisseurs', [
    'GET /api/fournisseurs[?search=&page=&limit=]',
    'GET /api/fournisseurs/:code/articles[?page=&limit=]',
    'GET /api/fournisseurs/:code/commandes[?dateDebut=&dateFin=&page=&limit=]',
    'GET /api/fournisseurs/:code/franco',
  ]],
  ['/api/commandes',      './routes/commandes', [
    'GET /api/commandes[?dateDebut=&dateFin=&codefou=&page=&limit=]',
    'GET /api/commandes/:noCommande',
    'GET /api/commandes/receptions/liste[?dateDebut=&dateFin=&page=&limit=]',
  ]],
  ['/api/stock',          './routes/stock', [
    'GET /api/stock[?site=&page=&limit=]',
    'GET /api/stock/article/:id',
    'GET /api/stock/article/:id/historique[?date=&site=]',
    'GET /api/stock/article/:id/periode[?dateDebut=&dateFin=&site=]',
    'GET /api/stock/site/:site[?page=&limit=]',
    'GET /api/stock/valorisation',
  ]],
  ['/api/mouvements',     './routes/mouvements', [
    'GET /api/mouvements/types',
    'GET /api/mouvements/articles[?dateDebut=&dateFin=&site=&codefou=&genremvt=&page=&limit=]',
    'GET /api/mouvements/entrees[?dateDebut=&dateFin=&site=&artNoId=&page=&limit=]',
    'GET /api/mouvements/regularisations[?dateDebut=&dateFin=&site=&artNoId=&page=&limit=]',
    'GET /api/mouvements/reglements[?dateDebut=&dateFin=&page=&limit=]',
    'GET /api/mouvements/synthese[?dateDebut=&dateFin=&site=&page=&limit=]',
  ]],
  ['/api/performance',    './routes/performance', [
    'GET /api/performance/ca[?dateDebut=&dateFin=&site=&groupBy=jour|mois]',
    'GET /api/performance/hitparade[?dateDebut=&dateFin=&site=&limit=&groupBy=qte|ca|marge&codefou=]',
    'GET /api/performance/ca/nomenclature[?dateDebut=&dateFin=&site=&niveau=]',
    'GET /api/performance/ca/fournisseur[?dateDebut=&dateFin=&site=]',
    'GET /api/performance/ca/gamme[?dateDebut=&dateFin=&site=]',
    'GET /api/performance/dashboard[?date=&site=]',
  ]],
  ['/api/sync',           './routes/sync', ['GET /api/sync/status']],
  ['/api/ranking',        './routes/ranking', [
    'GET /api/ranking[?gencod=&codein=&site=&foucentrale=&limit=]',
    'GET /api/ranking/article/:id',
  ]],
  ['/api/publicites',     './routes/publicites', [
    'GET /api/publicites[?search=&site=&dateDebut=&dateFin=&statut=&page=&limit=]',
    'GET /api/publicites/historique[?search=&dateDebut=&dateFin=&statut=]',
    'GET /api/publicites/:code[?site=]',
  ]],
  ['/api/commandes-auto', './routes/commandes-auto', [
    'GET /api/commandes-auto[?site=&codefou=]',
    'GET /api/commandes-auto/:codefou[?site=]',
  ]],
  ['/api/photos',         './routes/photos', [
    'POST /api/photos/reindex',
    'GET /api/photos/status',
  ]],
  // ── Analyses ajoutées oct. 2026 (nouvelles données synchronisées) ──
  ['/api/ventes',         './routes/ventes', [
    'GET /api/ventes/horaire[?dateDebut=&dateFin=&site=]',
    'GET /api/ventes/jours[?dateDebut=&dateFin=&site=]',
    'GET /api/ventes/paniers[?dateDebut=&dateFin=&site=]',
    'GET /api/ventes/associations/:id[?dateDebut=&dateFin=&site=&limit=]',
    'GET /api/ventes/reglements[?dateDebut=&dateFin=]',
    'GET /api/ventes/remises[?dateDebut=&dateFin=&site=&groupBy=motif|valeur|article&limit=]',
  ]],
  ['/api/pilotage',       './routes/pilotage', [
    'GET /api/pilotage/objectifs[?annee=&site=&niveau=1|2&nomenclature=]',
    'GET /api/pilotage/ca-jour[?dateDebut=&dateFin=&site=]',
    'GET /api/pilotage/meteo[?dateDebut=&dateFin=&site=]',
    'GET /api/pilotage/meteo/previsions[?site=]',
    'GET /api/pilotage/demarque[?dateDebut=&dateFin=&site=&groupBy=type|nomenclature|article&limit=]',
    'GET /api/pilotage/fidelite[?dateDebut=&dateFin=&site=]',
  ]],
  ['/api/prix',           './routes/prix', [
    'GET /api/prix/article/:id',
    'GET /api/prix/changements[?depuis=&site=&sens=hausse|baisse&page=&limit=]',
    'GET /api/prix/etiquettes[?traite=0|1&page=&limit=]',
    'GET /api/prix/promotions[?date=&site=&search=]',
    'GET /api/prix/promotions/:periodeId/articles[?limit=]',
  ]],
  ['/api/appro',          './routes/approvisionnement', [
    'GET /api/appro/suivi-commandes[?codefou=&etat=&site=&enCours=&page=&limit=]',
    'GET /api/appro/litiges[?dateDebut=&dateFin=&codefou=&groupBy=fournisseur|motif|detail]',
    'GET /api/appro/fournisseurs/:code/conditions',
    'GET /api/appro/rotation[?site=&filtre=surstock|sans_vente|tout&page=&limit=]',
    'GET /api/appro/article/:id',
  ]],
  ['/api/referentiels',   './routes/referentiels', [
    'GET /api/referentiels',
    'GET /api/referentiels/:nom   (types-mouvement, modes-reglement, sites, tarifs, etats-commande, motifs-refus, temps-meteo)',
    'GET /api/referentiels/inventaires/liste',
  ]],
  ['/api/clients',        './routes/clients', [
    'GET /api/clients/commandes[?dateDebut=&dateFin=&etat=&page=&limit=]',
    'GET /api/clients/commandes/:id',
    'GET /api/clients/web/commandes[?dateDebut=&dateFin=]',
    'GET /api/clients/web/stock[?article=&interdit=true&page=&limit=]',
  ]],
];

// Schéma de la base : uniquement sur demande explicite
const exposeSchema = process.env.EXPOSE_SCHEMA === 'true';
if (exposeSchema) {
  ROUTES.unshift(['/api/schema', './routes/schema', [
    'GET /api/schema',
    'GET /api/schema/tables[?search=]',
    'GET /api/schema/top[?limit=]',
    'GET /api/schema/tables/:tableName',
  ]]);
}

for (const [mount, modulePath] of ROUTES) {
  app.use(mount, require(modulePath));
}

app.use(notFound);
app.use(errorHandler);

// ── Démarrage ───────────────────────────────────────────────────────────────
const PORT = parseInt(process.env.API_PORT, 10) || 3000;

if (require.main === module) {
  if (!checkAuthConfig()) process.exit(1);
  const server = app.listen(PORT, () => {
    console.log(`\nAPI Foirfouille démarrée sur le port ${PORT} (PostgreSQL)`);
    console.log(`Limite de débit : ${RATE_LIMIT} requêtes/min/IP`);
    console.log('\nEndpoints disponibles :');
    console.log('  GET /api/health   (public)');
    for (const [, , endpoints] of ROUTES) {
      for (const e of endpoints) console.log(`  ${e}`);
    }
    if (!exposeSchema) console.log('  (/api/schema désactivé — EXPOSE_SCHEMA=true pour l\'activer)');
    console.log('');
    startAuthWarnings();
  });

  // Arrêt propre : on cesse d'accepter des connexions, puis on ferme le pool
  let stopping = false;
  const shutdown = (signal) => {
    if (stopping) return;
    stopping = true;
    console.log(`${signal} reçu — arrêt en cours…`);
    stopAuthWarnings();
    stopRateLimit();
    const force = setTimeout(() => {
      console.error('Arrêt forcé (délai dépassé)');
      process.exit(1);
    }, 10000);
    force.unref();
    server.close(async () => {
      try {
        await closePool();
      } catch (err) {
        console.error('Erreur fermeture pool :', err.message);
      }
      console.log('Arrêt terminé.');
      process.exit(0);
    });
    // Ferme les connexions keep-alive inactives pour accélérer l'arrêt
    server.closeIdleConnections?.();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

module.exports = app;
