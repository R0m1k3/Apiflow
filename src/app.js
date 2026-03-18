require('dotenv').config();
const express = require('express');
const { getPool } = require('./config/database');

const schemaRouter      = require('./routes/schema');
const articlesRouter    = require('./routes/articles');
const fournisseursRouter = require('./routes/fournisseurs');
const commandesRouter   = require('./routes/commandes');
const stockRouter       = require('./routes/stock');
const mouvementsRouter  = require('./routes/mouvements');
const performanceRouter = require('./routes/performance');
const syncRouter        = require('./routes/sync');

const app = express();
app.use(express.json());

// Health check
app.get('/api/health', async (req, res) => {
  try {
    const pool = getPool();
    await pool.query('SELECT 1 AS ok');
    res.json({ status: 'ok', database: process.env.PG_DB, server: process.env.PG_HOST });
  } catch (err) {
    res.status(500).json({ status: 'error', detail: err.message });
  }
});

// Routes
app.use('/api/schema',       schemaRouter);
app.use('/api/articles',     articlesRouter);
app.use('/api/fournisseurs', fournisseursRouter);
app.use('/api/commandes',    commandesRouter);
app.use('/api/stock',        stockRouter);
app.use('/api/mouvements',   mouvementsRouter);
app.use('/api/performance',  performanceRouter);
app.use('/api/sync',         syncRouter);

// 404
app.use((req, res) => {
  res.status(404).json({ error: 'Route introuvable' });
});

const PORT = process.env.API_PORT || 3000;
app.listen(PORT, () => {
  console.log(`\nAPI Foirfouille démarrée sur http://localhost:${PORT}`);
  console.log(`Base : ${process.env.PG_HOST}/${process.env.PG_DB} (PostgreSQL)\n`);
  console.log('Endpoints disponibles :');
  console.log('  GET /api/health');
  console.log('  GET /api/schema/tables[?search=]');
  console.log('  GET /api/schema/top[?limit=]');
  console.log('  GET /api/schema/tables/:nom');
  console.log('  GET /api/articles[?search=&codein=&ean=&codefou=&actif=&page=&limit=]');
  console.log('  GET /api/articles/:id');
  console.log('  GET /api/articles/:id/referentiel');
  console.log('  GET /api/articles/:id/mouvements');
  console.log('  GET /api/fournisseurs[?search=]');
  console.log('  GET /api/fournisseurs/:code/articles');
  console.log('  GET /api/fournisseurs/:code/commandes');
  console.log('  GET /api/commandes[?dateDebut=&dateFin=&codefou=]');
  console.log('  GET /api/commandes/:noCommande');
  console.log('  GET /api/commandes/receptions/liste');
  console.log('  GET /api/stock[?site=]');
  console.log('  GET /api/stock/article/:id');
  console.log('  GET /api/stock/site/:site');
  console.log('  GET /api/stock/valorisation');
  console.log('  GET /api/mouvements/articles[?dateDebut=&dateFin=&site=]');
  console.log('  GET /api/mouvements/reglements[?dateDebut=&dateFin=]');
  console.log('  GET /api/mouvements/synthese[?dateDebut=&dateFin=&site=]');
  console.log('  GET /api/performance/ca[?dateDebut=&dateFin=&site=&groupBy=jour|mois]');
  console.log('  GET /api/performance/hitparade[?dateDebut=&dateFin=&site=&limit=&groupBy=qte|ca|marge]');
  console.log('  GET /api/performance/ca/nomenclature[?dateDebut=&dateFin=&site=&niveau=]');
  console.log('  GET /api/performance/ca/gamme[?dateDebut=&dateFin=&site=]');
  console.log('  GET /api/sync/status');
});
