require('dotenv').config({ quiet: true });
const { Pool } = require('pg');

let pool = null;

// Délai max d'exécution d'une requête SQL (ms) — protège la base des requêtes trop lourdes
const STATEMENT_TIMEOUT_MS = Math.max(1000, parseInt(process.env.PG_STATEMENT_TIMEOUT_MS, 10) || 30000);

function getPool() {
  if (!pool) {
    pool = new Pool({
      host:     process.env.PG_HOST || 'postgres',
      port:     parseInt(process.env.PG_PORT, 10) || 5432,
      database: process.env.PG_DB || 'foirfouille',
      user:     process.env.PG_USER || 'ff_api',
      password: process.env.PG_PASSWORD,
      max: Math.max(1, parseInt(process.env.PG_POOL_MAX, 10) || 10),
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
      application_name: 'ff_api',
      options: `-c statement_timeout=${STATEMENT_TIMEOUT_MS}`,
    });
    // Erreur sur un client inactif (ex. redémarrage PostgreSQL) : ne doit pas tuer le process
    pool.on('error', (err) => {
      console.error('Erreur pool PostgreSQL (client inactif) :', err.message);
    });
  }
  return pool;
}

// Fermeture propre (arrêt du serveur)
async function closePool() {
  if (pool) {
    const p = pool;
    pool = null;
    await p.end();
  }
}

module.exports = { getPool, closePool };
