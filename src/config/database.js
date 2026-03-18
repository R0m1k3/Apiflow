require('dotenv').config();
const { Pool } = require('pg');

let pool = null;

function getPool() {
  if (!pool) {
    pool = new Pool({
      host:     process.env.PG_HOST || 'postgres',
      port:     parseInt(process.env.PG_PORT) || 5432,
      database: process.env.PG_DB || 'foirfouille',
      user:     process.env.PG_USER || 'ff_api',
      password: process.env.PG_PASSWORD,
      max: 10,
      idleTimeoutMillis: 30000,
    });
    pool.on('error', (err) => {
      console.error('PostgreSQL pool error:', err.message);
    });
  }
  return pool;
}

module.exports = { getPool };
