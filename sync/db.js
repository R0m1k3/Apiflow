require('dotenv').config();
const sql = require('mssql');
const { Pool } = require('pg');

const mssqlConfig = {
  server:   process.env.DB_HOST,
  port:     parseInt(process.env.DB_PORT) || 1433,
  database: process.env.DB_NAME,
  user:     process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  requestTimeout: 300000,   // 5 min pour les grosses requêtes (MvtArt)
  cancelTimeout:  10000,    // 10s pour annuler une requête
  options: {
    encrypt: false,
    trustServerCertificate: true,
    readOnlyIntent: true,
  },
  pool: { max: 5, min: 0, idleTimeoutMillis: 30000 },
};

let mssqlPool = null;
let pgPool = null;

async function getMssql() {
  if (!mssqlPool) {
    mssqlPool = await sql.connect(mssqlConfig);
  }
  return mssqlPool;
}

function getPg() {
  if (!pgPool) {
    pgPool = new Pool({
      host:     process.env.PG_HOST || 'postgres',
      port:     parseInt(process.env.PG_PORT) || 5432,
      database: process.env.PG_DB,
      user:     process.env.PG_USER,
      password: process.env.PG_PASSWORD,
      max: 5,
    });
  }
  return pgPool;
}

async function closeAll() {
  if (mssqlPool) { await mssqlPool.close(); mssqlPool = null; }
  if (pgPool)    { await pgPool.end();      pgPool = null; }
}

module.exports = { getMssql, getPg, closeAll, sql };
