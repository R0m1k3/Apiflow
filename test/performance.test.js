// Tests d'intégration des routes /api/performance/* sur une base PostgreSQL de TEST.
//
// Lancer : TEST_PG_HOST=127.0.0.1 TEST_PG_PORT=5432 TEST_PG_USER=postgres TEST_PG_DB=foirfouille_test npm test
// La base doit avoir le schéma (postgres/migrations/*.sql puis postgres/init.sql) et son nom doit
// finir par « _test » : test/fixtures.sql VIDE les tables mvtart, articles, artfou1, fouident.
// Sans TEST_PG_HOST, les tests sont ignorés.
'use strict';
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const path = require('node:path');
const { Client } = require('pg');

const PG = {
  host: process.env.TEST_PG_HOST,
  port: Number(process.env.TEST_PG_PORT || 5432),
  user: process.env.TEST_PG_USER || 'postgres',
  password: process.env.TEST_PG_PASSWORD,
  database: process.env.TEST_PG_DB || 'foirfouille_test',
};
const skip = !PG.host && 'TEST_PG_HOST non défini';
const PORT = 3999;
const KEY = 'cle-de-test';
let api;

async function get(url) {
  const r = await fetch(`http://127.0.0.1:${PORT}/api${url}`, { headers: { 'X-API-Key': KEY } });
  const body = await r.json();
  assert.equal(r.status, 200, JSON.stringify(body));
  return body;
}
const n = (v) => Number(v);
const PERIODE = 'site=292&dateDebut=2026-01-01&dateFin=2026-03-31';

before(async () => {
  if (skip) return;
  // Garde-fou : jamais sur une base qui pourrait être la production
  if (!PG.database.endsWith('_test')) throw new Error(`TEST_PG_DB doit finir par _test (reçu : ${PG.database})`);
  const c = new Client(PG);
  await c.connect();
  await c.query(fs.readFileSync(path.join(__dirname, 'fixtures.sql'), 'utf8'));
  await c.end();
  api = spawn(process.execPath, [path.join(__dirname, '..', 'src', 'app.js')], {
    env: {
      ...process.env, API_PORT: String(PORT), API_KEYS: KEY,
      PG_HOST: PG.host, PG_PORT: String(PG.port), PG_USER: PG.user, PG_PASSWORD: PG.password || '', PG_DB: PG.database,
    },
    stdio: ['ignore', 'ignore', 'inherit'],
  });
  // Attente de l'API : on interroge /api/health (pas de dépendance au texte des logs), 15 s max
  api.on('exit', (code) => { if (code) console.error(`API arrêtée (code ${code})`); });
  for (let i = 0; ; i++) {
    try { if ((await fetch(`http://127.0.0.1:${PORT}/api/health`)).ok) break; } catch { /* pas encore prête */ }
    if (i >= 150 || api.exitCode !== null) throw new Error('API non démarrée');
    await new Promise((r) => setTimeout(r, 100));
  }
});

after(() => { if (api) api.kill(); });

test('/ca : CA net des retours, détail brut / retours, taux sur CA HT net', { skip }, async () => {
  const { ca } = await get(`/performance/ca?${PERIODE}&groupBy=mois`);
  const tot = (k) => ca.reduce((s, r) => s + n(r[k]), 0);
  // ventes 10 + 5 + 5 + 20 = 40 HT ; retours 5 (type 12) + 5 (sans type) = 10 HT (montants positifs)
  // démarque (genremvt 9), réception, autre magasin et hors période exclus
  assert.equal(tot('ca_ht_brut'), 40);
  assert.equal(tot('retours_ht'), 10);
  assert.equal(tot('ca_ht'), 30);
  assert.equal(tot('ca_ttc_brut'), 48);
  assert.equal(tot('retours_ttc'), 12);
  assert.equal(tot('ca_ttc'), 36);
  assert.equal(tot('qte_vendue'), 6);
  assert.equal(tot('marge'), 14);
  const mars = ca.find((r) => r.periode === '2026-03');
  // mars : 5 − 5 + 5 (31/03 à 18 h 30, inclus) − 5 = 0 HT net → taux 0 par convention
  assert.equal(n(mars.ca_ht), 0);
  assert.equal(n(mars.taux_marge), 0);
  const fev = ca.find((r) => r.periode === '2026-02');
  assert.equal(n(fev.taux_marge), 46.67); // 14 / 30
});

test('/ca/fournisseur : fournisseur préféré, CA net, même total que /ca', { skip }, async () => {
  const { ca_par_fournisseur: rows } = await get(`/performance/ca/fournisseur?${PERIODE}`);
  const by = Object.fromEntries(rows.map((r) => [r.code_fournisseur, r]));
  assert.equal(n(by.L027.ca_ht), 10);
  assert.equal(n(by.L027.taux_marge), 40);
  assert.equal(n(by.S028.ca_ht), 20);
  assert.equal(rows.reduce((s, r) => s + n(r.ca_ht), 0), 30);
});

test('/hitparade : CA net par article', { skip }, async () => {
  const { hitparade } = await get(`/performance/hitparade?${PERIODE}`);
  const a1 = hitparade.find((r) => r.codein === 'A1');
  assert.equal(n(a1.ca_ht), 10);
  assert.equal(n(a1.qte_vendue), 2);
  assert.equal(n(a1.taux_marge), 40);
});

test('/ca : plus de retours que de ventes → CA négatif, taux 0 (et non −40 %)', { skip }, async () => {
  const { ca } = await get('/performance/ca?site=700&dateDebut=2026-03-01&dateFin=2026-03-31&groupBy=mois');
  assert.equal(n(ca[0].ca_ht), -5);
  assert.equal(n(ca[0].ca_ht_brut), 0);
  assert.equal(n(ca[0].retours_ht), 5);
  assert.equal(n(ca[0].taux_marge), 0);
});

test('/dashboard : retour déduit du CA des tops, article sans vente nette absent', { skip }, async () => {
  const d = await get('/performance/dashboard?date=2026-02-15&site=700');
  const a3 = d.top10_ca.find((r) => r.codein === 'A3');
  assert.equal(n(a3.ca_ht), 5);   // 10 vendus − 5 rapportés
  assert.equal(n(a3.ca_ttc), 6);
  assert.equal(d.top10_ca.find((r) => r.codein === 'A4'), undefined);
});

test('/dashboard : tops avec CA HT et CA TTC nets', { skip }, async () => {
  const d = await get('/performance/dashboard?date=2026-02-15&site=292');
  const a2 = d.top10_ca.find((r) => r.codein === 'A2');
  assert.equal(n(a2.ca_ht), 20);
  assert.equal(n(a2.ca_ttc), 24);
});
