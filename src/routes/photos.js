const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');

const FF_SITEMAPS = [
  'https://www.lafoirfouille.fr/sitemapProducts_0.xml',
  'https://www.lafoirfouille.fr/sitemapProducts_1.xml',
  'https://www.lafoirfouille.fr/sitemapProducts_2.xml',
];
const FF_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
// Code produit lafoirfouille.fr : 9-13 chiffres en fin d'URL
const CODE_RE = /\/p\/[^/]+-(\d{9,13})\.html/g;

let indexingInProgress = false;

async function indexSitemaps(pool) {
  let total = 0;
  for (const sitemapUrl of FF_SITEMAPS) {
    const res = await fetch(sitemapUrl, { headers: { 'User-Agent': FF_UA } });
    if (!res.ok) continue;
    const xml = await res.text();

    // Extrait toutes les URLs de produits
    const urlMatches = [...xml.matchAll(/<loc>(https[^<]+)<\/loc>/g)];
    const rows = [];
    for (const [, url] of urlMatches) {
      const codeMatch = url.match(/\/p\/[^/]+-(\d{9,13})\.html/);
      if (codeMatch) rows.push({ code: codeMatch[1], url });
    }

    if (!rows.length) continue;

    // Upsert par batch de 500
    for (let i = 0; i < rows.length; i += 500) {
      const batch = rows.slice(i, i + 500);
      const values = batch.map((r, j) => `($${j * 2 + 1}, $${j * 2 + 2})`).join(', ');
      const params = batch.flatMap(r => [r.code, r.url]);
      await pool.query(
        `INSERT INTO ff_photo_index (photo_code, product_url)
         VALUES ${values}
         ON CONFLICT (photo_code) DO UPDATE
           SET product_url = EXCLUDED.product_url, indexed_at = NOW()`,
        params
      );
    }
    total += rows.length;
  }
  return total;
}

// POST /api/photos/reindex — déclenche l'indexation des sitemaps
router.post('/reindex', async (req, res) => {
  if (indexingInProgress) return res.status(409).json({ error: 'Indexation déjà en cours' });

  const pool = getPool();
  indexingInProgress = true;
  const start = Date.now();

  try {
    const total = await indexSitemaps(pool);
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    res.json({ indexed: total, elapsed_s: parseFloat(elapsed) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  } finally {
    indexingInProgress = false;
  }
});

// GET /api/photos/status
router.get('/status', async (req, res) => {
  try {
    const pool = getPool();
    const result = await pool.query(
      `SELECT COUNT(*) AS total, MAX(indexed_at) AS last_index FROM ff_photo_index`
    );
    res.json({
      total: parseInt(result.rows[0].total),
      last_index: result.rows[0].last_index,
      indexing_in_progress: indexingInProgress,
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
