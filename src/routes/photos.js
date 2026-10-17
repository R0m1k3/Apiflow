const express = require('express');
const router = express.Router();
const { getPool } = require('../config/database');
const { HttpError } = require('../utils/errors');
const { FF_UA, isAllowedProductUrl } = require('../services/photos');

const FF_SITEMAPS = [
  'https://www.lafoirfouille.fr/sitemapProducts_0.xml',
  'https://www.lafoirfouille.fr/sitemapProducts_1.xml',
  'https://www.lafoirfouille.fr/sitemapProducts_2.xml',
];
// Les sitemaps pèsent plusieurs Mo : délai plus large que le proxy photo (8 s)
const SITEMAP_TIMEOUT_MS = 60000;
// Code produit lafoirfouille.fr : 9-13 chiffres en fin d'URL
const CODE_RE = /\/p\/[^/]+-(\d{9,13})\.html/;

let indexingInProgress = false;

async function indexSitemaps(pool) {
  let total = 0;
  for (const sitemapUrl of FF_SITEMAPS) {
    let xml;
    try {
      const res = await fetch(sitemapUrl, {
        headers: { 'User-Agent': FF_UA },
        signal: AbortSignal.timeout(SITEMAP_TIMEOUT_MS),
      });
      if (!res.ok) {
        console.warn(`[photos] sitemap ${sitemapUrl} : HTTP ${res.status}, ignoré`);
        continue;
      }
      xml = await res.text();
    } catch (err) {
      console.warn(`[photos] sitemap ${sitemapUrl} inaccessible : ${err.message}`);
      continue;
    }

    // Extrait les URLs produits (hôte lafoirfouille.fr uniquement)
    const rows = [];
    for (const [, rawUrl] of xml.matchAll(/<loc>(https[^<]+)<\/loc>/g)) {
      const url = rawUrl.trim().replace(/&amp;/g, '&');
      if (!isAllowedProductUrl(url)) continue;
      const codeMatch = url.match(CODE_RE);
      if (codeMatch) rows.push({ code: codeMatch[1], url });
    }
    if (!rows.length) continue;

    // Upsert par lots de 500
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

// POST /api/photos/reindex — déclenche l'indexation des sitemaps (authentifié, une seule à la fois)
router.post('/reindex', async (req, res) => {
  if (indexingInProgress) throw new HttpError(409, 'Indexation déjà en cours');

  indexingInProgress = true;
  const start = Date.now();
  try {
    const total = await indexSitemaps(getPool());
    const elapsed = ((Date.now() - start) / 1000).toFixed(1);
    res.json({ indexed: total, elapsed_s: parseFloat(elapsed) });
  } finally {
    indexingInProgress = false;
  }
});

// GET /api/photos/status
router.get('/status', async (req, res) => {
  const result = await getPool().query(
    'SELECT COUNT(*) AS total, MAX(indexed_at) AS last_index FROM ff_photo_index'
  );
  res.json({
    total: parseInt(result.rows[0].total, 10),
    last_index: result.rows[0].last_index,
    indexing_in_progress: indexingInProgress,
  });
});

module.exports = router;
