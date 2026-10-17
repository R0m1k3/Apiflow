// Photos produits lafoirfouille.fr : résolution de l'URL CDN signée + proxy en streaming.

const { Readable } = require('stream');
const { pipeline } = require('stream/promises');
const { HttpError } = require('../utils/errors');

const FF_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';
const FETCH_TIMEOUT_MS = 8000;
const PHOTO_CACHE_MAX = 5000;
const PHOTO_CACHE_TTL_MS = 3600_000;
const ALLOWED_HOSTS = new Set(['lafoirfouille.fr', 'www.lafoirfouille.fr']);
const PHOTO_SIZES = { small: '300Wx300H', large: '1200Wx1200H' };

// Cache LRU simple : Map conserve l'ordre d'insertion → on réinsère à chaque accès
const photoCache = new Map();

function cacheGet(key) {
  const entry = photoCache.get(key);
  if (!entry) return null;
  photoCache.delete(key);
  if (entry.expires <= Date.now()) return null;
  photoCache.set(key, entry);
  return entry.imageUrl;
}

function cacheSet(key, imageUrl) {
  photoCache.delete(key);
  photoCache.set(key, { imageUrl, expires: Date.now() + PHOTO_CACHE_TTL_MS });
  while (photoCache.size > PHOTO_CACHE_MAX) {
    photoCache.delete(photoCache.keys().next().value);
  }
}

// Code photo (nom de fichier sans extension) ou null
function photoCode(nomphoto) {
  return nomphoto ? String(nomphoto).replace(/\.[^.]+$/, '') : null;
}

// Champs photo_url / photo_url_large exposés dans les réponses articles
function photoUrls(noId, nomphoto) {
  const code = photoCode(nomphoto);
  return {
    photo_url:       code ? `/api/articles/${noId}/photo` : null,
    photo_url_large: code ? `/api/articles/${noId}/photo?size=large` : null,
  };
}

// N'autorise que https://(www.)lafoirfouille.fr (protection SSRF sur product_url)
function isAllowedProductUrl(url) {
  try {
    const u = new URL(url);
    return u.protocol === 'https:' && ALLOWED_HOSTS.has(u.hostname) && !u.port && !u.username;
  } catch {
    return false;
  }
}

// fetch avec timeout ; toute erreur réseau / timeout → 502
async function fetchUpstream(url, headers, timeoutMs = FETCH_TIMEOUT_MS) {
  try {
    return await fetch(url, { headers, signal: AbortSignal.timeout(timeoutMs), redirect: 'follow' });
  } catch (err) {
    const e = new HttpError(502, 'Source photo indisponible');
    e.cause = err;
    throw e;
  }
}

// Résout l'URL signée de l'image. Renvoie null si introuvable (→ 404 côté route).
async function resolvePhotoUrl(pool, code, size) {
  const cacheKey = `${code}:${size}`;
  const cached = cacheGet(cacheKey);
  if (cached) return cached;

  // 1. URL produit dans l'index sitemap
  const indexRow = await pool.query(
    'SELECT product_url FROM ff_photo_index WHERE photo_code = $1',
    [code]
  );
  if (!indexRow.rows.length) return null;

  const productPageUrl = indexRow.rows[0].product_url;
  if (!isAllowedProductUrl(productPageUrl)) {
    console.warn(`[photos] product_url refusée (hôte non autorisé) pour ${code}`);
    return null;
  }

  // 2. Page produit → URL CDN signée
  const productRes = await fetchUpstream(productPageUrl, {
    'User-Agent': FF_UA, 'Accept-Language': 'fr-FR,fr;q=0.9',
  });
  if (productRes.status === 404 || productRes.status === 410) return null;
  if (!productRes.ok) throw new HttpError(502, 'Source photo indisponible');

  let productHtml;
  try {
    productHtml = await productRes.text();
  } catch (err) {
    const e = new HttpError(502, 'Source photo indisponible');
    e.cause = err;
    throw e;
  }

  // Motif : https://prod-api.lafoirfouille.fr/medias/{code}-0-{size}?context=...
  const imgPattern = new RegExp(
    `https://prod-api\\.lafoirfouille\\.fr/medias/${code}-0-${size}\\?context=[A-Za-z0-9+/=_%-]+`
  );
  const imgMatch = productHtml.match(imgPattern);
  if (!imgMatch) return null;

  const imageUrl = imgMatch[0].replace(/&amp;/g, '&');
  cacheSet(cacheKey, imageUrl);
  return imageUrl;
}

// Proxy de l'image vers la réponse HTTP, en streaming.
// Aucun res.status() après l'envoi des en-têtes.
async function proxyImage(res, imageUrl) {
  const imgRes = await fetchUpstream(imageUrl, {
    Referer: 'https://www.lafoirfouille.fr/', 'User-Agent': FF_UA,
  });
  if (imgRes.status === 404 || imgRes.status === 410) {
    throw new HttpError(404, 'Photo non trouvée sur lafoirfouille.fr');
  }
  if (!imgRes.ok || !imgRes.body) throw new HttpError(502, 'Source photo indisponible');

  const contentType = imgRes.headers.get('content-type') || 'image/jpeg';
  if (!/^image\//i.test(contentType)) throw new HttpError(502, 'Réponse photo inattendue');

  res.setHeader('Content-Type', contentType);
  const len = imgRes.headers.get('content-length');
  if (len && /^\d+$/.test(len)) res.setHeader('Content-Length', len);
  res.setHeader('Cache-Control', 'public, max-age=3600');

  try {
    await pipeline(Readable.fromWeb(imgRes.body), res);
  } catch (err) {
    // Client parti ou flux amont interrompu : en-têtes déjà envoyés,
    // pipeline a détruit la réponse — on journalise sans renvoyer de statut.
    if (err.code !== 'ERR_STREAM_PREMATURE_CLOSE') {
      console.warn(`[photos] flux interrompu : ${err.message}`);
    }
  }
}

module.exports = {
  FF_UA, PHOTO_SIZES, photoCode, photoUrls, isAllowedProductUrl,
  resolvePhotoUrl, proxyImage,
};
