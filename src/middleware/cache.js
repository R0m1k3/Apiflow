// Cache des réponses JSON des requêtes GET.
//  - Cache-Control: private, max-age=300 sur les réponses 200 (données rafraîchies chaque nuit)
//  - cache mémoire : TTL CACHE_TTL_SEC (défaut 300, 0 = désactivé), 500 entrées max,
//    taille totale bornée ; clé = URL (sans nocache) + empreinte de la clé d'API
//  - ?nocache=1 : ignore le cache en lecture et rafraîchit l'entrée

const TTL_MS = Math.max(0, parseInt(process.env.CACHE_TTL_SEC ?? '300', 10) || 0) * 1000;
const MAX_ENTRIES = 500;
const MAX_ENTRY_BYTES = 2 * 1024 * 1024;   // une réponse > 2 Mo n'est pas mise en cache
const MAX_TOTAL_BYTES = 64 * 1024 * 1024;  // 64 Mo au total
const BROWSER_MAX_AGE = 300;

// Routes jamais mises en cache (état temps réel)
const NO_CACHE = [/^\/api\/health\/?$/, /^\/api\/photos\/status\/?$/];

const store = new Map(); // key -> { body, expires, size }
let totalBytes = 0;

function evict(key) {
  const e = store.get(key);
  if (e) {
    totalBytes -= e.size;
    store.delete(key);
  }
}

function put(key, body) {
  const size = Buffer.byteLength(body);
  if (size > MAX_ENTRY_BYTES) return;
  evict(key);
  store.set(key, { body, size, expires: Date.now() + TTL_MS });
  totalBytes += size;
  // Éviction des plus anciennes entrées (ordre d'insertion)
  while (store.size > MAX_ENTRIES || totalBytes > MAX_TOTAL_BYTES) {
    evict(store.keys().next().value);
  }
}

function get(key) {
  const e = store.get(key);
  if (!e) return null;
  if (e.expires <= Date.now()) {
    evict(key);
    return null;
  }
  return e.body;
}

// Retire le paramètre nocache de l'URL pour construire la clé
function cacheKeyUrl(originalUrl) {
  const i = originalUrl.indexOf('?');
  if (i < 0) return originalUrl;
  const params = new URLSearchParams(originalUrl.slice(i + 1));
  params.delete('nocache');
  const qs = params.toString();
  return originalUrl.slice(0, i) + (qs ? `?${qs}` : '');
}

function sendJson(res, body) {
  res.set('Content-Type', 'application/json; charset=utf-8');
  return res.send(body);
}

function responseCache(req, res, next) {
  if (req.method !== 'GET' || NO_CACHE.some(re => re.test(req.path))) {
    return next();
  }

  const key = `${req.apiKeyHash || 'anon'}|${cacheKeyUrl(req.originalUrl)}`;
  const bypass = req.query.nocache === '1';

  if (TTL_MS > 0 && !bypass) {
    const hit = get(key);
    if (hit) {
      res.set('X-Cache', 'HIT');
      res.set('Cache-Control', `private, max-age=${BROWSER_MAX_AGE}`);
      return sendJson(res, hit);
    }
  }

  // Intercepte res.json pour poser Cache-Control et mémoriser la réponse
  const originalJson = res.json.bind(res);
  res.json = (data) => {
    if (res.statusCode !== 200) return originalJson(data);
    const body = JSON.stringify(data);
    res.set('Cache-Control', `private, max-age=${BROWSER_MAX_AGE}`);
    if (TTL_MS > 0) {
      res.set('X-Cache', 'MISS');
      if (body !== undefined) put(key, body);
    }
    return sendJson(res, body);
  };
  return next();
}

module.exports = { responseCache };
