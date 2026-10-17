// Authentification par clé d'API.
//   - en-tête  X-API-Key: <clé>   ou   Authorization: Bearer <clé>
//   - clés autorisées : variable API_KEYS (séparées par des virgules)
//   - comparaison en temps constant (hash SHA-256 + timingSafeEqual)
// Si API_KEYS est vide : accès libre, avec avertissement au démarrage puis toutes les heures.

const crypto = require('crypto');

const sha256 = (s) => crypto.createHash('sha256').update(s, 'utf8').digest();

const KEYS = (process.env.API_KEYS || '')
  .split(',')
  .map(k => k.trim())
  .filter(Boolean);
const KEY_HASHES = KEYS.map(sha256);

const authEnabled = KEY_HASHES.length > 0;

// Routes publiques : santé + photo (utilisée directement dans des <img>)
const PUBLIC_ROUTES = [
  { methods: ['GET', 'HEAD'], re: /^\/api\/health\/?$/ },
  { methods: ['GET', 'HEAD'], re: /^\/api\/articles\/[^/]+\/photo\/?$/ },
];

function isPublic(req) {
  return PUBLIC_ROUTES.some(r => r.methods.includes(req.method) && r.re.test(req.path));
}

function extractKey(req) {
  const xKey = req.get('x-api-key');
  if (xKey) return xKey.trim();
  const authz = req.get('authorization');
  if (authz) {
    const m = /^Bearer\s+(.+)$/i.exec(authz.trim());
    if (m) return m[1].trim();
  }
  return null;
}

// Vrai si la clé correspond à l'une des clés configurées (sans court-circuit)
function isValidKey(key) {
  const h = sha256(key);
  let ok = false;
  for (const kh of KEY_HASHES) {
    if (crypto.timingSafeEqual(h, kh)) ok = true;
  }
  return ok;
}

function warnDisabled() {
  console.warn('\n' + '!'.repeat(72));
  console.warn('!! ATTENTION : API_KEYS non définie — AUTHENTIFICATION DÉSACTIVÉE.');
  console.warn("!! Toute personne connaissant l'URL publique peut lire les données.");
  console.warn('!! Définir API_KEYS=<clé1>,<clé2> dans l\'environnement du conteneur.');
  console.warn('!'.repeat(72) + '\n');
}

let warnTimer = null;
function startAuthWarnings() {
  if (authEnabled) {
    console.log(`Authentification par clé d'API activée (${KEY_HASHES.length} clé(s)).`);
    return;
  }
  warnDisabled();
  warnTimer = setInterval(warnDisabled, 3600_000);
  warnTimer.unref();
}

function stopAuthWarnings() {
  if (warnTimer) clearInterval(warnTimer);
}

function authMiddleware(req, res, next) {
  const key = extractKey(req);
  // Empreinte courte de la clé (utilisée pour la clé du cache de réponses)
  req.apiKeyHash = key ? sha256(key).toString('hex').slice(0, 16) : 'anon';

  if (!authEnabled || isPublic(req)) return next();

  if (!key || !isValidKey(key)) {
    res.set('WWW-Authenticate', 'Bearer realm="api"');
    return res.status(401).json({ error: key ? "Clé d'API invalide" : "Clé d'API requise (en-tête X-API-Key)" });
  }
  return next();
}

module.exports = { authMiddleware, authEnabled, startAuthWarnings, stopAuthWarnings, isPublic };
