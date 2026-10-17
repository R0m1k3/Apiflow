// En-têtes de sécurité + CORS (sans dépendance).

// En-têtes appliqués à toutes les réponses (JSON comme images)
function securityHeaders(req, res, next) {
  res.set({
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'Referrer-Policy': 'no-referrer',
    'Content-Security-Policy': "default-src 'none'; frame-ancestors 'none'",
  });
  next();
}

// CORS : CORS_ORIGINS = liste d'origines séparées par des virgules ('*' = toutes).
// Non défini → aucun en-tête CORS (comportement historique).
const CORS_ORIGINS = (process.env.CORS_ORIGINS || '')
  .split(',')
  .map(o => o.trim().replace(/\/+$/, ''))
  .filter(Boolean);
const corsEnabled = CORS_ORIGINS.length > 0;
const corsAny = CORS_ORIGINS.includes('*');

function cors(req, res, next) {
  if (!corsEnabled) return next();

  res.vary('Origin');
  const origin = req.get('origin');
  const allowed = origin && (corsAny || CORS_ORIGINS.includes(origin));
  if (!allowed) return next();

  res.set('Access-Control-Allow-Origin', origin);
  res.set('Access-Control-Expose-Headers', 'X-Cache, Retry-After');

  // Pré-vol : répondu ici, avant l'authentification
  if (req.method === 'OPTIONS' && req.get('access-control-request-method')) {
    res.set({
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'X-API-Key, Authorization, Content-Type',
      'Access-Control-Max-Age': '600',
    });
    return res.status(204).end();
  }
  return next();
}

module.exports = { securityHeaders, cors };
