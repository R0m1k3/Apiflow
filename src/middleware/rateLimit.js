// Limitation de débit en mémoire, fenêtre fixe d'une minute par IP client.
// RATE_LIMIT_PER_MIN (défaut 600). L'IP vient de req.ip (dépend de 'trust proxy').

const WINDOW_MS = 60_000;
const LIMIT = Math.max(1, parseInt(process.env.RATE_LIMIT_PER_MIN, 10) || 600);

const hits = new Map(); // ip -> { start, count }

// Purge périodique des fenêtres expirées
const purgeTimer = setInterval(() => {
  const now = Date.now();
  for (const [ip, w] of hits) {
    if (now - w.start >= WINDOW_MS) hits.delete(ip);
  }
}, WINDOW_MS);
purgeTimer.unref();

function rateLimit(req, res, next) {
  // Le healthcheck Docker ne doit jamais être limité
  if (req.path === '/api/health') return next();

  const ip = req.ip || req.socket.remoteAddress || 'inconnu';
  const now = Date.now();
  let w = hits.get(ip);
  if (!w || now - w.start >= WINDOW_MS) {
    w = { start: now, count: 0 };
    hits.set(ip, w);
  }
  w.count++;

  res.set('X-RateLimit-Limit', String(LIMIT));
  res.set('X-RateLimit-Remaining', String(Math.max(0, LIMIT - w.count)));

  if (w.count > LIMIT) {
    const retry = Math.ceil((w.start + WINDOW_MS - now) / 1000);
    res.set('Retry-After', String(retry));
    return res.status(429).json({ error: 'Trop de requêtes, réessayez plus tard' });
  }
  return next();
}

function stopRateLimit() {
  clearInterval(purgeTimer);
}

module.exports = { rateLimit, stopRateLimit, RATE_LIMIT: LIMIT };
