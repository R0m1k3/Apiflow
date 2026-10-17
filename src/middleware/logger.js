// Journal d'accès : une ligne par requête (méthode, URL, statut, durée, IP).
// Les éventuels paramètres sensibles de la query string sont masqués.

const SENSITIVE = /^(api[_-]?key|apikey|key|token|access_token)$/i;

function sanitizeUrl(originalUrl) {
  const i = originalUrl.indexOf('?');
  if (i < 0) return originalUrl;
  const params = new URLSearchParams(originalUrl.slice(i + 1));
  for (const k of [...params.keys()]) {
    if (SENSITIVE.test(k)) params.set(k, '***');
  }
  const qs = params.toString();
  return originalUrl.slice(0, i) + (qs ? `?${qs}` : '');
}

function requestLogger(req, res, next) {
  const start = process.hrtime.bigint();
  let logged = false;
  const log = () => {
    if (logged) return;
    logged = true;
    const ms = Number(process.hrtime.bigint() - start) / 1e6;
    const status = res.writableFinished ? res.statusCode : `${res.statusCode}(interrompu)`;
    console.log(`${req.method} ${sanitizeUrl(req.originalUrl)} ${status} ${ms.toFixed(0)}ms ${req.ip || ''}`);
  };
  res.on('finish', log);
  res.on('close', log);
  next();
}

module.exports = { requestLogger, sanitizeUrl };
