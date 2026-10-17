// Gestion centralisée des erreurs.
// Express 5 transmet ici les rejets des handlers async : les routes n'ont plus
// besoin de try/catch. Le détail technique n'est JAMAIS renvoyé au client.

const { sanitizeUrl } = require('./logger');

function notFound(req, res) {
  res.status(404).json({ error: 'Route introuvable' });
}

// eslint-disable-next-line no-unused-vars
function errorHandler(err, req, res, next) {
  // Corps JSON invalide / trop gros (body-parser)
  if (err && err.type === 'entity.parse.failed') {
    return res.status(400).json({ error: 'Corps JSON invalide' });
  }
  if (err && err.type === 'entity.too.large') {
    return res.status(413).json({ error: 'Corps de requête trop volumineux' });
  }

  // Erreurs maîtrisées (validation 400, 404, 409, 502…)
  if (err && err.expose && err.status >= 400 && err.status < 600) {
    if (err.status >= 500) logError(req, err);
    if (res.headersSent) return res.destroy();
    return res.status(err.status).json({ error: err.message });
  }

  // Erreurs PostgreSQL « attendues »
  if (err && err.code === '22P02') { // invalid_text_representation (ex. identifiant non numérique)
    if (!res.headersSent) return res.status(400).json({ error: 'Paramètre invalide' });
  }
  if (err && err.code === '57014') { // statement_timeout
    logError(req, err);
    if (!res.headersSent) return res.status(503).json({ error: 'Requête trop longue, réduisez la période ou les filtres' });
  }

  logError(req, err);
  if (res.headersSent) return res.destroy();
  return res.status(500).json({ error: 'Erreur interne' });
}

function logError(req, err) {
  const cause = err && err.cause ? ` | cause: ${err.cause.message || err.cause}` : '';
  console.error(
    `[ERREUR] ${req.method} ${sanitizeUrl(req.originalUrl)} : ${err && err.message}${cause}\n${err && err.stack}`
  );
}

module.exports = { errorHandler, notFound };
