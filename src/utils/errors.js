// Erreurs applicatives : le gestionnaire global (middleware/errorHandler.js)
// les traduit en réponse JSON avec le bon code HTTP.

// Erreur HTTP « maîtrisée » : le message est destiné au client.
class HttpError extends Error {
  constructor(status, message) {
    super(message);
    this.name = 'HttpError';
    this.status = status;
    this.expose = true;
  }
}

// Paramètre de requête invalide → 400
class ValidationError extends HttpError {
  constructor(message) {
    super(400, message);
    this.name = 'ValidationError';
  }
}

module.exports = { HttpError, ValidationError };
