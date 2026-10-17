// Validation des paramètres d'entrée (query string / path), sans dépendance.
// Chaque helper lève une ValidationError (→ HTTP 400) plutôt que de laisser
// passer NaN, une date invalide ou un tableau jusqu'au SQL.

const { ValidationError } = require('./errors');
const { diffDays, toUtcMs } = require('./dates');

const DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;

function isAbsent(v) {
  return v === undefined || v === null || v === '';
}

// Refuse les valeurs multiples (?site=a&site=b) ou objets
function scalar(v, name) {
  if (v !== undefined && v !== null && typeof v === 'object') {
    throw new ValidationError(`Paramètre '${name}' invalide (valeur multiple non supportée)`);
  }
  return v;
}

// Texte libre (recherche, code…) — longueur bornée
function parseText(v, name, { def = '', max = 200, trim = true } = {}) {
  scalar(v, name);
  if (isAbsent(v)) return def;
  let s = String(v);
  if (trim) s = s.trim();
  if (s.length > max) throw new ValidationError(`Paramètre '${name}' trop long (max ${max} caractères)`);
  // Pas de caractères de contrôle
  if (/[\u0000-\u001f\u007f]/.test(s)) throw new ValidationError(`Paramètre '${name}' invalide`);
  return s;
}

// Entier >= min (et <= max). clamp=true ramène dans les bornes au lieu de refuser.
function parsePositiveInt(v, name, { def, min = 1, max = Number.MAX_SAFE_INTEGER, clamp = false } = {}) {
  scalar(v, name);
  if (isAbsent(v)) {
    if (def === undefined) throw new ValidationError(`Paramètre '${name}' requis`);
    return def;
  }
  const s = String(v).trim();
  if (!/^\d{1,15}$/.test(s)) throw new ValidationError(`Paramètre '${name}' : entier attendu`);
  let n = Number(s);
  if (n < min) {
    if (!clamp) throw new ValidationError(`Paramètre '${name}' : minimum ${min}`);
    n = min;
  }
  if (n > max) {
    if (!clamp) throw new ValidationError(`Paramètre '${name}' : maximum ${max}`);
    n = max;
  }
  return n;
}

// Date stricte YYYY-MM-DD (date calendaire valide, années 1900-2100)
function parseDate(v, name, { def } = {}) {
  scalar(v, name);
  if (isAbsent(v)) {
    if (def === undefined) throw new ValidationError(`Paramètre '${name}' requis (YYYY-MM-DD)`);
    return def;
  }
  const s = String(v).trim();
  const m = DATE_RE.exec(s);
  if (!m) throw new ValidationError(`Paramètre '${name}' : date attendue au format YYYY-MM-DD`);
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  if (y < 1900 || y > 2100) throw new ValidationError(`Paramètre '${name}' : année hors limites (1900-2100)`);
  const dt = new Date(Date.UTC(y, mo - 1, d));
  if (dt.getUTCFullYear() !== y || dt.getUTCMonth() !== mo - 1 || dt.getUTCDate() !== d) {
    throw new ValidationError(`Paramètre '${name}' : date inexistante`);
  }
  return s;
}

// Valeur parmi une liste autorisée
function parseEnum(v, name, allowed, { def } = {}) {
  scalar(v, name);
  if (isAbsent(v)) {
    if (def === undefined) throw new ValidationError(`Paramètre '${name}' requis`);
    return def;
  }
  const s = String(v).trim();
  if (!allowed.includes(s)) {
    throw new ValidationError(`Paramètre '${name}' invalide (valeurs possibles : ${allowed.join(', ')})`);
  }
  return s;
}

// Identifiant numérique (BIGINT) — renvoyé en chaîne pour ne pas perdre de précision
function parseId(v, name = 'id') {
  scalar(v, name);
  const s = isAbsent(v) ? '' : String(v).trim();
  if (!/^\d{1,18}$/.test(s)) throw new ValidationError(`Identifiant '${name}' invalide`);
  return s;
}

// Code texte obligatoire (fournisseur, publicité…) passé dans le chemin
function parseCode(v, name, { max = 50 } = {}) {
  const s = parseText(v, name, { max });
  if (!s) throw new ValidationError(`Paramètre '${name}' requis`);
  return s;
}

// Pagination page/limit. La limite est ramenée au max (compatibilité ascendante).
function parsePagination(q, { defLimit = 50, maxLimit = 500, maxPage = 100000 } = {}) {
  const page  = parsePositiveInt(q.page,  'page',  { def: 1, min: 1, max: maxPage });
  const limit = parsePositiveInt(q.limit, 'limit', { def: defLimit, min: 1, max: maxLimit, clamp: true });
  return { page, limit, offset: (page - 1) * limit };
}

// Intervalle de dates [debut, fin] (bornes incluses), avec contrôle d'ordre et de durée max
function parseDateRange(q, {
  defDebut, defFin, maxDays, debutKey = 'dateDebut', finKey = 'dateFin',
} = {}) {
  const debut = parseDate(q[debutKey], debutKey, { def: defDebut });
  const fin   = parseDate(q[finKey],   finKey,   { def: defFin });
  checkRange(debut, fin, { maxDays, debutKey, finKey });
  return { debut, fin };
}

function checkRange(debut, fin, { maxDays, debutKey = 'dateDebut', finKey = 'dateFin' } = {}) {
  if (toUtcMs(debut) > toUtcMs(fin)) {
    throw new ValidationError(`'${debutKey}' doit être antérieure ou égale à '${finKey}'`);
  }
  if (maxDays && diffDays(debut, fin) + 1 > maxDays) {
    throw new ValidationError(`Période trop longue (max ${maxDays} jours)`);
  }
}

// Échappe les métacaractères LIKE/ILIKE (\, %, _) — échappement par défaut '\' de PostgreSQL
function escapeLike(s) {
  return String(s).replace(/[\\%_]/g, c => `\\${c}`);
}

// Motif « contient » pour LIKE/ILIKE
function likeContains(s) {
  return `%${escapeLike(s)}%`;
}

module.exports = {
  parseText, parsePositiveInt, parseDate, parseEnum, parseId, parseCode,
  parsePagination, parseDateRange, checkRange, escapeLike, likeContains,
};
