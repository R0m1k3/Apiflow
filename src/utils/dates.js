// Utilitaires de dates « calendaires » (chaînes YYYY-MM-DD), sans dépendance.
// Toute l'arithmétique se fait en UTC pour éviter les surprises de fuseau / heure d'été.

const DAY_MS = 86400000;

const parisFmt = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Europe/Paris', year: 'numeric', month: '2-digit', day: '2-digit',
});

// Date du jour à Paris, au format YYYY-MM-DD
function todayParis() {
  const parts = Object.fromEntries(parisFmt.formatToParts(new Date()).map(p => [p.type, p.value]));
  return `${parts.year}-${parts.month}-${parts.day}`;
}

function toUtcMs(iso) {
  const [y, m, d] = iso.split('-').map(Number);
  return Date.UTC(y, m - 1, d);
}

function fromUtcMs(ms) {
  return new Date(ms).toISOString().slice(0, 10);
}

// Ajoute n jours (négatif possible) à une date YYYY-MM-DD
function addDays(iso, n) {
  return fromUtcMs(toUtcMs(iso) + n * DAY_MS);
}

// Nombre de jours entre deux dates (b - a)
function diffDays(a, b) {
  return Math.round((toUtcMs(b) - toUtcMs(a)) / DAY_MS);
}

// Premier jour du mois de la date donnée
function firstDayOfMonth(iso) {
  return `${iso.slice(0, 8)}01`;
}

module.exports = { todayParis, addDays, diffDays, firstDayOfMonth, toUtcMs };
