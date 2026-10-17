// Helpers communs aux routes d'analyse (oct. 2026).
const { parseDateRange, parseText } = require('../utils/params');
const { todayParis, addDays } = require('../utils/dates');

// Ventes clients = types de mouvement 11 (ventes) et 12 (retours), cf. table mvttype.
// Repli sur genremvt = 3 pour d'éventuelles lignes anciennes sans mvttypnoid.
const VENTES = (a = 'm') => `COALESCE(${a}.mvttypnoid IN (11, 12), ${a}.genremvt = 3)`;

// Période [dateDebut, dateFin] inclusive ; défaut : 30 derniers jours complets ; max 3 ans
function periode(q, { defJours = 30, maxDays = 1100 } = {}) {
  const hier = addDays(todayParis(), -1);
  return parseDateRange(q, { defDebut: addDays(hier, -(defJours - 1)), defFin: hier, maxDays });
}

// Code site (égalité stricte), optionnel
function site(q) {
  return parseText(q.site, 'site', { max: 10 });
}

module.exports = { VENTES, periode, site };
