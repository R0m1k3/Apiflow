// Petits utilitaires de construction SQL paramétrée.

// Accumulateur de paramètres positionnels ($1, $2…)
class SqlParams {
  constructor(initial = []) {
    this.values = [...initial];
  }

  // Ajoute une valeur et renvoie son placeholder
  add(value) {
    this.values.push(value);
    return `$${this.values.length}`;
  }
}

// Filtre sur un intervalle de dates inclusif, correct pour une colonne TIMESTAMP :
//   col >= debut::date AND col < fin::date + 1
// (BETWEEN debut AND fin perdait toute la journée de fin après 00:00:00)
function dateRange(col, params, debut, fin) {
  return `${col} >= ${params.add(debut)}::date AND ${col} < ${params.add(fin)}::date + 1`;
}

// Filtre sur une seule journée (remplace col::date = $1, qui empêche l'usage d'un index)
function sameDay(col, params, jour) {
  const p = params.add(jour);
  return `${col} >= ${p}::date AND ${col} < ${p}::date + 1`;
}

function where(conditions) {
  const c = conditions.filter(Boolean);
  return c.length ? `WHERE ${c.join('\n        AND ')}` : '';
}

module.exports = { SqlParams, dateRange, sameDay, where };
