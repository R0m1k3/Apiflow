// CTE commune « franco de port » : seuil max de fouport2 par code fournisseur.
// Priorité à ce seuil, sinon foucad.franco (voir FRANCO_EXPR).
//
// codePlaceholder (optionnel) : restreint le calcul à un fournisseur ($n).

function francoPortCte(codePlaceholder = null) {
  return `franco_port AS (
        SELECT TRIM(fp.code) AS code, MAX(fp2.seuil) AS seuil
        FROM fouport fp
        JOIN fouport2 fp2 ON fp2.fou_no_id = fp.no_id
        ${codePlaceholder ? `WHERE TRIM(fp.code) = ${codePlaceholder}` : ''}
        GROUP BY TRIM(fp.code)
      )`;
}

// Franco effectif (alias fp = franco_port, fc = foucad)
const FRANCO_EXPR = 'COALESCE(fp.seuil, fc.franco)';

module.exports = { francoPortCte, FRANCO_EXPR };
