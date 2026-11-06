// Définition UNIQUE du chiffre d'affaires (CA) sur mvtart.
//
// Mouvements de vente : types 11 (ventes clients) et 12 (retours clients), cf. table mvttype.
// Repli sur genremvt = 3 pour les lignes anciennes sans mvttypnoid.
// (Avant oct. 2026 le filtre était genremvt IN (3, 4, 9) : 4 = sorties de cession,
//  9 = démarques / inventaires → ces sorties étaient comptées comme du CA.)
//
// Une vente sort du stock : montants négatifs dans mvtart. Un retour les remet : montants positifs.
//
// CA NET (comme Qlik « CA Foirfouille » ; estimé le 05/11/2026 : écart < 0,1 % sur 2 magasins) :
//   ca_ht       = − Σ mntmvtht      (ventes − retours)
//   ca_ttc      = − Σ mntmvtttc
//   qte_vendue  = − Σ qtemvt
//   marge       =   Σ margemvt      (retours déduits, comme le CA)
//   taux_marge  = marge / ca_ht × 100
// Détail brut / retours : une ligne de montant TTC positif est un retour (retour client ou
// annulation dans le ticket), comme avant le 05/11/2026 :
//   ca_ht_brut, ca_ttc_brut = lignes négatives (= anciens ca_ht / ca_ttc) ; retours_ht, retours_ttc = lignes positives
//   ca_ht = ca_ht_brut − retours_ht
//
// Avant le 05/11/2026, ca_ht / ca_ttc / qte_vendue étaient BRUTS (retours non déduits) alors que
// marge et taux_marge étaient déjà nets. Un client qui recalculait marge / ca_ht obtenait un taux
// trop bas d'environ 0,5 pt (cas du rapprochement Qlik de Jarvis). Les champs *_brut gardent
// exactement l'ancienne valeur.

const GENRES_VENTE = [3];          // repli (lignes sans mvttypnoid)
const TYPES_VENTE  = [11, 12];      // mvttype : ventes + retours clients

// Fragments SQL pour un alias de table mvtart donné (ex. 'm')
function caSql(alias = 'm') {
  const c = (col) => (alias ? `${alias}.${col}` : col);
  const estRetour = `${c('mntmvtttc')} > 0`;
  // COALESCE : 0 et non NULL quand tous les montants sont NULL (sinon NULL passe en tête des tris DESC)
  const caHt = `-COALESCE(SUM(${c('mntmvtht')}), 0)`;
  const marge = `SUM(${c('margemvt')})`;
  return {
    filtreVentes: `COALESCE(${c('mvttypnoid')} IN (${TYPES_VENTE.join(', ')}), ${c('genremvt')} = 3)`,
    qteVendue:    `-COALESCE(SUM(${c('qtemvt')}), 0)`,
    caHt,
    caTtc:        `-COALESCE(SUM(${c('mntmvtttc')}), 0)`,
    caHtBrut:     `SUM(CASE WHEN ${c('mntmvtttc')} < 0 THEN ABS(${c('mntmvtht')}) ELSE 0 END)`,
    caTtcBrut:    `SUM(CASE WHEN ${c('mntmvtttc')} < 0 THEN ABS(${c('mntmvtttc')}) ELSE 0 END)`,
    retoursHt:    `SUM(CASE WHEN ${estRetour} THEN ${c('mntmvtht')} ELSE 0 END)`,
    retoursTtc:   `SUM(CASE WHEN ${estRetour} THEN ${c('mntmvtttc')} ELSE 0 END)`,
    marge,
    tauxMarge:    tauxMarge(marge, caHt),
  };
}

// Taux de marge (%) à partir de deux expressions déjà agrégées : marge et CA HT net.
// 0 si le CA HT net est nul ou négatif (période avec plus de retours que de ventes).
function tauxMarge(margeExpr, caHtExpr) {
  return `CASE WHEN ${caHtExpr} > 0
          THEN ROUND((${margeExpr}) / (${caHtExpr}) * 100, 2)
          ELSE 0 END`;
}

module.exports = { GENRES_VENTE, TYPES_VENTE, caSql, tauxMarge };
