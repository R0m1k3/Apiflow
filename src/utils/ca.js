// Définition UNIQUE du chiffre d'affaires (CA) sur mvtart.
//
// Convention (reprise de /api/performance/ca) :
//  - mouvements de vente : types 11 (ventes clients) et 12 (retours clients), cf. table mvttype.
//    Avant oct. 2026 le filtre était genremvt IN (3, 4, 9) : or 4 = sorties de cession et
//    9 = démarques / inventaires (vol, casse, périmé…) → ces sorties étaient comptées comme du CA.
//    Repli sur genremvt = 3 pour les lignes anciennes sans mvttypnoid.
//  - une vente est un mouvement de montant TTC négatif (sortie de stock) :
//      qte_vendue = Σ |qtemvt|     des lignes mntmvtttc < 0
//      ca_ht      = Σ |mntmvtht|   des lignes mntmvtttc < 0
//      ca_ttc     = Σ |mntmvtttc|  des lignes mntmvtttc < 0  (ventes brutes)
//      retours_ttc= Σ mntmvtttc    des lignes mntmvtttc > 0
//  - marge      = Σ margemvt (toutes lignes, retours inclus)
//  - taux_marge = marge / |Σ mntmvtht| × 100

const GENRES_VENTE = [3];          // repli (lignes sans mvttypnoid)
const TYPES_VENTE  = [11, 12];      // mvttype : ventes + retours clients

// Fragments SQL pour un alias de table mvtart donné (ex. 'm')
function caSql(alias = 'm') {
  const c = (col) => (alias ? `${alias}.${col}` : col);
  return {
    filtreVentes: `COALESCE(${c('mvttypnoid')} IN (${TYPES_VENTE.join(', ')}), ${c('genremvt')} = 3)`,
    qteVendue:    `SUM(CASE WHEN ${c('mntmvtttc')} < 0 THEN ABS(${c('qtemvt')})    ELSE 0 END)`,
    caHt:         `SUM(CASE WHEN ${c('mntmvtttc')} < 0 THEN ABS(${c('mntmvtht')})  ELSE 0 END)`,
    caTtc:        `SUM(CASE WHEN ${c('mntmvtttc')} < 0 THEN ABS(${c('mntmvtttc')}) ELSE 0 END)`,
    retoursTtc:   `SUM(CASE WHEN ${c('mntmvtttc')} > 0 THEN ${c('mntmvtttc')}      ELSE 0 END)`,
    marge:        `SUM(${c('margemvt')})`,
    // Σ mntmvtht « net » : sert de dénominateur au taux de marge
    mntHtNet:     `SUM(${c('mntmvtht')})`,
    tauxMarge:    tauxMarge(`SUM(${c('margemvt')})`, `SUM(${c('mntmvtht')})`),
  };
}

// Taux de marge à partir de deux expressions déjà agrégées
function tauxMarge(margeExpr, mntHtNetExpr) {
  return `CASE WHEN ${mntHtNetExpr} != 0
          THEN ROUND(${margeExpr} / ABS(${mntHtNetExpr}) * 100, 2)
          ELSE 0 END`;
}

module.exports = { GENRES_VENTE, TYPES_VENTE, caSql, tauxMarge };
