// Fournisseur principal d'un article (artfou1.preference = 1).
// LATERAL … LIMIT 1 : un article ayant plusieurs lignes preference = 1 ne doit
// pas être compté plusieurs fois (doublons de CA dans les agrégats).
// Le code est renvoyé TRIMé ; joindre fouident avec TRIM(fi.code) = <alias>.code.

function fouPrincipalLateral(artNoIdExpr, alias = 'fp') {
  return `LEFT JOIN LATERAL (
        SELECT TRIM(af.code) AS code
        FROM artfou1 af
        WHERE af.art_no_id = ${artNoIdExpr} AND af.preference = 1
        ORDER BY af.no_id
        LIMIT 1
      ) ${alias} ON true`;
}

module.exports = { fouPrincipalLateral };
