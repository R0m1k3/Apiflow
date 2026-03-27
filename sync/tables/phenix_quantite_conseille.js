const { getMssql, getPg } = require('../db');
const { fullRefresh, logSync } = require('../utils');

const COLS = [
  'ident','site','codein','libelle_article','code1','libelle1',
  'idartfou1','art_no_id','colisage','moyenne_vente_jour','nb_jour_vte',
  'besoin_calcule','stock_mini','stock_maxi','qte_non_recue','qte_stock',
  'besoin_pris','a_commander','montant',
];

async function syncPhenixQuantiteConseille(force) {
  const ms = await getMssql();
  const pg  = getPg();

  try {
    const res = await ms.request().query(`
      SELECT ident, site, CODEIN, LibelleArticle, code1, libelle1,
             Idartfou1, Art_no_id, colisage, MoyenneVenteParJour, NbJourVte,
             BesoinCalcule, StockMini, StockMaxi, QteNonRecue, QteStock,
             BesoinPrisEnCompte, ACommander, Montant
      FROM PHENIX_QUANTITE_CONSEILLE
    `);

    const rows = res.recordset.map(r => ({
      ident:              r.ident,
      site:               r.site,
      codein:             r.CODEIN,
      libelle_article:    r.LibelleArticle,
      code1:              r.code1,
      libelle1:           r.libelle1,
      idartfou1:          r.Idartfou1,
      art_no_id:          r.Art_no_id,
      colisage:           r.colisage,
      moyenne_vente_jour: r.MoyenneVenteParJour,
      nb_jour_vte:        r.NbJourVte,
      besoin_calcule:     r.BesoinCalcule,
      stock_mini:         r.StockMini ?? null,
      stock_maxi:         r.StockMaxi ?? null,
      qte_non_recue:      r.QteNonRecue ?? null,
      qte_stock:          r.QteStock ?? null,
      besoin_pris:        r.BesoinPrisEnCompte,
      a_commander:        r.ACommander,
      montant:            r.Montant,
    }));

    const count = await fullRefresh(pg, 'phenix_quantite_conseille', rows, COLS);
    await logSync(pg, 'phenix_quantite_conseille', count, 'ok');
    console.log(`[phenix_quantite_conseille] ${count} lignes`);
  } catch (err) {
    await logSync(pg, 'phenix_quantite_conseille', 0, 'error', err.message);
    console.error(`[phenix_quantite_conseille] ERREUR: ${err.message}`);
  }
}

module.exports = { syncPhenixQuantiteConseille };
