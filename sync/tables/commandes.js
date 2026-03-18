const { getMssql, getPg } = require('../db');
const { fullRefresh, batchUpsert, getLastSync, logSync, safeBit, safeStr } = require('../utils');

const VIVANT_COLS = [
  'cdefou_ligne_com_no_id','artfou1_no_id','articles_codein','articles_libelle1',
  'articles_libelle2','artfou1_reference','artfou1_ean13','artfou1_itf',
  'cdefou_ligne_qtecde','cdefou_ligne_prixbrut','cdefou_ligne_remise',
  'cdefou_ligne_remise2','cdefou_ligne_remise3','cdefou_ligne_prixnet',
  'cdefou_ligne_montant','cdefou_ligne_prixvente','cdefou_ligne_gratuit',
  'cdefou_ligne_qteacc','cdefou_ligne_qteann','cdefou_ligne_qteatt',
  'cdefou_ligne_qterel','cdefou_ligne_cdeligtard','cdefou_ligne_cdeligtot',
  'cotisation_logistique','fraislogistic','transit','distribution','taxe',
  'commentaire','suividatecreation',
];
const RECEP_COLS = ['no_id','suividatecreation','suividatemodif'];
const RECEPLIG_COLS = [
  'no_id','cdefou_reception_no_id','artfou1_no_id',
  'qtebl','qterec','qteacc','qteref','qteaff',
  'pribrut','remise','prirec','mntrec',
  'motifrefus','recpb','recpbok',
  'suividatecreation','suividatemodif',
];

async function syncCommandes(force) {
  const ms = await getMssql();
  const pg  = getPg();

  // === CDEFOU_VIVANT (full refresh — commandes en cours uniquement) ===
  try {
    const res = await ms.request().query(`
      SELECT
        CDEFOU_LIGNE_COM_NO_ID, ARTFOU1_NO_ID,
        ARTICLES_CODEIN, ARTICLES_LIBELLE1, ARTICLES_LIBELLE2,
        ARTFOU1_REFERENCE, ARTFOU1_EAN13, ARTFOU1_ITF,
        CDEFOU_LIGNE_QTECDE, CDEFOU_LIGNE_PRIXBRUT, CDEFOU_LIGNE_REMISE,
        CDEFOU_LIGNE_REMISE2, CDEFOU_LIGNE_REMISE3, CDEFOU_LIGNE_PRIXNET,
        CDEFOU_LIGNE_MONTANT, CDEFOU_LIGNE_PRIXVENTE, CDEFOU_LIGNE_GRATUIT,
        CDEFOU_LIGNE_QTEACC, CDEFOU_LIGNE_QTEANN, CDEFOU_LIGNE_QTEATT,
        CDEFOU_LIGNE_QTEREL, CDEFOU_LIGNE_CDELIGTARD, CDEFOU_LIGNE_CDELIGTOT,
        COTISATION_LOGISTIQUE, FRAISLOGISTIC, TRANSIT, DISTRIBUTION, TAXE,
        COMMENTAIRE, SUIVIDATECREATION
      FROM CDEFOU_VIVANT
      WHERE ARTFOU1_NO_ID IS NOT NULL
    `);

    const rows = res.recordset.map(r => ({
      cdefou_ligne_com_no_id:  r.CDEFOU_LIGNE_COM_NO_ID,
      artfou1_no_id:           r.ARTFOU1_NO_ID,
      articles_codein:         r.ARTICLES_CODEIN,
      articles_libelle1:       r.ARTICLES_LIBELLE1,
      articles_libelle2:       r.ARTICLES_LIBELLE2,
      artfou1_reference:       r.ARTFOU1_REFERENCE,
      artfou1_ean13:           r.ARTFOU1_EAN13,
      artfou1_itf:             r.ARTFOU1_ITF,
      cdefou_ligne_qtecde:     r.CDEFOU_LIGNE_QTECDE,
      cdefou_ligne_prixbrut:   r.CDEFOU_LIGNE_PRIXBRUT,
      cdefou_ligne_remise:     r.CDEFOU_LIGNE_REMISE,
      cdefou_ligne_remise2:    r.CDEFOU_LIGNE_REMISE2,
      cdefou_ligne_remise3:    r.CDEFOU_LIGNE_REMISE3,
      cdefou_ligne_prixnet:    r.CDEFOU_LIGNE_PRIXNET,
      cdefou_ligne_montant:    r.CDEFOU_LIGNE_MONTANT,
      cdefou_ligne_prixvente:  r.CDEFOU_LIGNE_PRIXVENTE,
      cdefou_ligne_gratuit:    r.CDEFOU_LIGNE_GRATUIT,
      cdefou_ligne_qteacc:     r.CDEFOU_LIGNE_QTEACC,
      cdefou_ligne_qteann:     r.CDEFOU_LIGNE_QTEANN,
      cdefou_ligne_qteatt:     r.CDEFOU_LIGNE_QTEATT,
      cdefou_ligne_qterel:     r.CDEFOU_LIGNE_QTEREL,
      cdefou_ligne_cdeligtard: r.CDEFOU_LIGNE_CDELIGTARD,
      cdefou_ligne_cdeligtot:  r.CDEFOU_LIGNE_CDELIGTOT,
      cotisation_logistique:   r.COTISATION_LOGISTIQUE,
      fraislogistic:           r.FRAISLOGISTIC,
      transit:                 r.TRANSIT,
      distribution:            r.DISTRIBUTION,
      taxe:                    r.TAXE,
      commentaire:             r.COMMENTAIRE,
      suividatecreation:       r.SUIVIDATECREATION,
    }));

    const count = await fullRefresh(pg, 'cdefou_vivant', rows, VIVANT_COLS);
    await logSync(pg, 'cdefou_vivant', count, 'ok');
    console.log(`[cdefou_vivant] ${count} lignes refresh`);
  } catch (err) {
    await logSync(pg, 'cdefou_vivant', 0, 'error', err.message);
    console.error(`[cdefou_vivant] ERREUR: ${err.message}`);
  }

  // === CDEFOU_RECEPTION (upsert delta) ===
  try {
    const lastSync = force ? null : await getLastSync(pg, 'cdefou_reception');
    const where    = lastSync ? `WHERE SUIVIDATEMODIF > '${lastSync.toISOString()}'` : '';

    const res = await ms.request().query(
      `SELECT NO_ID, SUIVIDATECREATION, SUIVIDATEMODIF FROM CDEFOU_RECEPTION ${where}`
    );
    const rows = res.recordset.map(r => ({
      no_id:             r.NO_ID,
      suividatecreation: r.SUIVIDATECREATION,
      suividatemodif:    r.SUIVIDATEMODIF,
    }));
    const count = await batchUpsert(pg, 'cdefou_reception', rows, ['no_id'], RECEP_COLS);
    await logSync(pg, 'cdefou_reception', count, 'ok');
    console.log(`[cdefou_reception] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'cdefou_reception', 0, 'error', err.message);
    console.error(`[cdefou_reception] ERREUR: ${err.message}`);
  }

  // === CDEFOU_RECEPLIG (upsert delta) ===
  try {
    const lastSync = force ? null : await getLastSync(pg, 'cdefou_receplig');
    const where    = lastSync ? `WHERE SUIVIDATEMODIF > '${lastSync.toISOString()}'` : '';

    const res = await ms.request().query(`
      SELECT NO_ID, CDEFOU_RECEPTION_NO_ID, ARTFOU1_NO_ID,
             QTEBL, QTEREC, QTEACC, QTEREF, QTEAFF,
             PRIBRUT, REMISE, PRIREC, MNTREC,
             MOTIFREFUS, RECPB, RECPBOK,
             SUIVIDATECREATION, SUIVIDATEMODIF
      FROM CDEFOU_RECEPLIG ${where}
    `);
    const rows = res.recordset.map(r => ({
      no_id:                   r.NO_ID,
      cdefou_reception_no_id:  r.CDEFOU_RECEPTION_NO_ID,
      artfou1_no_id:           r.ARTFOU1_NO_ID,
      qtebl:                   r.QTEBL,
      qterec:                  r.QTEREC,
      qteacc:                  r.QTEACC,
      qteref:                  r.QTEREF,
      qteaff:                  r.QTEAFF,
      pribrut:                 r.PRIBRUT,
      remise:                  r.REMISE,
      prirec:                  r.PRIREC,
      mntrec:                  r.MNTREC,
      motifrefus:              safeStr(r.MOTIFREFUS),
      recpb:                   safeBit(r.RECPB),
      recpbok:                 safeBit(r.RECPBOK),
      suividatecreation:       r.SUIVIDATECREATION,
      suividatemodif:          r.SUIVIDATEMODIF,
    }));
    const count = await batchUpsert(pg, 'cdefou_receplig', rows, ['no_id'], RECEPLIG_COLS);
    await logSync(pg, 'cdefou_receplig', count, 'ok');
    console.log(`[cdefou_receplig] ${count} lignes upsert`);
  } catch (err) {
    await logSync(pg, 'cdefou_receplig', 0, 'error', err.message);
    console.error(`[cdefou_receplig] ERREUR: ${err.message}`);
  }
}

module.exports = { syncCommandes };
