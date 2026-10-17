/**
 * Moteur de synchronisation générique SQL Server → PostgreSQL.
 *
 * Trois stratégies, toutes transactionnelles (l'API ne voit jamais une table
 * vide ou à moitié chargée) et en streaming (pas de recordset entier en RAM) :
 *
 *  - full   : DELETE + INSERT de toute la table dans UNE transaction.
 *             Garde-fou de volume : si la source renvoie moins de 50 % des
 *             lignes existantes, ROLLBACK (sauf force).
 *  - window : remplacement par tranches mensuelles sur une colonne date.
 *             Nuit normale : on recharge les N derniers jours (corrections,
 *             lignes saisies en retard, suppressions dans la fenêtre).
 *             Initialisation / force / backfill : X mois en arrière.
 *  - custom : fonction dédiée (spec.run).
 *
 * Une spec décrit : name, table, mode, source (SQL), cols [[pg, src, conv]],
 * pk, et selon le mode : dateCol, srcDateCol, windowDays, initialMonths,
 * backfillCheck, expand(row) → lignes, dedupe.
 */
'use strict';
const { getMssql, getPg } = require('./db');
const { logSync } = require('./utils');

const MAX_PARAMS   = 60000;   // limite PG = 65535 paramètres par requête
const STREAM_BATCH = 5000;

// ── Conversions SQL Server → PostgreSQL ──────────────────────

const pad = (n) => String(n).padStart(2, '0');

const conv = {
  /** texte nettoyé (null bytes, espaces de padding des CHAR) */
  str(v) {
    if (v == null) return null;
    const s = String(v).replace(/\0/g, '').trim();
    return s === '' ? null : s;
  },
  num(v) {
    if (v == null || v === '') return null;
    const n = typeof v === 'number' ? v : Number(String(v).replace(',', '.'));
    return Number.isFinite(n) ? n : null;
  },
  int(v) {
    const n = conv.num(v);
    return n == null ? null : Math.round(n);
  },
  bool(v) {
    if (v == null) return null;
    if (typeof v === 'boolean') return v;
    return Number(v) !== 0;
  },
  /** BIGINT : les NO_ID SQL Server (13 chiffres) tiennent dans un double */
  id(v) {
    if (v == null || v === '') return null;
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  },
  /**
   * datetime → 'YYYY-MM-DD HH:MM:SS.mmm' (heure murale SQL Server, indépendante
   * du fuseau du process : mssql lit les datetime en UTC). null si ≤ 1901.
   */
  ts(v) {
    if (v == null) return null;
    const d = v instanceof Date ? v : new Date(v);
    if (isNaN(d) || d.getUTCFullYear() <= 1901) return null;
    return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} `
      + `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}.${String(d.getUTCMilliseconds()).padStart(3, '0')}`;
  },
  /** date seule 'YYYY-MM-DD' */
  day(v) {
    const s = conv.ts(v);
    return s ? s.slice(0, 10) : null;
  },
  /** "17/10/2026 09:47:39" → "2026-10-17 09:47:39" (heure locale magasin) */
  tsfr(v) {
    if (v == null) return null;
    const m = String(v).trim().match(/^(\d{2})\/(\d{2})\/(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/);
    if (!m) return null;
    return `${m[3]}-${m[2]}-${m[1]} ${m[4] || '00'}:${m[5] || '00'}:${m[6] || '00'}`;
  },
  /** "-50.00%" → -50 ; autre format → null */
  pct(v) {
    if (v == null) return null;
    const m = String(v).trim().match(/^(-?\d+(?:[.,]\d+)?)\s*%$/);
    return m ? Number(m[1].replace(',', '.')) : null;
  },
  /** code site : "0292" → "292" */
  site(v) {
    const s = conv.str(v);
    return s && /^\d+$/.test(s) ? String(Number(s)).padStart(3, '0') : s;
  },
};

// ── Helpers dates (bornes de tranches) ───────────────────────

/** Date JS UTC minuit pour mssql (useUTC) */
const utcDate = (y, m, d = 1) => new Date(Date.UTC(y, m, d));
const isoDay  = (d) => `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`;

/** Découpe [from, to[ en tranches mensuelles */
function monthChunks(from, to) {
  const chunks = [];
  let cur = from;
  while (cur < to) {
    const next = utcDate(cur.getUTCFullYear(), cur.getUTCMonth() + 1, 1);
    const end  = next < to ? next : to;
    chunks.push([cur, end]);
    cur = end;
  }
  return chunks;
}

// ── Lecture SQL Server en streaming ──────────────────────────

/**
 * Exécute `sqlText` et appelle `onBatch(rows)` par lots, en mettant la
 * requête en pause pendant l'écriture PG (mémoire bornée).
 * @returns {Promise<number>} nombre de lignes lues
 */
async function streamQuery(sqlText, inputs, onBatch, batchSize = STREAM_BATCH) {
  const ms = await getMssql();
  return new Promise((resolve, reject) => {
    const req = ms.request();
    for (const [k, v] of Object.entries(inputs || {})) req.input(k, v);
    req.stream = true;

    let buf = [];
    let total = 0;
    let failed = false;
    let chain = Promise.resolve();

    const fail = (err) => {
      if (failed) return;
      failed = true;
      try { req.cancel(); } catch { /* ignore */ }
      reject(err);
    };
    const flush = () => {
      if (!buf.length) return;
      const rows = buf;
      buf = [];
      req.pause();
      chain = chain
        .then(() => onBatch(rows))
        .then(() => { total += rows.length; if (!failed) req.resume(); })
        .catch(fail);
    };

    req.on('row', (r) => {
      if (failed) return;
      buf.push(r);
      if (buf.length >= batchSize) flush();
    });
    req.on('error', fail);
    req.on('done', () => {
      if (failed) return;
      flush();
      chain.then(() => { if (!failed) resolve(total); }).catch(fail);
    });
    req.query(sqlText);
  });
}

// ── Écriture PostgreSQL ──────────────────────────────────────

/**
 * INSERT multi-lignes paramétré.
 * @param {'update'|'nothing'|null} onConflict
 */
async function insertRows(client, table, cols, rows, pk = null, onConflict = 'update') {
  if (!rows.length) return 0;
  // Dédoublonnage par clé dans le lot (ON CONFLICT DO UPDATE refuse 2× la même clé)
  if (pk && pk.length) {
    const m = new Map();
    for (const r of rows) m.set(pk.map((k) => r[k]).join('\u0001'), r);
    rows = [...m.values()];
  }
  const chunk = Math.max(1, Math.floor(MAX_PARAMS / cols.length));
  let conflict = '';
  if (pk && pk.length && onConflict) {
    const upd = cols.filter((c) => !pk.includes(c));
    conflict = onConflict === 'update' && upd.length
      ? `ON CONFLICT (${pk.join(', ')}) DO UPDATE SET ${upd.map((c) => `${c} = EXCLUDED.${c}`).join(', ')}`
      : `ON CONFLICT (${pk.join(', ')}) DO NOTHING`;
  }
  for (let i = 0; i < rows.length; i += chunk) {
    const part = rows.slice(i, i + chunk);
    const values = [];
    const ph = part.map((row, ri) =>
      `(${cols.map((c, ci) => { values.push(row[c] ?? null); return `$${ri * cols.length + ci + 1}`; }).join(', ')})`
    );
    await client.query(`INSERT INTO ${table} (${cols.join(', ')}) VALUES ${ph.join(', ')} ${conflict}`, values);
  }
  return rows.length;
}

/** Transforme une ligne source selon spec.cols → objet PG (ou plusieurs via expand) */
function mapRows(spec, srcRows) {
  const out = [];
  for (const r of srcRows) {
    if (spec.expand) {
      for (const x of spec.expand(r, conv)) out.push(x);
      continue;
    }
    const o = {};
    for (const [pgCol, srcCol, fn] of spec.cols) {
      const raw = typeof srcCol === 'function' ? srcCol(r) : r[srcCol];
      o[pgCol] = fn ? fn(raw) : raw;
    }
    if (spec.filter && !spec.filter(o)) continue;
    out.push(o);
  }
  return out;
}

const pgCols = (spec) => spec.pgCols || spec.cols.map((c) => c[0]);

/** Exécute fn(client) dans une transaction dédiée */
async function withTx(fn) {
  const client = await getPg().connect();
  try {
    await client.query('BEGIN');
    const res = await fn(client);
    await client.query('COMMIT');
    return res;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

// ── Stratégies ───────────────────────────────────────────────

async function runFull(spec, { force }) {
  const cols = pgCols(spec);
  return withTx(async (client) => {
    const before = Number((await client.query(`SELECT count(*)::int AS n FROM ${spec.table}`)).rows[0].n);
    await client.query(`DELETE FROM ${spec.table}`);
    let written = 0;
    await streamQuery(spec.source, spec.inputs, async (rows) => {
      written += await insertRows(client, spec.table, cols, mapRows(spec, rows), spec.pk);
    });
    const after = Number((await client.query(`SELECT count(*)::int AS n FROM ${spec.table}`)).rows[0].n);
    const ratio = spec.minRatio ?? 0.5;
    if (!force && before > 100 && after < before * ratio) {
      throw new Error(`Garde-fou volume : ${after} lignes reçues contre ${before} existantes (< ${ratio * 100} %) — table conservée`);
    }
    return after;
  });
}

async function runWindow(spec, { force }) {
  const pg   = getPg();
  const cols = pgCols(spec);
  const now  = new Date();
  const to   = utcDate(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() + 1);

  // max lu en texte pour ne pas dépendre du fuseau du process
  const { rows: [{ mx: mxText }] } = await pg.query(`SELECT to_char(max(${spec.dateCol}), 'YYYY-MM-DD') AS mx FROM ${spec.table}`);
  const mx = mxText ? new Date(`${mxText}T00:00:00Z`) : null;
  let backfill = false;
  if (!force && mx && spec.backfillCheck) {
    backfill = (await pg.query(spec.backfillCheck)).rows[0]?.needed === true;
    if (backfill) console.log(`[${spec.name}] colonnes à compléter → rechargement historique`);
  }

  let from;
  if (force || !mx || backfill) {
    from = utcDate(now.getUTCFullYear(), now.getUTCMonth() - (spec.initialMonths ?? 24), 1);
  } else {
    // fenêtre glissante à partir du max connu (rattrape les lignes en retard)
    from = utcDate(mx.getUTCFullYear(), mx.getUTCMonth(), mx.getUTCDate() - (spec.windowDays ?? 10));
  }

  let total = 0;
  for (const [d1, d2] of monthChunks(from, to)) {
    const n = await withTx(async (client) => {
      await client.query(
        `DELETE FROM ${spec.table} WHERE ${spec.dateCol} >= $1::date AND ${spec.dateCol} < $2::date`,
        [isoDay(d1), isoDay(d2)]
      );
      let written = 0;
      await streamQuery(spec.source, { ...(spec.inputs || {}), d1, d2 }, async (rows) => {
        written += await insertRows(client, spec.table, cols, mapRows(spec, rows), spec.pk);
      });
      return written;
    });
    total += n;
    if (n) console.log(`[${spec.name}] ${isoDay(d1)} → ${isoDay(d2)} : ${n} lignes`);
  }
  return total;
}

/** Point d'entrée : exécute une spec, journalise dans sync_log, ne lève jamais */
async function runSpec(spec, opts = {}) {
  const t0 = Date.now();
  const pg = getPg();
  try {
    let n;
    if (spec.mode === 'full')        n = await runFull(spec, opts);
    else if (spec.mode === 'window') n = await runWindow(spec, opts);
    else if (spec.mode === 'custom') n = await spec.run(opts, { streamQuery, insertRows, withTx, conv, mapRows });
    else throw new Error(`mode inconnu: ${spec.mode}`);
    await logSync(pg, spec.name, n, 'ok');
    console.log(`[${spec.name}] ${n} lignes (${spec.mode}) en ${((Date.now() - t0) / 1000).toFixed(1)}s`);
    return { ok: true, rows: n };
  } catch (err) {
    console.error(`[${spec.name}] ERREUR: ${err.message}`);
    await logSync(pg, spec.name, 0, 'error', err.message);
    return { ok: false, error: err.message };
  }
}

/** Exécute une liste de specs dans l'ordre */
async function runSpecs(specs, opts = {}) {
  for (const s of specs) await runSpec(s, opts);
}

module.exports = { conv, streamQuery, insertRows, withTx, mapRows, runSpec, runSpecs, monthChunks, utcDate, isoDay };
