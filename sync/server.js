/**
 * Tableau de bord web du service sync — port 3001
 */
'use strict';
const express = require('express');
const crypto  = require('crypto');
const { getPg } = require('./db');

const app  = express();
const PORT = process.env.SYNC_DASHBOARD_PORT || 3001;

// ── État global ──────────────────────────────────────────────
const state = {
  running:   false,
  progress:  null,   // { current, index, total, startedAt, tables }
  lastError: null,
};


// Buffer des 300 dernières lignes de log
const logBuffer = [];
function pushLog(msg, level = 'info') {
  logBuffer.push({ ts: new Date().toISOString(), msg, level });
  if (logBuffer.length > 300) logBuffer.shift();
}

// Surcharge console
const _log = console.log.bind(console);
const _err = console.error.bind(console);
console.log   = (...a) => { pushLog(a.join(' '), 'info');  _log(...a); };
console.error = (...a) => { pushLog(a.join(' '), 'error'); _err(...a); };

// Appelé depuis index.js avant/après chaque groupe de tables
function setProgress(tableName, index, total) {
  state.progress = {
    current:   tableName,
    index:     index,          // 1-based
    total:     total,
    pct:       Math.round((index / total) * 100),
    startedAt: state.progress?.startedAt || new Date().toISOString(),
  };
}
function clearProgress() {
  state.progress = null;
}

// Fn sync enregistrée depuis index.js
let syncAllFn = null;
let cronExpr  = '45 6 * * *';
function registerSyncFn(fn, cron) { syncAllFn = fn; if (cron) cronExpr = cron; }
function setSyncRunning(val) { state.running = val; }
function isSyncRunning()     { return state.running; }

// ── Sécurité ─────────────────────────────────────────────────
app.disable('x-powered-by');

// Healthcheck Docker (sans authentification, ne divulgue rien)
app.get('/healthz', (req, res) => res.json({ ok: true }));

/**
 * Authentification HTTP Basic : DASHBOARD_USER / DASHBOARD_PASSWORD.
 * Sans ces variables, le dashboard est désactivé (il permet de lancer une sync
 * complète et affiche les logs : il ne doit jamais être ouvert sans mot de passe).
 */
function safeEqual(a, b) {
  const ha = crypto.createHash('sha256').update(String(a)).digest();
  const hb = crypto.createHash('sha256').update(String(b)).digest();
  return crypto.timingSafeEqual(ha, hb);
}
app.use((req, res, next) => {
  const user = process.env.DASHBOARD_USER;
  const pass = process.env.DASHBOARD_PASSWORD;
  if (!user || !pass) {
    return res.status(503).type('text').send('Dashboard désactivé : définir DASHBOARD_USER et DASHBOARD_PASSWORD');
  }
  const m = (req.headers.authorization || '').match(/^Basic (.+)$/);
  if (m) {
    const [u, ...rest] = Buffer.from(m[1], 'base64').toString('utf8').split(':');
    if (safeEqual(u, user) && safeEqual(rest.join(':'), pass)) return next();
  }
  res.set('WWW-Authenticate', 'Basic realm="sync", charset="UTF-8"');
  return res.status(401).type('text').send('Authentification requise');
});
app.use((req, res, next) => {
  res.set({
    'X-Content-Type-Options': 'nosniff',
    'X-Frame-Options': 'DENY',
    'Referrer-Policy': 'no-referrer',
    'Content-Security-Policy': "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'",
  });
  next();
});

// ── Routes API ───────────────────────────────────────────────
app.use(express.json());

// Statut complet (tables + progression en cours)
app.get('/api/status', async (req, res) => {
  try {
    const pg = getPg();
    const { rows } = await pg.query(
      `SELECT table_name, last_sync, rows_synced, status, error_msg
       FROM sync_log ORDER BY table_name`
    );
    res.json({
      running:  state.running,
      progress: state.progress,
      tables:   rows,
    });
  } catch (err) {
    console.error('[dashboard] /api/status :', err.message);
    res.status(500).json({ error: 'Erreur lecture sync_log' });
  }
});

// Logs en mémoire
app.get('/api/logs', (req, res) => {
  const limit = parseInt(req.query.limit) || 150;
  res.json(logBuffer.slice(-limit));
});

// Prochain cron (expression quotidienne « m h * * * », heure de Paris)
function parisParts(d) {
  const f = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Europe/Paris', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
  }).formatToParts(d);
  const g = (t) => Number(f.find((p) => p.type === t).value);
  return { y: g('year'), mo: g('month'), d: g('day'), h: g('hour'), mi: g('minute') };
}
function nextRun(expr, now = new Date()) {
  const m = String(expr).trim().match(/^(\d{1,2})\s+(\d{1,2})\s+\*\s+\*\s+\*$/);
  if (!m) return null;
  const min = Number(m[1]);
  const hour = Number(m[2]);
  for (let add = 0; add <= 2; add++) {
    const p = parisParts(new Date(now.getTime() + add * 86400e3));
    // Instant UTC « naïf » puis correction du décalage horaire de Paris
    let guess = new Date(Date.UTC(p.y, p.mo - 1, p.d, hour, min));
    const q = parisParts(guess);
    guess = new Date(guess.getTime() - ((q.h - hour) * 60 + (q.mi - min)) * 60e3);
    if (guess > now) return guess;
  }
  return null;
}
app.get('/api/next', (req, res) => {
  const next = nextRun(cronExpr);
  res.json({ next: next ? next.toISOString() : null, cron: cronExpr });
});

// Déclencher sync manuelle
app.post('/api/sync/run', async (req, res) => {
  if (state.running) return res.status(409).json({ error: 'Sync déjà en cours' });
  if (!syncAllFn)    return res.status(500).json({ error: 'syncAll non enregistré' });
  // force uniquement sur demande explicite (bouton « Forcer » avec confirmation)
  const force = req.body?.force === true;
  res.json({ message: force ? 'Sync complète forcée lancée' : 'Sync lancée' });
  syncAllFn(force).catch(err => console.error('Erreur sync manuelle:', err.message));
});

// ── Dashboard HTML ───────────────────────────────────────────
app.get('/', (req, res) => res.send(HTML));

function startDashboard() {
  app.listen(PORT, '0.0.0.0', () => {
    console.log(`Dashboard sync : http://0.0.0.0:${PORT}`);
  });
}

module.exports = { startDashboard, registerSyncFn, setSyncRunning, isSyncRunning, setProgress, clearProgress };

// ── HTML inline ──────────────────────────────────────────────
const HTML = `<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sync Dashboard</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:system-ui,sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh}
header{background:#1e293b;padding:1rem 2rem;display:flex;align-items:center;gap:1rem;border-bottom:1px solid #334155}
header h1{font-size:1.2rem;font-weight:700;color:#f8fafc}
.badge{font-size:.75rem;padding:.25rem .6rem;border-radius:9999px;font-weight:600}
.badge.idle{background:#1e3a5f;color:#93c5fd}
.badge.running{background:#713f12;color:#fde68a;animation:pulse 1s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}
main{padding:1.5rem 2rem;max-width:1100px;margin:0 auto}
.actions{display:flex;align-items:center;gap:1rem;flex-wrap:wrap;margin-bottom:1.5rem}
button{padding:.55rem 1.3rem;border:none;border-radius:.5rem;cursor:pointer;font-weight:600;font-size:.85rem;transition:.15s}
#btnSync{background:#2563eb;color:#fff}
#btnSync:hover:not(:disabled){background:#1d4ed8}
#btnSync:disabled{opacity:.4;cursor:not-allowed}
#btnRefresh{background:#334155;color:#cbd5e1}
#btnRefresh:hover{background:#475569}
#nextSync{font-size:.8rem;color:#64748b;margin-left:auto}
#msg{font-size:.82rem;padding:.35rem .75rem;border-radius:.4rem;display:none}
#msg.ok{background:#14532d;color:#86efac;display:inline-block}
#msg.err{background:#450a0a;color:#fca5a5;display:inline-block}

/* Progress */
#progressBox{background:#1e293b;border:1px solid #334155;border-radius:.75rem;padding:1rem 1.25rem;margin-bottom:1.5rem;display:none}
#progressBox.visible{display:block}
.progress-header{display:flex;justify-content:space-between;margin-bottom:.6rem;font-size:.85rem}
.progress-bar-bg{background:#0f172a;border-radius:9999px;height:12px;overflow:hidden}
.progress-bar-fill{height:100%;background:linear-gradient(90deg,#2563eb,#7c3aed);border-radius:9999px;transition:width .5s ease}
.progress-detail{margin-top:.5rem;font-size:.78rem;color:#64748b}

.grid{display:grid;grid-template-columns:1fr 1fr;gap:1.5rem}
@media(max-width:700px){.grid{grid-template-columns:1fr}}
.card{background:#1e293b;border:1px solid #334155;border-radius:.75rem;overflow:hidden}
.card-header{padding:.65rem 1rem;background:#0f172a;border-bottom:1px solid #334155;font-size:.75rem;font-weight:600;text-transform:uppercase;letter-spacing:.05em;color:#94a3b8}
table{width:100%;border-collapse:collapse;font-size:.85rem}
th{padding:.55rem 1rem;text-align:left;color:#64748b;font-weight:500;border-bottom:1px solid #334155}
td{padding:.55rem 1rem;border-bottom:1px solid #1e293b}
tr:last-child td{border-bottom:none}
.ok{color:#4ade80}.err{color:#f87171}
.log-box{height:320px;overflow-y:auto;font-family:monospace;font-size:.76rem;padding:.75rem 1rem;background:#0a0f1e;color:#a5f3fc;line-height:1.7;user-select:text;cursor:text}
.log-line{white-space:pre-wrap;word-break:break-all}
.log-line.error{color:#f87171}
.ts{color:#475569;margin-right:.5rem}
#btnCopyLogs{background:#334155;color:#cbd5e1;font-size:.75rem;padding:.35rem .8rem}
#btnCopyLogs:hover{background:#475569}
#lastRefresh{font-size:.75rem;color:#475569}
</style>
</head>
<body>
<header>
  <h1>⚡ Sync Dashboard</h1>
  <span id="statusBadge" class="badge idle">Idle</span>
  <span id="lastRefresh" style="margin-left:auto"></span>
</header>
<main>
  <div class="actions">
    <button id="btnSync" onclick="runSync(false)">▶ Lancer sync maintenant</button>
    <button id="btnForce" onclick="runSync(true)" title="Recharge 2 ans de mouvements et ignore les garde-fous de volume">⚠ Forcer sync complète</button>
    <button id="btnRefresh" onclick="refresh()">↺ Rafraîchir</button>
    <span id="msg"></span>
    <span id="nextSync"></span>
  </div>

  <!-- Barre de progression -->
  <div id="progressBox">
    <div class="progress-header">
      <span id="progressLabel">Synchronisation en cours…</span>
      <span id="progressPct" style="font-weight:700;color:#93c5fd">0%</span>
    </div>
    <div class="progress-bar-bg">
      <div class="progress-bar-fill" id="progressBar" style="width:0%"></div>
    </div>
    <div class="progress-detail" id="progressDetail"></div>
  </div>

  <div class="grid">
    <div class="card" style="grid-column:1/-1">
      <div class="card-header">Statut des tables</div>
      <table>
        <thead><tr><th>Table</th><th>Dernière sync</th><th>Lignes</th><th>Statut</th></tr></thead>
        <tbody id="tableBody"><tr><td colspan="4" style="color:#475569;padding:1rem">Chargement…</td></tr></tbody>
      </table>
    </div>
    <div class="card" style="grid-column:1/-1">
      <div class="card-header" style="display:flex;justify-content:space-between;align-items:center">
        <span>Logs</span>
        <button id="btnCopyLogs" onclick="copyLogs()">Copier les logs</button>
      </div>
      <div class="log-box" id="logBox">Chargement…</div>
    </div>
  </div>
</main>
<script>
let autoScroll = true;

function fmtDate(iso){
  if(!iso) return '—';
  return new Date(iso).toLocaleString('fr-FR');
}

function renderProgress(progress, running){
  const box = document.getElementById('progressBox');
  if(!running || !progress){ box.classList.remove('visible'); return; }
  box.classList.add('visible');
  document.getElementById('progressBar').style.width   = progress.pct + '%';
  document.getElementById('progressPct').textContent   = progress.pct + '%';
  document.getElementById('progressLabel').textContent =
    'Sync en cours — table ' + progress.index + '/' + progress.total;
  const elapsed = Math.round((Date.now() - new Date(progress.startedAt)) / 1000);
  document.getElementById('progressDetail').textContent =
    'En cours : ' + progress.current + ' | Durée : ' + elapsed + 's';
}

function esc(v){
  return String(v ?? '').replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
}
function renderStatus(data){
  const badge = document.getElementById('statusBadge');
  badge.textContent = data.running ? 'En cours…' : 'Idle';
  badge.className   = 'badge ' + (data.running ? 'running' : 'idle');
  document.getElementById('btnSync').disabled = data.running;
  document.getElementById('btnForce').disabled = data.running;
  renderProgress(data.progress, data.running);

  if(!data.tables.length){
    document.getElementById('tableBody').innerHTML =
      '<tr><td colspan="4" style="color:#475569;padding:1rem">Aucune sync — cliquez sur Lancer sync maintenant</td></tr>';
    return;
  }
  document.getElementById('tableBody').innerHTML = data.tables.map(t => \`
    <tr>
      <td><strong>\${esc(t.table_name)}</strong></td>
      <td>\${esc(fmtDate(t.last_sync))}</td>
      <td>\${esc(t.rows_synced ?? '—')}</td>
      <td class="\${t.status==='ok'?'ok':(t.status==='running'?'':'err')}">\${esc(t.status ?? '—')}\${t.error_msg?' — '+esc(t.error_msg):''}</td>
    </tr>
  \`).join('');
}

function renderLogs(logs){
  const box = document.getElementById('logBox');
  if(!logs.length){ box.textContent = 'Aucun log.'; return; }
  const atBottom = box.scrollTop + box.clientHeight >= box.scrollHeight - 10;
  box.innerHTML = logs.map(l => \`
    <div class="log-line \${l.level==='error'?'error':'info'}">
      <span class="ts">\${esc(new Date(l.ts).toLocaleTimeString('fr-FR'))}</span>\${esc(l.msg)}
    </div>
  \`).join('');
  if(atBottom) box.scrollTop = box.scrollHeight;
}

async function refresh(){
  try{
    const [s, logs, next] = await Promise.all([
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/logs?limit=150').then(r=>r.json()),
      fetch('/api/next').then(r=>r.json()),
    ]);
    renderStatus(s);
    renderLogs(logs);
    document.getElementById('nextSync').textContent = 'Prochaine sync auto : ' + fmtDate(next.next);
    document.getElementById('lastRefresh').textContent = 'Mis à jour ' + new Date().toLocaleTimeString('fr-FR');
  } catch(e){
    console.error(e);
  }
}

function copyLogs(){
  const box = document.getElementById('logBox');
  const text = Array.from(box.querySelectorAll('.log-line')).map(l => l.textContent).join('\\n');
  const btn = document.getElementById('btnCopyLogs');
  function onSuccess(){ btn.textContent = 'Copié !'; setTimeout(() => { btn.textContent = 'Copier les logs'; }, 2000); }
  if(navigator.clipboard && window.isSecureContext){
    navigator.clipboard.writeText(text).then(onSuccess).catch(fallback);
  } else {
    fallback();
  }
  function fallback(){
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.cssText = 'position:fixed;top:-9999px;left:-9999px;opacity:0';
    document.body.appendChild(ta);
    ta.focus(); ta.select();
    try{ document.execCommand('copy'); onSuccess(); } catch(e){ alert('Copie manuelle : sélectionnez les logs et Ctrl+C'); }
    document.body.removeChild(ta);
  }
}

async function runSync(force){
  if(force && !confirm('Forcer une sync complète ? (recharge 2 ans de mouvements, ignore les garde-fous de volume)')) return;
  const msg = document.getElementById('msg');
  msg.style.display = 'none';
  const r    = await fetch('/api/sync/run',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({force:!!force})});
  const data = await r.json();
  msg.textContent = r.ok ? '✓ ' + data.message : '✗ ' + data.error;
  msg.className   = r.ok ? 'ok' : 'err';
  setTimeout(()=>{ msg.style.display='none'; }, 5000);
  setTimeout(refresh, 800);
}

// Refresh rapide (5s) quand sync en cours, lent (15s) sinon
let timer;
async function loop(){
  await refresh();
  const s = await fetch('/api/status').then(r=>r.json()).catch(()=>({}));
  timer = setTimeout(loop, s.running ? 3000 : 15000);
}
loop();
</script>
</body>
</html>`;
