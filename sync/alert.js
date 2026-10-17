/**
 * Alerte webhook (Slack / Discord / Mattermost / ntfy…) si ALERT_WEBHOOK_URL est défini.
 * Envoie {text, content} pour être compris par les formats Slack et Discord.
 * Ne lève jamais.
 */
'use strict';

async function sendAlert(message) {
  const url = process.env.ALERT_WEBHOOK_URL;
  if (!url) return;
  try {
    const text = `[foirfouille-sync] ${message}`.slice(0, 1900);
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text, content: text }),
      signal: AbortSignal.timeout(10000),
    });
    if (!res.ok) console.error(`[alerte] webhook HTTP ${res.status}`);
  } catch (err) {
    console.error(`[alerte] envoi impossible : ${err.message}`);
  }
}

module.exports = { sendAlert };
