// Service worker: app shell cached for offline start; API calls always go to the network.
// It also sends the outbox when the net comes back while the app is closed (Android Chrome: Background Sync).
const V = 'bele-v18', SHELL = ['./', './index.html', './manifest.webmanifest', './icon.svg', './icon-192.png', './icon-512.png', './apple-touch-icon.png'];
self.addEventListener('install', e => { e.waitUntil(caches.open(V).then(c => c.addAll(SHELL)).then(() => self.skipWaiting())); });
self.addEventListener('activate', e => { e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== V).map(k => caches.delete(k)))).then(() => self.clients.claim())); });
self.addEventListener('fetch', e => {
  const r = e.request; if (r.method !== 'GET') return;
  const u = new URL(r.url);
  if (u.hostname.endsWith('supabase.co') || u.hostname.endsWith('script.google.com') || u.hostname.endsWith('googleusercontent.com')) return; // API: network only
  e.respondWith(fetch(r).then(res => { if (u.origin === location.origin || u.hostname.includes('gstatic') || u.hostname.includes('googleapis')) { const cp = res.clone(); caches.open(V).then(c => c.put(r, cp)); } return res; }).catch(() => caches.match(r).then(m => m || caches.match('./index.html'))));
});

// ---- the same small IndexedDB store the app writes: cfg, sess, outbox; we write back sent / failed
const KV = { db: null,
  open() { return this.db || (this.db = new Promise((res, rej) => { const q = indexedDB.open('bele', 1); q.onupgradeneeded = () => q.result.createObjectStore('kv'); q.onsuccess = () => res(q.result); q.onerror = () => rej(q.error); })); },
  async get(k) { const d = await this.open(); return new Promise(res => { const q = d.transaction('kv').objectStore('kv').get(k); q.onsuccess = () => res(q.result); q.onerror = () => res(undefined); }); },
  async set(k, v) { const d = await this.open(); return new Promise(res => { const x = d.transaction('kv', 'readwrite'); x.objectStore('kv').put(v, k); x.oncomplete = () => res(true); x.onerror = () => res(false); }); } };

let busy = null;
const sendOutbox = () => busy || (busy = send().finally(() => { busy = null; }));
async function send() {
  const cfg = await KV.get('cfg'), sess0 = await KV.get('sess'); let L = (await KV.get('outbox')) || [];
  if (!cfg || !sess0 || !L.length) return;
  let sess = sess0; const sent = [], failed = [];
  const post = (path, body, at) => fetch(cfg.url + path, { method: 'POST', body: JSON.stringify(body || {}),
    headers: Object.assign({ apikey: cfg.anon, 'Content-Type': 'application/json' }, at ? { Authorization: 'Bearer ' + at } : {}) });
  async function token() {                       // renew the phone's sign-in if needed (never a new sign-up here)
    if (sess.exp > Date.now() / 1000 + 60) return sess.at;
    const r = await post('/auth/v1/token?grant_type=refresh_token', { refresh_token: sess.rt });
    if (!r.ok) { if (r.status >= 500 || r.status === 429) throw new Error('net'); return null; }   // sign-in lost: the app handles it when opened
    const j = await r.json();
    sess = { at: j.access_token, rt: j.refresh_token, exp: j.expires_at || Math.floor(Date.now() / 1000) + (j.expires_in || 3600) };
    await KV.set('sess', sess); return sess.at;
  }
  try {
    while (L.length) {
      const at = await token(); if (!at) break;
      const head = L[0];
      if (head.type === 'rpc') {
        const r = await post('/rest/v1/rpc/' + head.rpc, head.args, at);
        if (r.status >= 500 || r.status === 401 || r.status === 429) throw new Error('net');
        if (!r.ok) { const j = await r.json().catch(() => ({})); failed.push({ client_id: head.client_id, err: String(j.message || '').match(/^E:[\w:.\-]+/)?.[0] || j.message || '' }); }
        sent.push(head.client_id); L = L.slice(1); continue;
      }
      const batch = []; for (const x of L) { if (x.type === 'rpc' || batch.length >= 40) break; batch.push(x); }
      const r = await post('/rest/v1/rpc/bb_save', { p_items: batch }, at);
      if (!r.ok) throw new Error('net');
      const res = await r.json(), keep = new Set();
      (res || []).forEach(x => { if (!x.ok && x.retry) keep.add(x.client_id); else if (!x.ok) failed.push(Object.assign({}, x)); });
      batch.forEach(x => { if (!keep.has(x.client_id)) sent.push(x.client_id); });
      const done = new Set(sent); L = L.filter(x => !done.has(x.client_id));
      if (keep.size) throw new Error('retry');
    }
  } finally {                                      // tell the app what went (it removes those from its own list)
    if (sent.length) { await KV.set('outbox', L); await KV.set('sent', ((await KV.get('sent')) || []).concat(sent)); }
    if (failed.length) await KV.set('failed', ((await KV.get('failed')) || []).concat(failed.map(f => Object.assign({ at: Date.now() }, f))));
    if (sent.length) (await self.clients.matchAll({ includeUncontrolled: true })).forEach(c => c.postMessage('bb-sent'));
  }
}
// a failed attempt rejects, so the phone tries again later by itself
self.addEventListener('sync', e => { if (e.tag === 'bb-outbox') e.waitUntil(sendOutbox()); });
self.addEventListener('periodicsync', e => { if (e.tag === 'bb-outbox') e.waitUntil(sendOutbox().catch(() => {})); });
self.addEventListener('message', e => { if (e.data === 'bb-sync') e.waitUntil(sendOutbox().catch(() => {})); });
