'use strict';
/*
 * MDK Dokunma Oyunu - 8.4 CHAT Edition sohbet sunucusu
 * Sadece gerçek insanlar bağlanır. Sunucuda sahte oyuncu / yapay zekâ / bot YOKTUR.
 * Sunucu sadece: ad kaydı, çevrimiçi/çevrimdışı durumu ve mesajlaşma için çalışır.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

/* ------------------------- AYARLAR ------------------------- */
const PORT = process.env.PORT || 3000;
// Kalıcı diskte bir yol ver (örn. /data/data.json). Kalıcı disk yoksa sunucu yeniden başlayınca veriler silinir.
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
// "MDK (GELİŞTİRİCİ)" adını sadece bu şifreyi bilen alabilir. Boş bırakırsan o ad kimseye verilmez.
const MDK_SECRET = process.env.MDK_SECRET || '';
// Boş = her siteden bağlanılabilir. Örn: https://siten.com,https://www.siten.com
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim()).filter(Boolean);
const MAX_NEW_ACCOUNTS_PER_IP = Number(process.env.MAX_NEW_ACCOUNTS_PER_IP) || 10; // 24 saatte
const MAX_CONN_PER_IP = Number(process.env.MAX_CONN_PER_IP) || 10;

const DEV_NAME = 'MDK (GELİŞTİRİCİ)';
const NAME_MIN = 2, NAME_MAX = 25, NAME_MAX_SPACES = 4;
const NAME_COOLDOWN_MS = 13 * 24 * 60 * 60 * 1000; // 13 gün (SUNUCU saati)
const MSG_MAX = 200;
const HISTORY_STORE = 200, HISTORY_SEND = 100;
const MAX_USERS_SENT = 1000;
const DAY = 24 * 60 * 60 * 1000;

/* ------------------------- YARDIMCILAR ------------------------- */
const hasOwn = (o, k) => Object.prototype.hasOwnProperty.call(o, k);
const sha = s => crypto.createHash('sha256').update(String(s)).digest('hex');
const newId = () => crypto.randomBytes(6).toString('hex');
const newToken = () => crypto.randomBytes(24).toString('hex');
function safeEqual(a, b) {
  const ha = crypto.createHash('sha256').update(String(a)).digest();
  const hb = crypto.createHash('sha256').update(String(b)).digest();
  return crypto.timingSafeEqual(ha, hb);
}
function fold(s) {
  return String(s).normalize('NFKC').replace(/İ/g, 'i').replace(/I/g, 'i').toLowerCase()
    .replace(/ı/g, 'i').replace(/ş/g, 's').replace(/ç/g, 'c').replace(/ğ/g, 'g').replace(/ö/g, 'o').replace(/ü/g, 'u');
}
const nameKey = n => fold(n).replace(/[\s_.\-]/g, '');
const DEV_KEY = nameKey(DEV_NAME);

// Basit hız sınırı kovaları
const buckets = new Map();
function count(key, windowMs) {
  const now = Date.now();
  const a = (buckets.get(key) || []).filter(t => now - t < windowMs);
  buckets.set(key, a);
  return a.length;
}
function add(key) {
  const a = buckets.get(key) || [];
  a.push(Date.now());
  buckets.set(key, a);
}
setInterval(() => {
  const now = Date.now();
  for (const [k, a] of buckets) if (!a.length || now - a[a.length - 1] > DAY) buckets.delete(k);
}, 60 * 60 * 1000).unref();

/* ------------------------- VERİ ------------------------- */
let db = { users: {}, messages: {} };
try {
  const raw = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
  if (raw && typeof raw === 'object') { db.users = raw.users || {}; db.messages = raw.messages || {}; }
  console.log('Kayıtlı kullanıcı sayısı:', Object.keys(db.users).length);
} catch (e) { console.log('Kayıt dosyası yok, temiz başlanıyor.'); }

const keyIndex = new Map();
for (const u of Object.values(db.users)) keyIndex.set(u.key, u);

let dirty = false;
const markDirty = () => { dirty = true; };
function flush() {
  if (!dirty) return;
  dirty = false;
  const tmp = DATA_FILE + '.tmp';
  try {
    fs.mkdirSync(path.dirname(DATA_FILE), { recursive: true });
    fs.writeFileSync(tmp, JSON.stringify(db));
    fs.renameSync(tmp, DATA_FILE);
  } catch (e) { console.error('Kayıt hatası:', e.message); dirty = true; }
}
setInterval(flush, 5000).unref();
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, () => { flush(); process.exit(0); });

/* ------------------------- ÇEVRİMİÇİ TAKİBİ ------------------------- */
const online = new Map(); // userId -> Set<ws>
const isOnline = id => { const s = online.get(id); return !!s && s.size > 0; };
const publicUser = u => ({ id: u.id, name: u.name, online: isOnline(u.id) });

function sendTo(ws, obj) { if (ws.readyState === 1) ws.send(JSON.stringify(obj)); }
function sendToUser(id, obj) { const s = online.get(id); if (s) for (const w of s) sendTo(w, obj); }
function broadcast(obj, except) {
  const data = JSON.stringify(obj);
  for (const w of wss.clients) if (w !== except && w.userId && w.readyState === 1) w.send(data);
}
function sendErr(ws, ctx, code, msg, extra) { sendTo(ws, Object.assign({ t: 'error', ctx, code, msg }, extra || {})); }

function attach(ws, u) {
  ws.userId = u.id;
  let s = online.get(u.id);
  if (!s) { s = new Set(); online.set(u.id, s); }
  const wasOffline = s.size === 0;
  s.add(ws);
  u.lastSeen = Date.now();
  if (wasOffline) broadcast(Object.assign({ t: 'user' }, publicUser(u)), ws);
}
function detach(ws) {
  const id = ws.userId;
  if (!id) return;
  ws.userId = null;
  const s = online.get(id);
  if (!s) return;
  s.delete(ws);
  if (s.size === 0) {
    online.delete(id);
    const u = db.users[id];
    if (u) { u.lastSeen = Date.now(); markDirty(); broadcast(Object.assign({ t: 'user' }, publicUser(u))); }
  }
}

function meInfo(u) {
  const left = u.lastNameChange ? u.lastNameChange + NAME_COOLDOWN_MS - Date.now() : 0;
  return { id: u.id, name: u.name, changeInMs: Math.max(0, left) };
}
function userList() {
  return Object.values(db.users)
    .sort((a, b) => (isOnline(b.id) - isOnline(a.id)) || ((b.lastSeen || 0) - (a.lastSeen || 0)))
    .slice(0, MAX_USERS_SENT)
    .map(publicUser);
}
function sendWelcome(ws, u, token) {
  const out = { t: 'welcome', me: meInfo(u), users: userList() };
  if (token) out.token = token;
  sendTo(ws, out);
}

/* ------------------------- AD KURALLARI ------------------------- */
function validateName(raw) {
  if (typeof raw !== 'string') return { err: 'Ad geçersiz.' };
  const name = raw.normalize('NFC').trim();
  const len = Array.from(name).length;
  if (len < NAME_MIN) return { err: `Ad en az ${NAME_MIN} harf olmalı.` };
  if (len > NAME_MAX) return { err: `Ad en fazla ${NAME_MAX} harf olabilir.` };
  if ((name.match(/ /g) || []).length > NAME_MAX_SPACES) return { err: `Adda en fazla ${NAME_MAX_SPACES} boşluk olabilir.` };
  if (!/^[\p{L}\p{N} _.\-()]+$/u.test(name)) return { err: 'Adda sadece harf, rakam, boşluk ve _ . - ( ) kullanılabilir.' };
  if (!/[\p{L}\p{N}]/u.test(name)) return { err: 'Adda en az bir harf veya rakam olmalı.' };
  return { name, key: nameKey(name) };
}

// null = korumalı değil | {ok:true} = geliştirici şifresi doğru | {code,msg} = hata
function checkReserved(v, secret, ip) {
  const reserved = v.key.includes('mdk') || v.key.includes('gelistirici');
  if (!reserved) return null;
  if (v.key !== DEV_KEY) return { code: 'reserved', msg: 'Bu ad korumalı (MDK / GELİŞTİRİCİ). Başka bir ad seç.' };
  if (!MDK_SECRET) return { code: 'reserved', msg: 'Geliştirici adı bu sunucuda kapalı.' };
  if (count('sec:' + ip, 60 * 60 * 1000) >= 5) return { code: 'rate', msg: 'Çok fazla yanlış deneme. Bir saat sonra tekrar dene.' };
  if (typeof secret !== 'string' || !secret) return { code: 'need_secret', msg: 'Bu ad sadece geliştiriciye ait. Geliştirici şifresini yaz.' };
  if (!safeEqual(secret, MDK_SECRET)) { add('sec:' + ip); return { code: 'bad_secret', msg: 'Geliştirici şifresi yanlış.' }; }
  return { ok: true };
}

const convKey = (a, b) => [a, b].sort().join(':');

/* ------------------------- MESAJ İŞLEME ------------------------- */
function handle(ws, m) {
  switch (m.t) {
    case 'ping': return sendTo(ws, { t: 'pong' });

    case 'hello': {
      if (ws.userId) return;
      const u = (typeof m.id === 'string' && hasOwn(db.users, m.id)) ? db.users[m.id] : null;
      if (u && typeof m.token === 'string' && u.tokens.includes(sha(m.token))) {
        attach(ws, u);
        sendWelcome(ws, u);
      } else {
        sendTo(ws, { t: 'anon' });
      }
      return;
    }

    case 'register': {
      if (ws.userId) return sendErr(ws, 'name', 'already', 'Zaten giriş yaptın.');
      const v = validateName(m.name);
      if (v.err) return sendErr(ws, 'name', 'bad_name', v.err);
      const r = checkReserved(v, m.secret, ws.ip);
      if (r && !r.ok) return sendErr(ws, 'name', r.code, r.msg);
      const existing = keyIndex.get(v.key);
      if (existing && !(r && r.ok)) return sendErr(ws, 'name', 'taken', 'Bu ad alınmış, başka bir ad dene.');
      if (existing) { // geliştirici başka cihazdan giriş yapıyor
        const token = newToken();
        existing.tokens.push(sha(token));
        existing.tokens = existing.tokens.slice(-5);
        markDirty();
        attach(ws, existing);
        return sendWelcome(ws, existing, token);
      }
      if (count('reg:' + ws.ip, DAY) >= MAX_NEW_ACCOUNTS_PER_IP) {
        return sendErr(ws, 'name', 'limit', 'Bu bağlantıdan bugün çok fazla hesap açıldı. Yarın tekrar dene.');
      }
      const token = newToken();
      const u = { id: newId(), name: v.name, key: v.key, tokens: [sha(token)], createdAt: Date.now(), lastSeen: Date.now(), lastNameChange: 0 };
      db.users[u.id] = u;
      keyIndex.set(u.key, u);
      add('reg:' + ws.ip);
      markDirty();
      attach(ws, u);
      sendWelcome(ws, u, token);
      broadcast(Object.assign({ t: 'user' }, publicUser(u)), ws);
      return;
    }

    case 'rename': {
      const u = auth(ws, 'name'); if (!u) return;
      const v = validateName(m.name);
      if (v.err) return sendErr(ws, 'name', 'bad_name', v.err);
      const left = meInfo(u).changeInMs;
      if (left > 0) {
        const d = Math.floor(left / DAY), h = Math.floor((left % DAY) / 3600000), mi = Math.ceil((left % 3600000) / 60000);
        const txt = d > 0 ? `${d} gün ${h} saat` : (h > 0 ? `${h} saat ${mi} dk` : `${mi} dk`);
        return sendErr(ws, 'name', 'cooldown', `Adını ${txt} sonra değiştirebilirsin.`, { changeInMs: left });
      }
      if (v.name === u.name) return sendErr(ws, 'name', 'same', 'Zaten bu adı kullanıyorsun.');
      const r = checkReserved(v, m.secret, ws.ip);
      if (r && !r.ok) return sendErr(ws, 'name', r.code, r.msg);
      const existing = keyIndex.get(v.key);
      if (existing && existing.id !== u.id) return sendErr(ws, 'name', 'taken', 'Bu ad alınmış, başka bir ad dene.');
      if (keyIndex.get(u.key) === u) keyIndex.delete(u.key);
      u.name = v.name; u.key = v.key; u.lastNameChange = Date.now();
      keyIndex.set(u.key, u);
      markDirty();
      sendToUser(u.id, { t: 'me', me: meInfo(u) });
      broadcast(Object.assign({ t: 'user' }, publicUser(u)));
      return;
    }

    case 'history': {
      const u = auth(ws, 'chat'); if (!u) return;
      if (typeof m.with !== 'string' || !hasOwn(db.users, m.with)) return;
      const arr = db.messages[convKey(u.id, m.with)] || [];
      return sendTo(ws, { t: 'history', with: m.with, msgs: arr.slice(-HISTORY_SEND) });
    }

    case 'msg': {
      const u = auth(ws, 'chat'); if (!u) return;
      if (typeof m.to !== 'string' || !hasOwn(db.users, m.to) || m.to === u.id) return sendErr(ws, 'chat', 'bad_msg', 'Mesaj gönderilemedi.');
      if (typeof m.text !== 'string') return;
      const text = m.text.replace(/[\u0000-\u001F\u007F]/g, ' ').replace(/\s+/g, ' ').trim();
      if (!text) return;
      if (Array.from(text).length > MSG_MAX) return sendErr(ws, 'chat', 'bad_msg', `Mesaj en fazla ${MSG_MAX} harf olabilir.`);
      const now = Date.now();
      if (now - (ws.lastMsgAt || 0) < 700 || count('msg:' + u.id, 30000) >= 20) {
        return sendErr(ws, 'chat', 'too_fast', 'Çok hızlı yazıyorsun, biraz bekle.');
      }
      if (!isOnline(m.to)) return sendErr(ws, 'chat', 'offline', 'Bu kişi çevrimdışı, mesaj gönderilemedi.');
      ws.lastMsgAt = now; add('msg:' + u.id);
      const msg = { id: crypto.randomBytes(5).toString('hex'), from: u.id, to: m.to, text, ts: now };
      const key = convKey(u.id, m.to);
      const arr = db.messages[key] || (db.messages[key] = []);
      arr.push(msg);
      if (arr.length > HISTORY_STORE) arr.splice(0, arr.length - HISTORY_STORE);
      markDirty();
      const payload = { t: 'msg', m: msg, fn: u.name };
      sendToUser(m.to, payload);
      sendToUser(u.id, payload);
      return;
    }
  }
}

function auth(ws, ctx) {
  const u = ws.userId ? db.users[ws.userId] : null;
  if (!u) { sendErr(ws, ctx, 'not_authed', 'Önce adını seçmelisin.'); return null; }
  return u;
}

/* ------------------------- SUNUCU ------------------------- */
const server = http.createServer((req, res) => {
  if (req.url === '/' || req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    return res.end('MDK sohbet sunucusu çalışıyor');
  }
  res.writeHead(404); res.end();
});
const wss = new WebSocketServer({ server, maxPayload: 4096 });
const ipConns = new Map();
const getIp = req => ((req.headers['x-forwarded-for'] || '').split(',')[0].trim()) || req.socket.remoteAddress || 'x';

wss.on('connection', (ws, req) => {
  if (ALLOWED_ORIGINS.length && !ALLOWED_ORIGINS.includes(req.headers.origin)) return ws.close(1008, 'origin');
  const ip = getIp(req);
  const cur = ipConns.get(ip) || 0;
  if (cur >= MAX_CONN_PER_IP) return ws.close(1013, 'limit');
  ipConns.set(ip, cur + 1);

  ws.ip = ip; ws.isAlive = true; ws.userId = null; ws.lastMsgAt = 0; ws.recent = [];
  ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', (raw, isBinary) => {
    if (isBinary) return;
    const now = Date.now();
    ws.recent = ws.recent.filter(t => now - t < 10000);
    ws.recent.push(now);
    if (ws.recent.length > 40) return ws.close(1008, 'flood'); // 10 sn'de 40+ istek = spam
    let m;
    try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    try { handle(ws, m); } catch (e) { console.error('İşlem hatası:', e.message); }
  });
  ws.on('close', () => { ipConns.set(ip, Math.max(0, (ipConns.get(ip) || 1) - 1)); detach(ws); });
  ws.on('error', () => {});
});

setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { ws.terminate(); continue; }
    ws.isAlive = false;
    ws.ping();
  }
}, 30000).unref();

server.listen(PORT, () => console.log('MDK sohbet sunucusu port ' + PORT + ' üzerinde çalışıyor'));
