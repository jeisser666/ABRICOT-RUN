'use strict';
// Сервер «Абрикосового забега»: раздаёт игру, держит лобби и рейтинг.
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const PUBLIC = path.join(__dirname, 'public');
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'players.json');
const MAX_ROOM = 5;
const MAX_PLAYERS = 5000;

/* ---------- рейтинг ---------- */
let players = {};
try { players = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')) || {}; } catch (e) { players = {}; }
let saveTimer = null;
function scheduleSave() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    try {
      fs.mkdirSync(DATA_DIR, { recursive: true });
      const tmp = DATA_FILE + '.tmp';
      fs.writeFileSync(tmp, JSON.stringify(players));
      fs.renameSync(tmp, DATA_FILE);
    } catch (e) { console.error('save failed', e.message); }
  }, 2000);
}
const int = (v, max) => Math.max(0, Math.min(max, Math.floor(Number(v) || 0)));
function cleanNick(v) {
  return String(v == null ? '' : v).replace(/[\u0000-\u001f<>]/g, '').trim().slice(0, 14);
}

/* ---------- статика ---------- */
const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.png': 'image/png', '.json': 'application/json',
  '.webmanifest': 'application/manifest+json', '.svg': 'image/svg+xml', '.ico': 'image/x-icon'
};
const server = http.createServer((req, res) => {
  let url = decodeURIComponent((req.url || '/').split('?')[0]);
  if (url === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); return res.end('ok'); }
  if (url === '/') url = '/index.html';
  const file = path.normalize(path.join(PUBLIC, url));
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404, { 'Content-Type': 'text/plain' }); return res.end('Not found'); }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(file)] || 'application/octet-stream',
      'Cache-Control': url === '/index.html' ? 'no-cache' : 'public, max-age=3600'
    });
    res.end(buf);
  });
});

/* ---------- websocket ---------- */
const wss = new WebSocketServer({ server, path: '/ws', maxPayload: 16 * 1024 });
const rooms = new Map(); // name -> { members: Map(peer -> {ws, by, pres}), dirty }
const colSubs = new Set(); // ws, подписанные на рейтинг

function send(ws, o) { if (ws.readyState === 1) ws.send(JSON.stringify(o)); }
function roomPeers(r) {
  return [...r.members.entries()].map(([peer, m]) => ({ peer, by: m.by, pres: m.pres }));
}
function leaveRoom(ws, name) {
  const r = rooms.get(name);
  if (!r) return;
  for (const [peer, m] of r.members) if (m.ws === ws) r.members.delete(peer);
  ws.rooms.delete(name);
  if (r.members.size === 0) rooms.delete(name); else r.dirty = true;
}
setInterval(() => {
  for (const [name, r] of rooms) {
    if (!r.dirty) continue;
    r.dirty = false;
    const msg = JSON.stringify({ t: 'peers', room: name, peers: roomPeers(r) });
    for (const m of r.members.values()) if (m.ws.readyState === 1) m.ws.send(msg);
  }
}, 60);

wss.on('connection', (ws) => {
  ws.id = null; ws.peer = null; ws.rooms = new Set(); ws.alive = true;
  let tokens = 120, last = Date.now();
  ws.on('pong', () => { ws.alive = true; });
  ws.on('message', (raw) => {
    const now = Date.now();
    tokens = Math.min(120, tokens + (now - last) / 1000 * 80); last = now;
    if (--tokens < 0) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;

    if (m.t === 'hello') {
      if (typeof m.id === 'string' && /^[A-Za-z0-9_-]{6,40}$/.test(m.id)) ws.id = m.id;
      if (typeof m.peer === 'string' && /^[A-Za-z0-9_-]{4,40}$/.test(m.peer)) ws.peer = m.peer;
      return;
    }
    if (!ws.id || !ws.peer) return;

    if (m.t === 'get') {
      const mm = /^players\/([A-Za-z0-9_-]+)$/.exec(String(m.path || ''));
      send(ws, { t: 'res', rid: m.rid, data: mm && players[mm[1]] ? players[mm[1]] : null });
    } else if (m.t === 'set') {
      if (m.path !== 'players/' + ws.id || !m.data || typeof m.data !== 'object') return;
      if (!players[ws.id] && Object.keys(players).length >= MAX_PLAYERS) return;
      const nick = cleanNick(m.data.nick);
      if (nick.length < 2) return;
      const doc = { nick, best: int(m.data.best, 1e6), bestApr: int(m.data.bestApr, 1e5), life: int(m.data.life, 1e8), upd: now };
      players[ws.id] = doc;
      scheduleSave();
      const out = JSON.stringify({ t: 'doc', path: 'players/' + ws.id, data: doc });
      for (const s of colSubs) if (s.readyState === 1) s.send(out);
    } else if (m.t === 'sub') {
      if (m.col !== 'players') return;
      colSubs.add(ws);
      send(ws, { t: 'col', col: 'players', docs: Object.keys(players).map((id) => ({ id, data: players[id] })) });
    } else if (m.t === 'join') {
      const name = String(m.room || '');
      if (!/^[a-z0-9][a-z0-9_.-]{0,47}$/.test(name)) return send(ws, { t: 'joinfail', room: name, code: 'invalid_argument' });
      let r = rooms.get(name);
      if (r && r.members.has(ws.peer) && r.members.get(ws.peer).ws === ws) return send(ws, { t: 'joined', room: name });
      if (r) { // старое соединение того же участника (переподключение)
        const old = r.members.get(ws.peer);
        if (old) r.members.delete(ws.peer);
      }
      if (r && r.members.size >= MAX_ROOM) return send(ws, { t: 'joinfail', room: name, code: 'limit_reached' });
      if (ws.rooms.size >= 2) return send(ws, { t: 'joinfail', room: name, code: 'limit_reached' });
      if (!r) { r = { members: new Map(), dirty: false }; rooms.set(name, r); }
      r.members.set(ws.peer, { ws, by: ws.id, pres: {} });
      ws.rooms.add(name);
      r.dirty = true;
      send(ws, { t: 'joined', room: name });
    } else if (m.t === 'pres') {
      const r = rooms.get(String(m.room || ''));
      const me = r && r.members.get(ws.peer);
      if (!me || me.ws !== ws || !m.pres || typeof m.pres !== 'object') return;
      const s = JSON.stringify(m.pres);
      if (s.length > 2048) return;
      me.pres = m.pres; r.dirty = true;
    } else if (m.t === 'leave') {
      leaveRoom(ws, String(m.room || ''));
    }
  });
  ws.on('close', () => {
    colSubs.delete(ws);
    for (const name of [...ws.rooms]) leaveRoom(ws, name);
  });
  ws.on('error', () => {});
});
setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.alive) { ws.terminate(); continue; }
    ws.alive = false; try { ws.ping(); } catch (e) {}
  }
}, 25000);

server.listen(PORT, () => console.log('Абрикосовый забег: http://localhost:' + PORT));
process.on('SIGTERM', () => { try { fs.mkdirSync(DATA_DIR, { recursive: true }); fs.writeFileSync(DATA_FILE, JSON.stringify(players)); } catch (e) {} process.exit(0); });
