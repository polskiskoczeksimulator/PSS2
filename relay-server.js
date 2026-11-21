/* Polski Skoczek — serwer pośredniczący dla trybu CO-OP ONLINE
   Bez zależności, wystarczy Node.js 14+.   Start:  node relay-server.js
   Port: zmienna PORT (hosting ustawia ją sam), domyślnie 8787.

   Endpointy używane przez grę:
     POST /pub?ch=KANAŁ   – wiadomość JSON trafia do wszystkich nasłuchujących kanał (także do nadawcy)
     GET  /sub?ch=KANAŁ   – strumień SSE (EventSource) z wiadomościami kanału
     GET  /rooms          – lista pokoi (tablica JSON)
     POST /rooms          – dodaj/odśwież pokój (JSON z polem "id"); host robi to co 3 s
     GET  /               – prosty test „działa” (przydatne do wybudzenia hostingu) */
'use strict';
var http = require('http');
var PORT = parseInt(process.env.PORT, 10) || 8787;

var MAX_BODY = 64 * 1024;          // limit rozmiaru wiadomości
var MAX_SUBS_PER_CH = 32;          // limit słuchaczy jednego kanału
var MAX_ROOMS = 200;
var ROOM_TTL_OPEN = 12 * 1000;     // pokój nierozpoczęty znika, gdy host przestanie go odświeżać (co 3 s)
var ROOM_TTL_STARTED = 6 * 3600 * 1000; // pokój rozpoczęty trzymamy do 6 h od ostatniej aktualizacji

var channels = Object.create(null); // ch -> Set(res)
var rooms = Object.create(null);    // id -> { room, at }

function cors(res) {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  res.setHeader('Access-Control-Max-Age', '86400');
}
function send(res, code, body, type) {
  cors(res);
  res.writeHead(code, { 'Content-Type': type || 'text/plain; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(body);
}
function readBody(req, cb) {
  var size = 0, chunks = [], dead = false;
  req.on('data', function (c) {
    if (dead) return;
    size += c.length;
    if (size > MAX_BODY) { dead = true; cb(null); req.destroy(); return; }
    chunks.push(c);
  });
  req.on('end', function () { if (!dead) cb(Buffer.concat(chunks).toString('utf8')); });
  req.on('error', function () { if (!dead) { dead = true; cb(null); } });
}
function chName(q) {
  var c = q.searchParams.get('ch') || '';
  return /^[\w:.\-]{1,64}$/.test(c) ? c : null;
}
function cleanRooms() {
  var now = Date.now();
  Object.keys(rooms).forEach(function (id) {
    var r = rooms[id], ttl = r.room && r.room.started ? ROOM_TTL_STARTED : ROOM_TTL_OPEN;
    if (now - r.at > ttl) delete rooms[id];
  });
}
setInterval(cleanRooms, 5000).unref();

var server = http.createServer(function (req, res) {
  var u;
  try { u = new URL(req.url, 'http://x'); } catch (e) { return send(res, 400, 'bad url'); }
  var path = u.pathname.replace(/\/+$/, '') || '/';

  if (req.method === 'OPTIONS') { cors(res); res.writeHead(204); return res.end(); }

  if (path === '/' && req.method === 'GET') return send(res, 200, 'Polski Skoczek relay: dziala');

  /* ---- nasłuch kanału (SSE) ---- */
  if (path === '/sub' && req.method === 'GET') {
    var ch = chName(u);
    if (!ch) return send(res, 400, 'bad channel');
    var set = channels[ch] || (channels[ch] = new Set());
    if (set.size >= MAX_SUBS_PER_CH) return send(res, 429, 'too many listeners');
    cors(res);
    res.writeHead(200, {
      'Content-Type': 'text/event-stream; charset=utf-8',
      'Cache-Control': 'no-cache, no-transform',
      'Connection': 'keep-alive',
      'X-Accel-Buffering': 'no'
    });
    res.write('retry: 2000\n: ok\n\n');
    set.add(res);
    req.on('close', function () {
      set.delete(res);
      if (!set.size && channels[ch] === set) delete channels[ch];
    });
    return;
  }

  /* ---- wysłanie wiadomości na kanał ---- */
  if (path === '/pub' && req.method === 'POST') {
    var pc = chName(u);
    if (!pc) return send(res, 400, 'bad channel');
    return readBody(req, function (body) {
      if (body == null) return send(res, 413, 'too large');
      try { JSON.parse(body); } catch (e) { return send(res, 400, 'bad json'); }
      var set2 = channels[pc], n = 0;
      if (set2) {
        var line = 'data: ' + body.replace(/\r?\n/g, ' ') + '\n\n';
        set2.forEach(function (r) { try { r.write(line); n++; } catch (e) { set2.delete(r); } });
      }
      send(res, 200, JSON.stringify({ ok: true, delivered: n }), 'application/json');
    });
  }

  /* ---- lista pokoi ---- */
  if (path === '/rooms' && req.method === 'GET') {
    cleanRooms();
    var list = Object.keys(rooms).map(function (id) { return rooms[id].room; });
    return send(res, 200, JSON.stringify(list), 'application/json; charset=utf-8');
  }

  /* ---- dodanie / odświeżenie pokoju ---- */
  if (path === '/rooms' && req.method === 'POST') {
    return readBody(req, function (body) {
      if (body == null) return send(res, 413, 'too large');
      var room;
      try { room = JSON.parse(body); } catch (e) { return send(res, 400, 'bad json'); }
      if (!room || typeof room.id !== 'string' || !/^[\w\-]{1,16}$/.test(room.id)) return send(res, 400, 'bad room');
      if (!rooms[room.id] && Object.keys(rooms).length >= MAX_ROOMS) return send(res, 503, 'too many rooms');
      rooms[room.id] = { room: room, at: Date.now() };
      send(res, 200, '{"ok":true}', 'application/json');
    });
  }

  send(res, 404, 'not found');
});

/* podtrzymanie połączeń SSE (proxy hostingów zrywają ciche strumienie) */
setInterval(function () {
  Object.keys(channels).forEach(function (ch) {
    channels[ch].forEach(function (r) { try { r.write(': ping\n\n'); } catch (e) { channels[ch].delete(r); } });
  });
}, 15000).unref();

server.keepAliveTimeout = 65000;
server.listen(PORT, '0.0.0.0', function () {
  console.log('Polski Skoczek relay nasluchuje na porcie ' + PORT);
});
