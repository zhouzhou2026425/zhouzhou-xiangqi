'use strict';
// ============================================================
// 周周象棋 - 联机服务器（纯 Node，无第三方依赖）
// 静态托管 index.html + WebSocket 房间中继
// 启动：node server.js   默认端口 3000（可用环境变量 PORT）
// ============================================================

const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const ROOT = __dirname;
const MAX_PAYLOAD = 64 * 1024;
const WS_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.webmanifest': 'application/manifest+json',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.txt': 'text/plain; charset=utf-8'
};

// ============================================================
// 静态文件
// ============================================================
const server = http.createServer((req, res) => {
  let urlPath;
  try {
    urlPath = decodeURIComponent((req.url || '/').split('?')[0]);
  } catch (e) {
    res.writeHead(400); res.end('Bad Request'); return;
  }
  if (urlPath === '/' || urlPath === '') urlPath = '/index.html';

  if (urlPath === '/healthz' || urlPath === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('ok');
    return;
  }

  const filePath = path.resolve(ROOT, '.' + urlPath);
  if (filePath !== ROOT && !filePath.startsWith(ROOT + path.sep)) {
    res.writeHead(403); res.end('Forbidden'); return;
  }

  fs.stat(filePath, (err, stat) => {
    if (err || !stat.isFile()) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('404 Not Found');
      return;
    }
    const ext = path.extname(filePath).toLowerCase();
    const headers = { 'Content-Type': MIME[ext] || 'application/octet-stream' };
    if (ext === '.html') headers['Cache-Control'] = 'no-cache';
    res.writeHead(200, headers);
    fs.createReadStream(filePath).pipe(res);
  });
});

server.on('clientError', (err, socket) => {
  try { socket.destroy(); } catch (e) {}
});

// ============================================================
// WebSocket 基础实现
// ============================================================
function encodeFrame(payload, opcode) {
  const data = Buffer.isBuffer(payload) ? payload : Buffer.from(String(payload || ''), 'utf8');
  const len = data.length;
  let head;
  if (len < 126) {
    head = Buffer.alloc(2);
    head[0] = 0x80 | (opcode || 1);
    head[1] = len;
  } else if (len < 65536) {
    head = Buffer.alloc(4);
    head[0] = 0x80 | (opcode || 1);
    head[1] = 126;
    head.writeUInt16BE(len, 2);
  } else {
    head = Buffer.alloc(10);
    head[0] = 0x80 | (opcode || 1);
    head[1] = 127;
    head.writeBigUInt64BE(BigInt(len), 2);
  }
  return Buffer.concat([head, data]);
}

class Client {
  constructor(socket) {
    this.socket = socket;
    this.buffer = Buffer.alloc(0);
    this.fragments = [];
    this.fragOpcode = 0;
    this.closed = false;
    this.room = null;
    this.color = null;

    socket.on('data', (chunk) => this.onData(chunk));
    socket.on('error', () => this.close());
    socket.on('close', () => this.onClose());
    socket.on('end', () => this.onClose());
  }

  onData(chunk) {
    if (this.closed) return;
    this.buffer = Buffer.concat([this.buffer, chunk]);
    while (this.buffer.length >= 2) {
      const b0 = this.buffer[0];
      const b1 = this.buffer[1];
      const fin = (b0 & 0x80) !== 0;
      const opcode = b0 & 0x0f;
      const masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f;
      let offset = 2;

      if (len === 126) {
        if (this.buffer.length < 4) return;
        len = this.buffer.readUInt16BE(2);
        offset = 4;
      } else if (len === 127) {
        if (this.buffer.length < 10) return;
        const big = this.buffer.readBigUInt64BE(2);
        if (big > BigInt(MAX_PAYLOAD)) { this.close(); return; }
        len = Number(big);
        offset = 10;
      }
      if (len > MAX_PAYLOAD) { this.close(); return; }

      let maskKey = null;
      if (masked) {
        if (this.buffer.length < offset + 4) return;
        maskKey = this.buffer.slice(offset, offset + 4);
        offset += 4;
      }
      if (this.buffer.length < offset + len) return;

      let payload = this.buffer.slice(offset, offset + len);
      this.buffer = this.buffer.slice(offset + len);
      if (masked) {
        const out = Buffer.alloc(len);
        for (let i = 0; i < len; i++) out[i] = payload[i] ^ maskKey[i % 4];
        payload = out;
      }

      if (opcode === 0x8) { this.close(); return; }
      if (opcode === 0x9) { this.sendFrame(payload, 0xA); continue; }
      if (opcode === 0xA) { continue; }

      if (opcode === 0x0) {
        this.fragments.push(payload);
      } else {
        this.fragments = [payload];
        this.fragOpcode = opcode;
      }
      if (!fin) continue;

      const full = Buffer.concat(this.fragments);
      this.fragments = [];
      if (this.fragOpcode === 0x1) {
        this.onText(full.toString('utf8'));
      }
    }
  }

  onText(text) {
    let msg;
    try { msg = JSON.parse(text); } catch (e) { return; }
    if (!msg || typeof msg.type !== 'string') return;
    handleMessage(this, msg);
  }

  send(obj) {
    this.sendFrame(Buffer.from(JSON.stringify(obj), 'utf8'), 0x1);
  }

  sendFrame(payload, opcode) {
    if (this.closed) return;
    try { this.socket.write(encodeFrame(payload, opcode)); } catch (e) { this.close(); }
  }

  ping() {
    if (this.closed) return;
    try { this.socket.write(encodeFrame(Buffer.alloc(0), 0x9)); } catch (e) {}
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    try { this.socket.end(); } catch (e) {}
    this.onClose();
  }

  onClose() {
    if (this.cleaned) return;
    this.cleaned = true;
    leaveRoom(this);
  }
}

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  const upgrade = (req.headers['upgrade'] || '').toLowerCase();
  if (!key || upgrade !== 'websocket') {
    socket.write('HTTP/1.1 400 Bad Request\r\n\r\n');
    socket.destroy();
    return;
  }
  const accept = crypto.createHash('sha1').update(key + WS_GUID).digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
    'Upgrade: websocket\r\n' +
    'Connection: Upgrade\r\n' +
    'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
  );
  new Client(socket);
});

// ============================================================
// 房间逻辑
// ============================================================
const rooms = new Map(); // code -> { code, red, black, createdAt }
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

function makeCode() {
  for (let attempt = 0; attempt < 50; attempt++) {
    let code = '';
    const bytes = crypto.randomBytes(6);
    for (let i = 0; i < 6; i++) code += CODE_CHARS[bytes[i] % CODE_CHARS.length];
    if (!rooms.has(code)) return code;
  }
  return String(Date.now()).slice(-6);
}

function newRoom(client) {
  const code = makeCode();
  const room = { code, red: client, black: null, createdAt: Date.now(), started: false };
  rooms.set(code, room);
  client.room = room;
  client.color = 'red';
  return room;
}

function opponentOf(client) {
  if (!client.room) return null;
  return client.color === 'red' ? client.room.black : client.room.red;
}

function leaveRoom(client) {
  const room = client.room;
  if (!room) return;
  client.room = null;
  const wasColor = client.color;
  client.color = null;

  if (rooms.get(room.code) !== room) return;

  if (wasColor === 'red' || room.started) {
    // 房主离开或对局已开始：房间结束
    rooms.delete(room.code);
    const other = wasColor === 'red' ? room.black : room.red;
    if (other && other !== client) {
      other.room = null;
      other.color = null;
      other.send({
        type: 'peer-left',
        message: wasColor === 'red' ? '房主已离开，房间结束' : '对方已离开，对局结束'
      });
    }
  } else {
    // 加入者在对局开始前离开：保留房间，房主可以继续等新对手
    room.black = null;
    if (room.red && room.red !== client) {
      room.red.send({ type: 'peer-left', message: '对方已离开' });
    }
  }
}

function handleMessage(client, msg) {
  const type = msg.type;

  if (type === 'ping') {
    client.send({ type: 'pong' });
    return;
  }

  if (type === 'create') {
    if (client.room) leaveRoom(client);
    const room = newRoom(client);
    client.send({ type: 'created', code: room.code, color: 'red' });
    return;
  }

  if (type === 'join') {
    const code = String(msg.code || '').trim().toUpperCase();
    if (!/^[A-Z0-9]{4,8}$/.test(code)) {
      client.send({ type: 'error', message: '房号格式不正确' });
      return;
    }
    const room = rooms.get(code);
    if (!room) {
      client.send({ type: 'error', message: '房间不存在或已结束' });
      return;
    }
    if (client.room === room) return;
    if (room.black) {
      client.send({ type: 'error', message: '房间已满' });
      return;
    }
    if (client.room) leaveRoom(client);
    room.black = client;
    client.room = room;
    client.color = 'black';
    room.started = true;

    client.send({ type: 'joined', code: room.code, color: 'black' });
    if (room.red) room.red.send({ type: 'ready', message: '对方已加入' });
    if (room.red) room.red.send({ type: 'start', code: room.code, color: 'red' });
    client.send({ type: 'start', code: room.code, color: 'black' });
    return;
  }

  if (type === 'move') {
    if (!client.room || !opponentOf(client)) return;
    const m = msg.move || {};
    const nums = [m.fromRow, m.fromCol, m.toRow, m.toCol];
    if (!nums.every((n) => Number.isInteger(n))) return;
    const opp = opponentOf(client);
    if (opp) opp.send({ type: 'move', move: {
      fromRow: m.fromRow, fromCol: m.fromCol, toRow: m.toRow, toCol: m.toCol
    } });
    return;
  }

  if (type === 'restart') {
    const room = client.room;
    if (!room) return;
    if (room.red) room.red.send({ type: 'restart' });
    if (room.black) room.black.send({ type: 'restart' });
    return;
  }

  if (type === 'leave') {
    leaveRoom(client);
    return;
  }
}

// 心跳：每 25 秒 ping 一次，浏览器会自动回 pong
setInterval(() => {
  rooms.forEach((room) => {
    if (room.red) room.red.ping();
    if (room.black) room.black.ping();
  });
}, 25000).unref();

// 每小时清理超长空房（房主在线也最多保留 12 小时）
setInterval(() => {
  const now = Date.now();
  rooms.forEach((room, code) => {
    if (now - room.createdAt > 12 * 60 * 60 * 1000 && !room.black) {
      rooms.delete(code);
      if (room.red) {
        room.red.room = null;
        room.red.color = null;
        room.red.send({ type: 'error', message: '房间已超时关闭' });
      }
    }
  });
}, 60 * 60 * 1000).unref();

server.listen(PORT, '0.0.0.0', () => {
  console.log('周周象棋服务器已启动: http://localhost:' + PORT);
  console.log('联机对战请用部署后的网址打开页面（不要用 file:// 打开）');
});
