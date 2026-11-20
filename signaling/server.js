"use strict";
// Andor signaling server (Render service: khanvibes-signaling)
// Relays SDP/ICE between exactly two peers per live session. Messages and files never touch it.
//
// v1 events (unchanged): open-room, join-room, offer, answer, ice, peer-joined, peer-left, room-expired
// v2 events (named rooms, stored in the rooms database — see db.js):
//   lobby:subscribe              → ack {ok, rooms}; then "lobby:update" {rooms} pushes on every change
//   room:check   {name}          → ack {ok, valid, available}
//   room:info    {name}          → ack {ok, room}
//   room:create  {name, password, emoji, color, description, listed, ownerToken} → ack {ok, room}
//   room:enter   {name, password}→ ack {ok, isInitiator}  (initiator waits for peer-joined, as with open-room)
//   room:change-password {name, oldPassword, newPassword} → ack {ok}
//   room:update  {name, password|ownerToken, listed?, emoji?, color?, description?} → ack {ok, room}
//   room:delete  {name, password|ownerToken} → ack {ok}
//   room:mine    {ownerToken}    → ack {ok, rooms}
const http = require("http");
const crypto = require("crypto");
const { Server } = require("socket.io");
const bcrypt = require("bcrypt");
const { createStore } = require("./db");

const PORT = Number(process.env.PORT) || 3000;
const ROOM_TTL_MS = 10 * 60 * 1000; // 10 min idle (live session only; named rooms stay in the database)
const MAX_PEERS = 2;
const RETENTION_DAYS = Number(process.env.ROOM_RETENTION_DAYS) || 30;
const NAMED_PREFIX = "named:";
const NAME_RE = /^[a-z0-9][a-z0-9_-]{2,31}$/;
const EMOJIS = new Set(["💬", "🌙", "🎧", "☕", "🎮", "📚", "🔥", "🌸", "🚀", "🎨", "⚽", "🍕", "🌊", "🦉", "💡", "🎬"]);
const COLORS = new Set(["violet", "cyan", "orange", "pink", "green", "blue", "red", "yellow"]);

const store = createStore();

/** @type {Map<string, {hash: string|null, peers: Set<string>, timer: NodeJS.Timeout|null, named?: string}>} */
const rooms = new Map();

// ── helpers ───────────────────────────────────────────────────────────────
function ack(cb, payload) { if (typeof cb === "function") cb(payload); }
const str = (v, max = 200) => (typeof v === "string" ? v.slice(0, max) : "");
const normName = (v) => str(v, 64).trim().toLowerCase();
const sha = (s) => crypto.createHash("sha256").update(s).digest("hex");
const validPassword = (p) => typeof p === "string" && p.length >= 4 && p.length <= 64;

function occupancy(name) {
  const live = rooms.get(NAMED_PREFIX + name);
  return live ? live.peers.size : 0;
}
function publicRoom(r, ownerHash) {
  return {
    name: r.name,
    emoji: r.emoji,
    color: r.color,
    description: r.description || "",
    listed: !!r.listed,
    occupancy: occupancy(r.name),
    capacity: MAX_PEERS,
    createdAt: r.createdAt,
    lastActiveAt: r.lastActiveAt,
    mine: !!(ownerHash && r.ownerHash && ownerHash === r.ownerHash),
  };
}

// Simple attempt limiter for password checks: 6 failures per (ip+room) per 5 min.
const failures = new Map();
function limited(key) {
  const f = failures.get(key);
  return !!(f && f.count >= 6 && Date.now() - f.first < 5 * 60 * 1000);
}
function recordFailure(key) {
  const now = Date.now();
  const f = failures.get(key);
  if (!f || now - f.first > 5 * 60 * 1000) failures.set(key, { count: 1, first: now });
  else f.count++;
}
setInterval(() => {
  const now = Date.now();
  for (const [k, f] of failures) if (now - f.first > 5 * 60 * 1000) failures.delete(k);
}, 60 * 1000).unref();

async function authorize(room, data) {
  const token = str(data && data.ownerToken, 128);
  if (token && room.ownerHash && sha(token) === room.ownerHash) return true;
  const pw = data && typeof data.password === "string" ? data.password : "";
  return pw ? bcrypt.compare(pw, room.passwordHash) : false;
}

// ── HTTP ──────────────────────────────────────────────────────────────────
const server = http.createServer(async (req, res) => {
  const p = req.url.split("?")[0];
  if (req.method === "GET" && p === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
    return;
  }
  if (req.method === "GET" && p === "/rooms") {
    try {
      const list = (await store.listListed()).map((r) => publicRoom(r));
      res.writeHead(200, { "Content-Type": "application/json", "Access-Control-Allow-Origin": "*" });
      res.end(JSON.stringify({ rooms: list }));
    } catch (e) {
      res.writeHead(500, { "Content-Type": "text/plain" });
      res.end("error");
    }
    return;
  }
  res.writeHead(404, { "Content-Type": "text/plain" });
  res.end("not found");
});

const io = new Server(server, {
  cors: { origin: "*" },
  pingTimeout: 20000,
  maxHttpBufferSize: 1e6,
});

// ── lobby broadcast (debounced) ───────────────────────────────────────────
let lobbyTimer = null;
function lobbyChanged() {
  if (lobbyTimer) return;
  lobbyTimer = setTimeout(async () => {
    lobbyTimer = null;
    try {
      const list = (await store.listListed()).map((r) => publicRoom(r));
      io.to("lobby").emit("lobby:update", { rooms: list });
    } catch (e) { console.error("lobby update failed", e); }
  }, 250);
}

// ── live sessions ─────────────────────────────────────────────────────────
function touchRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  if (room.timer) clearTimeout(room.timer);
  room.timer = setTimeout(() => {
    const r = rooms.get(roomId);
    if (!r) return;
    io.to(roomId).emit("room-expired", { roomId });
    for (const sid of r.peers) {
      const s = io.sockets.sockets.get(sid);
      if (s) {
        s.leave(roomId);
        s.data.roomId = undefined;
      }
    }
    rooms.delete(roomId);
    if (r.named) lobbyChanged();
  }, ROOM_TTL_MS);
  if (typeof room.timer.unref === "function") room.timer.unref();
}

function deleteRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  if (room.timer) clearTimeout(room.timer);
  rooms.delete(roomId);
}

function leaveCurrent(socket) {
  const roomId = socket.data.roomId;
  const room = roomId && rooms.get(roomId);
  if (!room) return;
  room.peers.delete(socket.id);
  socket.leave(roomId);
  socket.data.roomId = undefined;
  socket.to(roomId).emit("peer-left", { peerId: socket.id });
  if (room.peers.size === 0) deleteRoom(roomId);
  else touchRoom(roomId);
  if (room.named) lobbyChanged();
}

io.on("connection", (socket) => {
  const ip = (socket.handshake.headers["x-forwarded-for"] || socket.handshake.address || "").toString().split(",")[0].trim();

  // ── v1: code rooms (unchanged behaviour) ──
  socket.on("open-room", async (data, cb) => {
    try {
      const roomId = data && typeof data.roomId === "string" ? data.roomId : "";
      const password = data && typeof data.password === "string" ? data.password : "";
      if (roomId.length < 6 || roomId.startsWith(NAMED_PREFIX)) return ack(cb, { ok: false, error: "INVALID_ROOM" });
      if (!password) return ack(cb, { ok: false, error: "INVALID_PASSWORD" });
      if (rooms.has(roomId)) return ack(cb, { ok: false, error: "ROOM_EXISTS" });
      const hash = await bcrypt.hash(password, 10);
      if (rooms.has(roomId)) return ack(cb, { ok: false, error: "ROOM_EXISTS" });
      rooms.set(roomId, { hash, peers: new Set([socket.id]), timer: null });
      socket.join(roomId);
      socket.data.roomId = roomId;
      touchRoom(roomId);
      ack(cb, { ok: true, isInitiator: true });
    } catch (e) {
      ack(cb, { ok: false, error: "SERVER_ERROR" });
    }
  });

  socket.on("join-room", async (data, cb) => {
    try {
      const roomId = data && typeof data.roomId === "string" ? data.roomId : "";
      const password = data && typeof data.password === "string" ? data.password : "";
      const room = rooms.get(roomId);
      if (!room || room.named) return ack(cb, { ok: false, error: "NOT_FOUND" });
      if (room.peers.size >= MAX_PEERS) return ack(cb, { ok: false, error: "ROOM_FULL" });
      const match = await bcrypt.compare(password, room.hash);
      if (!match) return ack(cb, { ok: false, error: "BAD_PASSWORD" });
      const current = rooms.get(roomId);
      if (!current) return ack(cb, { ok: false, error: "NOT_FOUND" });
      if (current.peers.size >= MAX_PEERS) return ack(cb, { ok: false, error: "ROOM_FULL" });
      current.peers.add(socket.id);
      socket.join(roomId);
      socket.data.roomId = roomId;
      touchRoom(roomId);
      socket.to(roomId).emit("peer-joined", { peerId: socket.id });
      ack(cb, { ok: true, isInitiator: false });
    } catch (e) {
      ack(cb, { ok: false, error: "SERVER_ERROR" });
    }
  });

  // ── v2: lobby + named rooms ──
  socket.on("lobby:subscribe", async (data, cb) => {
    try {
      socket.join("lobby");
      const token = str(data && data.ownerToken, 128);
      const oh = token ? sha(token) : null;
      const list = (await store.listListed()).map((r) => publicRoom(r, oh));
      const mine = oh ? (await store.listByOwner(oh)).map((r) => publicRoom(r, oh)) : [];
      ack(cb, { ok: true, rooms: list, mine });
    } catch (e) { ack(cb, { ok: false, error: "SERVER_ERROR" }); }
  });
  socket.on("lobby:unsubscribe", (_d, cb) => { socket.leave("lobby"); ack(cb, { ok: true }); });

  socket.on("room:check", async (data, cb) => {
    try {
      const name = normName(data && data.name);
      if (!NAME_RE.test(name)) return ack(cb, { ok: true, valid: false, available: false });
      ack(cb, { ok: true, valid: true, available: !(await store.get(name)) });
    } catch (e) { ack(cb, { ok: false, error: "SERVER_ERROR" }); }
  });

  socket.on("room:info", async (data, cb) => {
    try {
      const name = normName(data && data.name);
      const r = NAME_RE.test(name) ? await store.get(name) : null;
      if (!r) return ack(cb, { ok: false, error: "NOT_FOUND" });
      const token = str(data && data.ownerToken, 128);
      ack(cb, { ok: true, room: publicRoom(r, token ? sha(token) : null) });
    } catch (e) { ack(cb, { ok: false, error: "SERVER_ERROR" }); }
  });

  socket.on("room:create", async (data, cb) => {
    try {
      const name = normName(data && data.name);
      const password = data && data.password;
      if (!NAME_RE.test(name)) return ack(cb, { ok: false, error: "INVALID_NAME" });
      if (!validPassword(password)) return ack(cb, { ok: false, error: "INVALID_PASSWORD" });
      const emoji = EMOJIS.has(data.emoji) ? data.emoji : "💬";
      const color = COLORS.has(data.color) ? data.color : "violet";
      const token = str(data.ownerToken, 128);
      const now = Date.now();
      const room = {
        name, emoji, color,
        description: str(data.description, 80).trim(),
        listed: data.listed !== false,
        passwordHash: await bcrypt.hash(password, 10),
        ownerHash: token ? sha(token) : null,
        createdAt: now,
        lastActiveAt: now,
      };
      if (!(await store.insert(room))) return ack(cb, { ok: false, error: "NAME_TAKEN" });
      lobbyChanged();
      ack(cb, { ok: true, room: publicRoom(room, room.ownerHash) });
    } catch (e) {
      console.error("room:create", e);
      ack(cb, { ok: false, error: "SERVER_ERROR" });
    }
  });

  socket.on("room:enter", async (data, cb) => {
    try {
      const name = normName(data && data.name);
      const password = data && typeof data.password === "string" ? data.password : "";
      const key = ip + "|" + name;
      if (limited(key)) return ack(cb, { ok: false, error: "TOO_MANY_ATTEMPTS" });
      const r = NAME_RE.test(name) ? await store.get(name) : null;
      if (!r) return ack(cb, { ok: false, error: "NOT_FOUND" });
      const liveId = NAMED_PREFIX + name;
      if (occupancy(name) >= MAX_PEERS) return ack(cb, { ok: false, error: "ROOM_FULL" });
      if (!(await bcrypt.compare(password, r.passwordHash))) {
        recordFailure(key);
        return ack(cb, { ok: false, error: "BAD_PASSWORD" });
      }
      if (socket.data.roomId) leaveCurrent(socket);
      let live = rooms.get(liveId);
      let isInitiator;
      if (!live) {
        live = { hash: null, peers: new Set([socket.id]), timer: null, named: name };
        rooms.set(liveId, live);
        isInitiator = true;
      } else {
        if (live.peers.size >= MAX_PEERS) return ack(cb, { ok: false, error: "ROOM_FULL" });
        live.peers.add(socket.id);
        isInitiator = false;
      }
      socket.join(liveId);
      socket.data.roomId = liveId;
      touchRoom(liveId);
      if (!isInitiator) socket.to(liveId).emit("peer-joined", { peerId: socket.id });
      store.update(name, { lastActiveAt: Date.now() }).catch(() => {});
      lobbyChanged();
      ack(cb, { ok: true, isInitiator });
    } catch (e) {
      console.error("room:enter", e);
      ack(cb, { ok: false, error: "SERVER_ERROR" });
    }
  });

  socket.on("room:change-password", async (data, cb) => {
    try {
      const name = normName(data && data.name);
      const key = ip + "|" + name;
      if (limited(key)) return ack(cb, { ok: false, error: "TOO_MANY_ATTEMPTS" });
      const r = NAME_RE.test(name) ? await store.get(name) : null;
      if (!r) return ack(cb, { ok: false, error: "NOT_FOUND" });
      if (!validPassword(data.newPassword)) return ack(cb, { ok: false, error: "INVALID_PASSWORD" });
      const ok = await authorize(r, { password: data.oldPassword, ownerToken: data.ownerToken });
      if (!ok) { recordFailure(key); return ack(cb, { ok: false, error: "BAD_PASSWORD" }); }
      await store.update(name, { passwordHash: await bcrypt.hash(data.newPassword, 10) });
      ack(cb, { ok: true });
    } catch (e) { ack(cb, { ok: false, error: "SERVER_ERROR" }); }
  });

  socket.on("room:update", async (data, cb) => {
    try {
      const name = normName(data && data.name);
      const r = NAME_RE.test(name) ? await store.get(name) : null;
      if (!r) return ack(cb, { ok: false, error: "NOT_FOUND" });
      if (!(await authorize(r, data))) return ack(cb, { ok: false, error: "FORBIDDEN" });
      const patch = {};
      if (typeof data.listed === "boolean") patch.listed = data.listed;
      if (EMOJIS.has(data.emoji)) patch.emoji = data.emoji;
      if (COLORS.has(data.color)) patch.color = data.color;
      if (typeof data.description === "string") patch.description = str(data.description, 80).trim();
      await store.update(name, patch);
      lobbyChanged();
      const token = str(data.ownerToken, 128);
      ack(cb, { ok: true, room: publicRoom({ ...r, ...patch }, token ? sha(token) : null) });
    } catch (e) { ack(cb, { ok: false, error: "SERVER_ERROR" }); }
  });

  socket.on("room:delete", async (data, cb) => {
    try {
      const name = normName(data && data.name);
      const r = NAME_RE.test(name) ? await store.get(name) : null;
      if (!r) return ack(cb, { ok: false, error: "NOT_FOUND" });
      if (!(await authorize(r, data))) return ack(cb, { ok: false, error: "FORBIDDEN" });
      await store.remove(name);
      const liveId = NAMED_PREFIX + name;
      const live = rooms.get(liveId);
      if (live) {
        for (const sid of live.peers) if (sid !== socket.id) io.to(sid).emit("room-expired", { roomId: liveId });
      }
      lobbyChanged();
      ack(cb, { ok: true });
    } catch (e) { ack(cb, { ok: false, error: "SERVER_ERROR" }); }
  });

  socket.on("room:mine", async (data, cb) => {
    try {
      const token = str(data && data.ownerToken, 128);
      if (!token) return ack(cb, { ok: true, rooms: [] });
      const oh = sha(token);
      ack(cb, { ok: true, rooms: (await store.listByOwner(oh)).map((r) => publicRoom(r, oh)) });
    } catch (e) { ack(cb, { ok: false, error: "SERVER_ERROR" }); }
  });

  // ── relay (unchanged) ──
  const relay = (event) => (payload, cb) => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);
    if (!room) return ack(cb, { ok: false, error: "NOT_IN_ROOM" });
    touchRoom(roomId);
    for (const sid of room.peers) {
      if (sid !== socket.id) io.to(sid).emit(event, { ...(payload || {}), from: socket.id });
    }
    ack(cb, { ok: true });
  };
  socket.on("offer", relay("offer"));
  socket.on("answer", relay("answer"));
  socket.on("ice", relay("ice"));

  socket.on("disconnect", () => leaveCurrent(socket));
});

// Rooms nobody entered for RETENTION_DAYS are removed, freeing their names.
setInterval(() => {
  store.purgeInactive(Date.now() - RETENTION_DAYS * 86400000)
    .then((n) => { if (n) lobbyChanged(); })
    .catch((e) => console.error("purge failed", e));
}, 60 * 60 * 1000).unref();

const ready = store.init().then(() => {
  server.listen(PORT, () => console.log(`signaling listening on :${PORT} (rooms db: ${store.kind})`));
});
ready.catch((e) => { console.error("database init failed", e); process.exit(1); });

module.exports = { server, io, rooms, store, ready };
