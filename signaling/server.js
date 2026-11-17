"use strict";
// Connect signaling server (Render service: khanvibes-signaling)
// Stateless relay for SDP/ICE between exactly two peers per room. Nothing is persisted.
const http = require("http");
const { Server } = require("socket.io");
const bcrypt = require("bcrypt");

const PORT = Number(process.env.PORT) || 3000;
const ROOM_TTL_MS = 10 * 60 * 1000; // 10 min idle
const MAX_PEERS = 2;

const server = http.createServer((req, res) => {
  if (req.method === "GET" && req.url.split("?")[0] === "/health") {
    res.writeHead(200, { "Content-Type": "text/plain" });
    res.end("ok");
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

/** @type {Map<string, {hash: string, peers: Set<string>, timer: NodeJS.Timeout|null}>} */
const rooms = new Map();

function ack(cb, payload) {
  if (typeof cb === "function") cb(payload);
}

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
  }, ROOM_TTL_MS);
  if (typeof room.timer.unref === "function") room.timer.unref();
}

function deleteRoom(roomId) {
  const room = rooms.get(roomId);
  if (!room) return;
  if (room.timer) clearTimeout(room.timer);
  rooms.delete(roomId);
}

io.on("connection", (socket) => {
  socket.on("open-room", async (data, cb) => {
    try {
      const roomId = data && typeof data.roomId === "string" ? data.roomId : "";
      const password = data && typeof data.password === "string" ? data.password : "";
      if (roomId.length < 6) return ack(cb, { ok: false, error: "INVALID_ROOM" });
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
      if (!room) return ack(cb, { ok: false, error: "NOT_FOUND" });
      if (room.peers.size >= MAX_PEERS) return ack(cb, { ok: false, error: "ROOM_FULL" });
      const match = await bcrypt.compare(password, room.hash);
      if (!match) return ack(cb, { ok: false, error: "BAD_PASSWORD" });
      // re-check after async compare
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

  socket.on("disconnect", () => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);
    if (!room) return;
    room.peers.delete(socket.id);
    socket.to(roomId).emit("peer-left", { peerId: socket.id });
    if (room.peers.size === 0) deleteRoom(roomId);
    else touchRoom(roomId);
  });
});

server.listen(PORT, () => {
  console.log(`signaling listening on :${PORT}`);
});

module.exports = { server, io, rooms };
