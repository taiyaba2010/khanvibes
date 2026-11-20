"use strict";
// Andor room database. Stores named rooms (name, look, bcrypt password hash, owner hash).
// Never stores messages, files or anything said inside a room.
//   DATABASE_URL set  → PostgreSQL (table "rooms", created on boot)
//   otherwise         → JSON file at ROOMS_DB_FILE (default ./data/rooms.json); fine for dev,
//                       but Render's free disk is wiped on every deploy, so use Postgres in prod.
const fs = require("fs");
const path = require("path");

function rowToRoom(r) {
  if (!r) return null;
  return {
    name: r.name,
    emoji: r.emoji,
    color: r.color,
    description: r.description || "",
    listed: !!r.listed,
    passwordHash: r.password_hash,
    ownerHash: r.owner_hash || null,
    createdAt: Number(r.created_at),
    lastActiveAt: Number(r.last_active_at),
  };
}

class PgStore {
  constructor(url) {
    const { Pool } = require("pg");
    const ssl = /sslmode=disable/.test(url) || /localhost|127\.0\.0\.1/.test(url) ? false : { rejectUnauthorized: false };
    this.pool = new Pool({ connectionString: url, ssl, max: 5 });
    // Neon/managed Postgres closes idle connections; log instead of crashing the process.
    this.pool.on("error", (err) => console.error("[db] idle client error:", err.message));
    this.kind = "postgres";
  }
  async init() {
    await this.pool.query(`CREATE TABLE IF NOT EXISTS rooms (
      name TEXT PRIMARY KEY,
      emoji TEXT NOT NULL DEFAULT '💬',
      color TEXT NOT NULL DEFAULT 'violet',
      description TEXT NOT NULL DEFAULT '',
      listed BOOLEAN NOT NULL DEFAULT TRUE,
      password_hash TEXT NOT NULL,
      owner_hash TEXT,
      created_at BIGINT NOT NULL,
      last_active_at BIGINT NOT NULL
    )`);
    await this.pool.query(`CREATE INDEX IF NOT EXISTS rooms_listed_idx ON rooms (listed, last_active_at DESC)`);
  }
  async get(name) {
    const { rows } = await this.pool.query("SELECT * FROM rooms WHERE name=$1", [name]);
    return rowToRoom(rows[0]);
  }
  async insert(r) {
    try {
      await this.pool.query(
        `INSERT INTO rooms (name, emoji, color, description, listed, password_hash, owner_hash, created_at, last_active_at)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [r.name, r.emoji, r.color, r.description, r.listed, r.passwordHash, r.ownerHash, r.createdAt, r.lastActiveAt],
      );
      return true;
    } catch (e) {
      if (e && e.code === "23505") return false; // unique violation
      throw e;
    }
  }
  async update(name, patch) {
    const map = { emoji: "emoji", color: "color", description: "description", listed: "listed", passwordHash: "password_hash", lastActiveAt: "last_active_at" };
    const sets = []; const vals = [];
    for (const [k, col] of Object.entries(map)) if (patch[k] !== undefined) { vals.push(patch[k]); sets.push(`${col}=$${vals.length}`); }
    if (!sets.length) return;
    vals.push(name);
    await this.pool.query(`UPDATE rooms SET ${sets.join(",")} WHERE name=$${vals.length}`, vals);
  }
  async remove(name) { await this.pool.query("DELETE FROM rooms WHERE name=$1", [name]); }
  async listListed(limit = 200) {
    const { rows } = await this.pool.query("SELECT * FROM rooms WHERE listed ORDER BY last_active_at DESC LIMIT $1", [limit]);
    return rows.map(rowToRoom);
  }
  async listByOwner(ownerHash) {
    const { rows } = await this.pool.query("SELECT * FROM rooms WHERE owner_hash=$1 ORDER BY created_at DESC", [ownerHash]);
    return rows.map(rowToRoom);
  }
  async purgeInactive(before) {
    const { rowCount } = await this.pool.query("DELETE FROM rooms WHERE last_active_at < $1", [before]);
    return rowCount;
  }
}

class FileStore {
  constructor(file) {
    this.file = file;
    this.kind = "file";
    this.rooms = new Map();
    this.writing = Promise.resolve();
  }
  async init() {
    fs.mkdirSync(path.dirname(this.file), { recursive: true });
    if (fs.existsSync(this.file)) {
      const arr = JSON.parse(fs.readFileSync(this.file, "utf8") || "[]");
      for (const r of arr) this.rooms.set(r.name, r);
    }
  }
  flush() {
    const data = JSON.stringify([...this.rooms.values()]);
    this.writing = this.writing.then(() => {
      const tmp = this.file + ".tmp";
      fs.writeFileSync(tmp, data);
      fs.renameSync(tmp, this.file);
    }).catch((e) => console.error("rooms db write failed", e));
    return this.writing;
  }
  async get(name) { const r = this.rooms.get(name); return r ? { ...r } : null; }
  async insert(r) { if (this.rooms.has(r.name)) return false; this.rooms.set(r.name, { ...r }); await this.flush(); return true; }
  async update(name, patch) {
    const r = this.rooms.get(name); if (!r) return;
    for (const [k, v] of Object.entries(patch)) if (v !== undefined) r[k] = v;
    await this.flush();
  }
  async remove(name) { this.rooms.delete(name); await this.flush(); }
  async listListed(limit = 200) {
    return [...this.rooms.values()].filter((r) => r.listed).sort((a, b) => b.lastActiveAt - a.lastActiveAt).slice(0, limit).map((r) => ({ ...r }));
  }
  async listByOwner(ownerHash) {
    return [...this.rooms.values()].filter((r) => r.ownerHash && r.ownerHash === ownerHash).sort((a, b) => b.createdAt - a.createdAt).map((r) => ({ ...r }));
  }
  async purgeInactive(before) {
    let n = 0;
    for (const [k, r] of this.rooms) if (r.lastActiveAt < before) { this.rooms.delete(k); n++; }
    if (n) await this.flush();
    return n;
  }
}

function createStore() {
  if (process.env.DATABASE_URL) return new PgStore(process.env.DATABASE_URL);
  return new FileStore(process.env.ROOMS_DB_FILE || path.join(__dirname, "data", "rooms.json"));
}

module.exports = { createStore };
