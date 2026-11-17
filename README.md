# Connect

Private, ephemeral, two-person rooms for Android: text chat, file sharing, and audio/video calls over a **direct WebRTC peer-to-peer** connection. Rooms are joined with a code + password. No accounts, no cloud history, nothing persisted.

- Android package: `com.khanvibes.app` (app label **Connect**)
- Signaling API: `https://api.khanvibes.com` / `wss://api.khanvibes.com` (Render service `khanvibes-signaling`)
- TURN: `turn:turn.khanvibes.com:3478` (Coturn on a VPS) or Metered.ca fallback

## Architecture

```
Android A ─┐
           ├─ Socket.IO (wss://api.khanvibes.com) ─► Signaling server (Node.js 20 on Render)
Android B ─┘        open-room / join-room / offer / answer / ice / peer-joined / peer-left / room-expired
Peer A ◄════════════ DIRECT P2P (SRTP + SCTP DataChannels) ════════════► Peer B
         "chat" DataChannel          UTF-8 JSON control frames
         "file-transfer" DataChannel binary, 16 KB chunks, ordered + reliable
         MediaStream tracks          audio + video
   STUN stun:stun.l.google.com:19302   ·   TURN turn:turn.khanvibes.com:3478
```

The signaling server only relays SDP/ICE between the two sockets of a room. It stores a bcrypt hash of the room password in memory, caps rooms at 2 peers, and deletes a room after 10 minutes idle (`room-expired`) or when the last peer leaves. Messages and files never touch it.

### Repo layout

| Path | What |
|---|---|
| `signaling/` | `server.js` + `package.json` (Socket.IO 4, bcrypt) |
| `android/` | Single-module Compose app (Gradle 8.9, AGP 8.7.3, Kotlin 2.0.21, Hilt, KSP) |
| `scripts/deploy-api.sh` | PART 2: GitHub push → Render service → custom domain → Cloudflare CNAME → TLS/WSS checks → `dist/deploy-manifest.json` |
| `scripts/verify-wss.js` | WSS handshake check (`node scripts/verify-wss.js [wssUrl]`) |
| `scripts/test-signaling.js` | Local protocol test for the signaling server |
| `scripts/create-turn-dns.sh` | Cloudflare A record `turn` → `$VPS_IP` (proxied off) |
| `scripts/setup-turn.sh` | Coturn install + TLS on the VPS (run as root on the VPS) |
| `scripts/gen-keystore.sh` | Release keystore + `android/keystore.properties` |
| `scripts/build-apk.sh` | Signed release APK → `dist/connect-v1.0.0.apk` |
| `scripts/sandbox-gradle.sh` | Only for aarch64 build hosts behind an auth proxy (see "Building on aarch64") |

### DataChannel protocol

Control frames on `chat` (all 8 handled on receive in `RoomViewModel.onChat`):

```json
{ "type": "msg",          "id": "uuid", "text": "...", "ts": 1700000000 }
{ "type": "msg_del",      "id": "uuid" }
{ "type": "msg_del_all" }
{ "type": "typing",       "on": true }
{ "type": "file_offer",   "id": "uuid", "name": "...", "size": 1234, "mime": "..." }
{ "type": "file_accept",  "id": "uuid" }
{ "type": "file_complete","id": "uuid" }
{ "type": "file_del",     "id": "uuid" }
```

Binary frames on `file-transfer`: `[int32 fileIdLen][fileId UTF-8][int32 chunkIndex][bytes]`, 16 KB payloads. Received files go to `cacheDir/room_{id}/files/{uuid}_{sanitized_name}`.

### Ephemerality

- Messages: RAM only (`EphemeralMessageStore`, a `StateFlow`). No Room, DataStore or SharedPreferences anywhere in the app.
- Files: `cacheDir/room_{sessionId}/` only (`EphemeralFileStore`).
- Wiped on: `ConnectApp.onCreate()` (`wipeAllRooms()`), `MainActivity.onStop()` (app backgrounded), `RoomViewModel.onCleared()`, ICE `CLOSED`, a second ICE `DISCONNECTED`/`FAILED` (after one ICE restart attempt, 15 s timeout), peer leaving, and "Leave room".
- Deletion is synchronized: delete locally (optimistic) and send `msg_del` / `file_del` / `msg_del_all`; the peer applies the same delete.
- `FLAG_SECURE` blocks screenshots, screen recording and the recents thumbnail. `allowBackup=false` and backup exclusion rules are set.
- One deliberate exception to the background wipe: when Connect itself launches the file picker, the share sheet, or "open file", that one `onStop` is skipped so the session survives the action the user asked for.

## Prerequisites

- **Node.js 20+** and npm (signaling server, scripts)
- **JDK 17** (JDK 21 also works for the build)
- **Android SDK** with `platforms;android-35`, `build-tools;35.0.0`, `platform-tools` (set `ANDROID_HOME` or `android/local.properties` `sdk.dir=`)
- `curl`, `jq`, `git`, `openssl`, `keytool` for the deploy/keystore scripts

## 1. Signaling server (PART 1)

```bash
cd signaling && npm install && npm start &        # listens on $PORT or 3000
sleep 2 && curl -s localhost:3000/health          # → ok
cd .. && npm install && node scripts/test-signaling.js   # protocol test (BAD_PASSWORD, ROOM_FULL, relay…)
```

## 2. Deploy `api.khanvibes.com` (PART 2)

Export these, then run the idempotent script (it stops if any are missing and never invents values):

```bash
export CF_API_TOKEN=…      # Cloudflare token, Zone.DNS Edit on khanvibes.com
export CF_ZONE_ID=…        # zone ID of khanvibes.com
export RENDER_API_KEY=…    # Render API key
export RENDER_OWNER_ID=…   # Render workspace owner ID
export GITHUB_REPO_URL=https://github.com/<user>/<repo>
export GITHUB_TOKEN=…      # PAT with repo scope
export TURN_PASSWORD=…     # optional; generated into .deploy-env if absent
./scripts/deploy-api.sh
```

Steps it runs, stopping on any failure: push `signaling/` to `main` → create the Render web service `khanvibes-signaling` (free plan, region fallback **oregon → frankfurt → singapore**) → wait for the deploy to be `live` → check `<onrender URL>/health` → attach `api.khanvibes.com` → upsert the Cloudflare **CNAME `api`, `proxied:false`** (grey cloud; proxying breaks the WebSocket upgrade) → wait for `https://api.khanvibes.com/health` = 200 → `node scripts/verify-wss.js` → write `dist/deploy-manifest.json`. State goes in `.deploy-env` (gitignored); re-runs skip what exists. Nothing is ever auto-deleted.

## 3. TURN (PART 3)

**Option A: self-hosted Coturn** (when you have a VPS with a public IPv4):

```bash
export VPS_IP=203.0.113.10
./scripts/create-turn-dns.sh                                   # A record turn → $VPS_IP, proxied:false
scp scripts/setup-turn.sh root@$VPS_IP:/root/
ssh root@$VPS_IP "sudo /root/setup-turn.sh $VPS_IP '$TURN_PASSWORD'"
```

Then add to `android/keystore.properties`:

```properties
[turn]
TURN_URL=turn:turn.khanvibes.com:3478
TURN_USER=kv
TURN_PASS=<TURN_PASSWORD>
```

**Option B: Metered.ca** (no VPS): self-hosting TURN is skipped. Sign up at https://www.metered.ca/tools/openrelay/ (free 50 GB/mo), copy the credentials, and put `TURN_URL`, `TURN_USER`, `TURN_PASS` under a `[turn]` section in `android/keystore.properties`. `TURN_URL` may list several URLs separated by commas. If no TURN is configured, the app is STUN-only, which works for roughly 70% of networks.

## 4. Build (PART 4 + 5)

```bash
./scripts/gen-keystore.sh        # android/app/keystore.jks + android/keystore.properties (keeps an existing [turn] section)
cd android && ./gradlew assembleDebug && cd ..
./scripts/build-apk.sh           # clean assembleRelease → dist/connect-v1.0.0.apk + SHA-256
```

Build-time config (`android/app/build.gradle.kts`):

- `BuildConfig.SIGNALING_URL` comes from `dist/deploy-manifest.json` → `signalingUrl`. If the file is missing, the build logs a warning and falls back to `https://api.khanvibes.com`.
- `BuildConfig.STUN_URL = "stun:stun.l.google.com:19302"`; `TURN_URL/TURN_USER/TURN_PASS` from `keystore.properties` (blank if absent).
- Release signing comes from `keystore.properties` (`storeFile`, `storePassword`, `keyAlias`, `keyPassword`). Without it, the build falls back to debug signing.
- Release is built unminified. ABIs: arm64-v8a, armeabi-v7a, x86_64.

### Building on aarch64 Linux

AGP's `aapt2` is x86_64-only. `scripts/sandbox-gradle.sh` was used to build this repo on an aarch64 sandbox. It runs Google's aapt2 under `qemu-x86_64` (`-Pandroid.aapt2FromMavenOverride=…`) and maps `$HTTPS_PROXY` to JVM proxy properties. On a normal x86_64 machine or a Mac, use `./gradlew` directly.

## Security notes

- **Deletion is best-effort within the live session.** "Delete for both" and "Clear all" remove data from both devices only while the peer is running an honest copy of Connect and is connected. Anything the other person has already seen, photographed with another device, screen-recorded on a rooted or modified phone, or opened in another app (exported) **cannot be recalled**. `FLAG_SECURE` only blocks normal screenshots and recordings on stock Android.
- Media and DataChannels are encrypted end to end by WebRTC (DTLS-SRTP / DTLS-SCTP). When a TURN relay is used, it forwards encrypted packets and cannot read them.
- Room passwords are only bcrypt-hashed in the signaling server's memory. A 6-character password is meant for a room that lives a few minutes, not for long-term secrecy. Share code and password over a channel you trust.
- **Static TURN credentials are compiled into the APK** (`BuildConfig.TURN_PASS`) and can be extracted. **Rotate them for production**, preferably by switching Coturn to `use-auth-secret` with short-lived HMAC credentials issued by the signaling server.
- Keep `android/keystore.properties`, `android/app/keystore.jks` and `.deploy-env` out of git (already in `.gitignore`) and back them up somewhere safe. Losing the keystore means you can't ship updates to installed apps.
- A peer whose signaling socket drops ends the session for both sides (privacy-first). The signaling room's 10-minute idle expiry does not end a session that is already connected peer to peer.
