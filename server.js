/*
 * ============================================================
 *  KEEGS CHAT — RELAY SERVER
 *  Node.js / WebSocket
 *  Deploy to Railway.app
 * ============================================================
 *
 *  PROTOCOL (JSON messages over WebSocket)
 *  ----------------------------------------
 *  Client → Server:
 *    { type:"register" }                          → server assigns ID
 *    { type:"ping" }                              → keepalive
 *    { type:"call", to:"123456" }                 → request chat with ID
 *    { type:"accept", to:"123456" }               → accept incoming call
 *    { type:"reject", to:"123456" }               → reject incoming call
 *    { type:"discover" }                          → enter discovery mode
 *    { type:"text", to:"123456", text:"hello" }   → send text message
 *    { type:"audio", to:"123456", data:"base64"  }→ send ADPCM audio chunk
 *    { type:"hangup", to:"123456" }               → end session
 *
 *  Server → Client:
 *    { type:"registered", id:"123456" }           → your assigned ID
 *    { type:"pong" }                              → keepalive reply
 *    { type:"incomingCall", from:"123456" }       → someone wants to chat
 *    { type:"callAccepted", from:"123456" }       → they accepted
 *    { type:"callRejected", from:"123456" }       → they rejected
 *    { type:"discovered", partnerId:"123456" }    → discovery match found
 *    { type:"text", from:"123456", text:"hello" } → incoming text
 *    { type:"audio", from:"123456", data:"base64"}→ incoming audio chunk
 *    { type:"hangup", from:"123456" }             → other party hung up
 *    { type:"error", message:"..." }              → error
 *    { type:"partnerOffline" }                    → target ID not connected
 * ============================================================
 */

const WebSocket = require('ws');

const PORT = process.env.PORT || 3000;
const wss  = new WebSocket.Server({ port: PORT });

// Connected clients: id → { ws, id, partnerId, inDiscovery }
const clients = new Map();

// Discovery queue: id → timestamp
const discoveryQueue = new Map();
const DISCOVERY_TIMEOUT_MS = 30000;

// ── ID generation ─────────────────────────────────────────────
function generateId() {
  let id;
  do {
    id = String(Math.floor(100000 + Math.random() * 900000));
  } while (clients.has(id));
  return id;
}

// ── Send helpers ──────────────────────────────────────────────
function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function sendToId(id, obj) {
  const client = clients.get(id);
  if (client) send(client.ws, obj);
  return !!client;
}

function sendError(ws, message) {
  send(ws, { type: 'error', message });
}

// ── Discovery matcher ─────────────────────────────────────────
function tryMatchDiscovery(newId) {
  const now = Date.now();

  // Clean expired entries
  for (const [id, ts] of discoveryQueue) {
    if (now - ts > DISCOVERY_TIMEOUT_MS || !clients.has(id)) {
      discoveryQueue.delete(id);
    }
  }

  // Find a waiting partner (not ourselves)
  for (const [waitingId] of discoveryQueue) {
    if (waitingId !== newId) {
      discoveryQueue.delete(waitingId);
      discoveryQueue.delete(newId);

      // Pair them
      const c1 = clients.get(newId);
      const c2 = clients.get(waitingId);
      if (c1 && c2) {
        c1.partnerId = waitingId;
        c2.partnerId = newId;
        c1.inDiscovery = false;
        c2.inDiscovery = false;
        send(c1.ws, { type: 'discovered', partnerId: waitingId });
        send(c2.ws, { type: 'discovered', partnerId: newId });
        console.log(`Discovery matched: ${newId} <-> ${waitingId}`);
      }
      return;
    }
  }

  // No match yet — add to queue
  discoveryQueue.set(newId, now);
  const client = clients.get(newId);
  if (client) client.inDiscovery = true;
  console.log(`${newId} waiting in discovery (${discoveryQueue.size} in queue)`);
}

// ── Connection handler ────────────────────────────────────────
wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress;
  let clientId = null;

  console.log(`New connection from ${ip}`);

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      sendError(ws, 'Invalid JSON');
      return;
    }

    const { type } = msg;

    // ── Register ──────────────────────────────────────────────
    if (type === 'register') {
      clientId = generateId();
      clients.set(clientId, { ws, id: clientId, partnerId: null, inDiscovery: false });
      send(ws, { type: 'registered', id: clientId });
      console.log(`Registered: ${clientId} (${clients.size} online)`);
      return;
    }

    // All other messages require registration
    if (!clientId || !clients.has(clientId)) {
      sendError(ws, 'Not registered');
      return;
    }

    const me = clients.get(clientId);

    // ── Ping ──────────────────────────────────────────────────
    if (type === 'ping') {
      send(ws, { type: 'pong' });
      return;
    }

    // ── Discovery ─────────────────────────────────────────────
    if (type === 'discover') {
      tryMatchDiscovery(clientId);
      return;
    }

    // ── Call request ──────────────────────────────────────────
    if (type === 'call') {
      const targetId = String(msg.to);
      if (targetId === clientId) { sendError(ws, 'Cannot call yourself'); return; }
      const ok = sendToId(targetId, { type: 'incomingCall', from: clientId });
      if (!ok) send(ws, { type: 'partnerOffline' });
      else console.log(`Call: ${clientId} → ${targetId}`);
      return;
    }

    // ── Accept call ───────────────────────────────────────────
    if (type === 'accept') {
      const fromId = String(msg.to);
      me.partnerId = fromId;
      const partner = clients.get(fromId);
      if (partner) partner.partnerId = clientId;
      sendToId(fromId, { type: 'callAccepted', from: clientId });
      console.log(`Accepted: ${clientId} ← ${fromId}`);
      return;
    }

    // ── Reject call ───────────────────────────────────────────
    if (type === 'reject') {
      const fromId = String(msg.to);
      sendToId(fromId, { type: 'callRejected', from: clientId });
      return;
    }

    // ── Text message ──────────────────────────────────────────
    if (type === 'text') {
      const targetId = me.partnerId || String(msg.to);
      if (!targetId) { sendError(ws, 'No partner'); return; }
      const ok = sendToId(targetId, { type: 'text', from: clientId, text: msg.text });
      if (!ok) send(ws, { type: 'partnerOffline' });
      return;
    }

    // ── Audio chunk ───────────────────────────────────────────
    if (type === 'audio') {
      const targetId = me.partnerId;
      if (!targetId) return; // silently drop if no partner
      sendToId(targetId, { type: 'audio', from: clientId, data: msg.data });
      return;
    }

    // ── Hangup ────────────────────────────────────────────────
    if (type === 'hangup') {
      const targetId = me.partnerId;
      if (targetId) {
        sendToId(targetId, { type: 'hangup', from: clientId });
        const partner = clients.get(targetId);
        if (partner) partner.partnerId = null;
      }
      me.partnerId = null;
      me.inDiscovery = false;
      discoveryQueue.delete(clientId);
      console.log(`Hangup: ${clientId}`);
      return;
    }

    sendError(ws, `Unknown message type: ${type}`);
  });

  ws.on('close', () => {
    if (clientId && clients.has(clientId)) {
      const me = clients.get(clientId);

      // Notify partner if connected
      if (me.partnerId) {
        sendToId(me.partnerId, { type: 'hangup', from: clientId });
        const partner = clients.get(me.partnerId);
        if (partner) partner.partnerId = null;
      }

      // Remove from discovery
      discoveryQueue.delete(clientId);

      clients.delete(clientId);
      console.log(`Disconnected: ${clientId} (${clients.size} online)`);
    }
  });

  ws.on('error', (err) => {
    console.error(`WS error for ${clientId}: ${err.message}`);
  });
});

// ── Status log ────────────────────────────────────────────────
setInterval(() => {
  console.log(`[STATUS] ${clients.size} connected | ${discoveryQueue.size} in discovery`);
}, 60000);

console.log(`Keegs Chat relay server running on port ${PORT}`);
