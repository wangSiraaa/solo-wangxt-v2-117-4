'use strict';

const Y = require('yjs');
const { getRoom } = require('./room');
const { processUpdate } = require('./update-service');
const { compact } = require('./compaction');
const { resolveToken, getActiveRole } = require('./permissions');
const yutil = require('./yutil');
const { logError } = require('./errorlog');
const views = require('./views');

// Wire protocol (JSON text frames; Yjs binaries are base64 fields):
//   client -> server  hello      { token, docId, sv?(b64 state vector) }
//   server -> client  hello-ok   { docId, seq, sv, state }   (state = diff vs sv)
//   server -> client  hello-err  { code, message } + close
//   client -> server  update     { msgId, update(b64), svHash? }
//   server -> client  ack        { msgId, ok, seq, duplicated? } | nack fields
//   server -> client  update     { seq, msgId, origin, update(b64) }
//   client -> server  sync-req   { sv(b64) }   -> sync-diff { seq, update(b64) }
//   client -> server  ping       -> pong
//
// Ephemeral "view position" frames (memory only; never persisted, never
// applied to the Yjs doc, absent from the update log and snapshots):
//   client -> server  view       { selection: { start, end } }
//   server -> client  view       { userId, name, role, start, end }  (peers)
//   server -> client  view-ack   { ok, start, end } | { ok:false, code, message }
//   server -> client  view-clear { userId, reason }
//                        reason: expired | disconnect | out-of-bounds | cleared
//   server -> client  views      { views: [...] }   (snapshot sent on join)
//
// The docId on every frame is the authenticated room's docId: client frames
// never carry a room id after hello, so a client cannot write into another
// room by claiming one.

async function wsConnection(socket /* WebSocket */, req) {
  const peer = `${req.socket.remoteAddress}:${req.socket.remotePort}`;
  let ctx = null;
  let room = null;
  let closed = false;

  socket.on('error', (err) => {
    console.error(`[ws ${peer}] socket error:`, err.message);
  });

  socket.on('close', () => {
    closed = true;
    if (room) {
      // Drop the user's ephemeral view (unless another socket of the same
      // user remains) BEFORE removing the socket from the room set.
      views.onDisconnect(room, socket);
      room.sockets.delete(socket);
    }
  });

  socket.on('message', async (raw) => {
    let frame;
    try {
      frame = JSON.parse(raw.toString('utf8'));
    } catch (e) {
      await logError({ raw: Buffer.from(raw), code: 'BAD_JSON', message: e.message });
      safeSend(socket, { type: 'error', code: 'BAD_JSON', message: 'frame is not JSON' });
      return;
    }
    if (!frame || typeof frame !== 'object' || typeof frame.type !== 'string') {
      await logError({ raw: Buffer.from(raw), code: 'BAD_ENVELOPE', message: 'missing type' });
      return safeSend(socket, { type: 'error', code: 'BAD_ENVELOPE', message: 'missing type' });
    }

    try {
      if (!ctx) {
        if (frame.type !== 'hello') {
          await logError({ raw: Buffer.from(raw), code: 'NOT_HELLO',
            message: `first frame was ${frame.type}` });
          safeSend(socket, { type: 'hello-err', code: 'NOT_HELLO',
            message: 'first frame must be hello' });
          return socket.close();
        }
        return await handleHello(socket, frame, (c, r) => { ctx = c; room = r; });
      }

      switch (frame.type) {
        case 'ping':
          return safeSend(socket, { type: 'pong' });
        case 'update':
          return await processUpdate(socket, room, ctx, frame);
        case 'sync-req':
          return await handleSyncReq(socket, room, ctx, frame);
        case 'compact':
          return await handleCompact(socket, room, ctx, frame);
        case 'view':
          return await handleView(socket, room, ctx, frame);
        default:
          await logError({ docId: room.docId, userId: ctx.userId,
            tenantId: ctx.tenantId, raw: Buffer.from(raw), code: 'UNKNOWN_TYPE',
            message: `unknown frame type ${frame.type}` });
          return safeSend(socket, {
            type: 'error', code: 'UNKNOWN_TYPE',
            message: `unknown frame type: ${frame.type}`,
          });
      }
    } catch (err) {
      // Defensive: one bad frame must never kill the gateway process.
      console.error(`[ws ${peer}] handler error:`, err);
      await logError({
        docId: room ? room.docId : null,
        userId: ctx ? ctx.userId : null,
        tenantId: ctx ? ctx.tenantId : null,
        raw: Buffer.from(raw), code: 'INTERNAL',
        message: err && err.stack ? err.stack : String(err),
      }).catch(() => {});
      safeSend(socket, { type: 'error', code: 'INTERNAL', message: 'internal error' });
    }
  });
}

async function handleHello(socket, frame, bind) {
  const { token, docId } = frame;
  if (!token || !docId || typeof docId !== 'string') {
    safeSend(socket, { type: 'hello-err', code: 'BAD_ENVELOPE',
      message: 'hello requires token and docId' });
    return socket.close();
  }

  const session = await resolveToken(token);
  if (!session) {
    await logError({ docId, raw: Buffer.from(JSON.stringify({ docId })),
      code: 'BAD_TOKEN', message: `token/user not found: ${String(token).slice(0, 64)}` });
    safeSend(socket, { type: 'hello-err', code: 'BAD_TOKEN', message: 'unknown token' });
    return socket.close();
  }

  // Re-resolve the doc id against membership: do not trust the claimed room.
  const membership = await getActiveRole(session.user_id, docId);
  if (!membership || membership.tenant_id !== session.tenant_id) {
    await logError({ docId, userId: session.user_id, tenantId: session.tenant_id,
      code: 'FORBIDDEN', message: 'no active membership for claimed document' });
    safeSend(socket, { type: 'hello-err', code: 'FORBIDDEN',
      message: 'no access to this document' });
    return socket.close();
  }

  const room = await getRoom(docId);
  const ctx = {
    userId: session.user_id,
    tenantId: session.tenant_id,
    role: membership.role,
    name: session.name,
  };
  socket.ctx = ctx;
  socket.docId = docId;
  room.sockets.add(socket);
  bind(ctx, room);

  // Catch-up against the claimed state vector. Missing structs only are
  // sent; a fresh/empty sv receives the full state.
  let svBytes;
  if (frame.sv && typeof frame.sv === 'string') {
    try {
      svBytes = Buffer.from(frame.sv, 'base64');
      // Validate that it parses as a Yjs vector before trusting it.
      yutil.decodeStateVector(svBytes);
    } catch (e) {
      await logError({ docId, userId: ctx.userId, tenantId: ctx.tenantId,
        code: 'BAD_SV', message: e.message });
      svBytes = Buffer.alloc(0);
    }
  } else {
    svBytes = Buffer.alloc(0);
  }

  const diff = svBytes.length === 0
    ? yutil.encodeState(room.doc)
    : yutil.diffUpdate(room.doc, svBytes);
  const sv = yutil.stateVector(room.doc);

  safeSend(socket, {
    type: 'hello-ok',
    docId,
    role: ctx.role,
    seq: room.loadedSeq,
    sv: sv.toString('base64'),
    state: diff.toString('base64'),
  });

  room.broadcast({
    type: 'presence',
    docId,
    user: { id: ctx.userId, name: ctx.name, role: ctx.role },
    joined: true,
    members: memberList(room),
  }, socket);

  // Current valid ephemeral views in the room (memory only). Includes the
  // joining user's own view when they reconnect with another socket.
  safeSend(socket, { type: 'views', docId, views: views.snapshot(room) });
}

async function handleSyncReq(socket, room, ctx, frame) {
  if (!frame.sv || typeof frame.sv !== 'string') {
    return safeSend(socket, { type: 'error', code: 'BAD_SV', message: 'sync-req requires sv' });
  }
  let svBytes;
  try {
    svBytes = Buffer.from(frame.sv, 'base64');
    yutil.decodeStateVector(svBytes);
  } catch (e) {
    return safeSend(socket, { type: 'error', code: 'BAD_SV', message: e.message });
  }
  // Re-check membership on explicit catch-up too.
  const role = await getActiveRole(ctx.userId, room.docId);
  if (!role || role.tenant_id !== ctx.tenantId) {
    return safeSend(socket, { type: 'error', code: 'FORBIDDEN', message: 'access revoked' });
  }
  const diff = yutil.diffUpdate(room.doc, svBytes);
  safeSend(socket, {
    type: 'sync-diff',
    docId: room.docId,
    seq: room.loadedSeq,
    update: diff.toString('base64'),
  });
}

async function handleCompact(socket, room, ctx, frame) {
  if (ctx.role === 'reader') {
    return safeSend(socket, { type: 'error', code: 'READ_ONLY', message: 'reader cannot compact' });
  }
  const out = await compact(room, {
    deleteFolded: !!frame.deleteFolded,
    minUpdates: frame.minUpdates || 1,
  });
  safeSend(socket, { type: 'compact-result', ...out });
}

// Ephemeral view position. Readers may advertise where they look too (it is
// not a document write), but the connection must still be an authenticated
// member of THIS room. Validation and storage run in the room's serial queue
// so the text length cannot change between the bounds check and the store.
async function handleView(socket, room, ctx, frame) {
  const shape = views.checkShape(frame);
  if (!shape.ok) {
    await logError({ docId: room.docId, userId: ctx.userId, tenantId: ctx.tenantId,
      code: shape.code, message: shape.message });
    return safeSend(socket, { type: 'view-ack', ok: false,
      code: shape.code, message: shape.message });
  }
  // Re-check membership on every frame, exactly like document writes: a
  // member revoked mid-session must stop broadcasting positions.
  const role = await getActiveRole(ctx.userId, room.docId);
  if (!role || role.tenant_id !== ctx.tenantId) {
    await logError({ docId: room.docId, userId: ctx.userId, tenantId: ctx.tenantId,
      code: 'FORBIDDEN', message: 'view frame after access revoked' });
    return safeSend(socket, { type: 'view-ack', ok: false,
      code: 'FORBIDDEN', message: 'access revoked' });
  }
  await room.enqueue(() => {
    const range = views.checkInRange(room, shape.start, shape.end);
    if (!range.ok) {
      safeSend(socket, { type: 'view-ack', ok: false,
        code: range.code, message: range.message });
      return;
    }
    views.setView(room, ctx, shape.start, shape.end);
    safeSend(socket, { type: 'view-ack', ok: true,
      start: shape.start, end: shape.end, ttlMs: require('./config').viewTtlMs });
  });
}

function memberList(room) {
  const seen = new Map();
  for (const ws of room.sockets) {
    if (ws.ctx) seen.set(ws.ctx.userId, ws.ctx);
  }
  return [...seen.values()];
}

function safeSend(socket, obj) {
  if (socket.readyState === socket.OPEN) {
    socket.send(JSON.stringify(obj));
  }
}

module.exports = { wsConnection };
