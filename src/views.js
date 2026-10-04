'use strict';

// Ephemeral "view position" (selection/caret) collaboration state.
//
// Unlike Yjs updates this information is NEVER durable:
//   - stored only in Room memory (room.views: userId -> entry)
//   - absent from doc_updates / doc_snapshots / the update log
//   - dropped on disconnect, after a short TTL, and on gateway restart
//   - cleared as soon as later Yjs edits make the saved offsets out of range
//
// Every mutation runs inside the room's serial queue (same queue that
// applies Yjs updates), so a view's range validation and a text-shortening
// update can never interleave into a stale-but-stored position.

const config = require('./config');

function contentLength(room) {
  return room.doc.getText('content').length;
}

// Validate the wire shape of a view frame. Returns { ok } or an error frame
// descriptor. Offsets must be non-negative integers with start <= end and
// fit inside the CURRENT text length.
function checkShape(frame) {
  const sel = frame && frame.selection;
  if (!sel || typeof sel !== 'object') {
    return { ok: false, code: 'BAD_ENVELOPE', message: 'view requires selection {start,end}' };
  }
  const { start, end } = sel;
  if (!Number.isInteger(start) || !Number.isInteger(end)
      || start < 0 || end < 0 || start > end) {
    return { ok: false, code: 'BAD_SELECTION',
      message: 'selection requires integers with 0 <= start <= end' };
  }
  return { ok: true, start, end };
}

function checkInRange(room, start, end) {
  const len = contentLength(room);
  if (end > len) {
    return { ok: false, code: 'BAD_SELECTION',
      message: `selection [${start},${end}) exceeds content length ${len}` };
  }
  return { ok: true, len };
}

function clearTimer(entry) {
  if (entry && entry.timer) {
    clearTimeout(entry.timer);
    entry.timer = null;
  }
}

// Remove a user's view and announce it. `reason' is included on the wire so
// clients can distinguish expiry/disconnect/overflow; it is informational.
function clearView(room, userId, reason) {
  const entry = room.views.get(userId);
  if (!entry) return null;
  clearTimer(entry);
  room.views.delete(userId);
  const msg = { type: 'view-clear', docId: room.docId, userId, reason: reason || 'cleared' };
  room.broadcast(msg);
  return msg;
}

// Store (or replace) a user's view, arm its TTL, and broadcast to peers.
// Must be called inside room.enqueue.
function setView(room, ctx, start, end) {
  const prior = room.views.get(ctx.userId);
  clearTimer(prior);
  const entry = {
    userId: ctx.userId,
    name: ctx.name,
    role: ctx.role,
    start,
    end,
    updatedAt: Date.now(),
    timer: null,
  };
  entry.timer = setTimeout(() => {
    // Refresh races: if the timer fires against a replaced entry it is a
    // no-op. Runs via enqueue so it cannot interleave with an update apply.
    room.enqueue(() => {
      if (room.views.get(ctx.userId) === entry) {
        clearView(room, ctx.userId, 'expired');
      }
    });
  }, config.viewTtlMs);
  // Don't keep the event loop alive solely for an ephemeral presence timer.
  if (entry.timer.unref) entry.timer.unref();
  room.views.set(ctx.userId, entry);

  room.broadcast({
    type: 'view',
    docId: room.docId,
    userId: ctx.userId,
    name: ctx.name,
    role: ctx.role,
    start,
    end,
  });
  return entry;
}

// Drop every view whose end offset no longer fits the current text.
// Called after a Yjs update has been applied, inside the serial queue:
// text length is fixed while this runs. Offsets that are still in range are
// left untouched — later local edits are the client's own responsibility to
// refresh, but we never continue to advertise a provably wrong position.
function pruneOutOfBounds(room) {
  const len = contentLength(room);
  for (const [userId, entry] of room.views) {
    if (entry.end > len) clearView(room, userId, 'out-of-bounds');
  }
}

// Current valid views for a newly joined member. Entries are plain objects;
// the timer never leaks across this boundary.
function snapshot(room) {
  const out = [];
  for (const entry of room.views.values()) {
    out.push({
      userId: entry.userId,
      name: entry.name,
      role: entry.role,
      start: entry.start,
      end: entry.end,
      updatedAt: entry.updatedAt,
    });
  }
  return out;
}

// A socket disconnected: only clear the user's view when no other socket of
// the same authenticated user remains in the room.
function onDisconnect(room, ws) {
  const ctx = ws.ctx;
  if (!ctx) return;
  for (const other of room.sockets) {
    if (other !== ws && other.readyState === other.OPEN && other.ctx
        && other.ctx.userId === ctx.userId) {
      return;
    }
  }
  if (room.views.has(ctx.userId)) {
    room.enqueue(() => clearView(room, ctx.userId, 'disconnect')).catch(() => {});
  }
}

module.exports = {
  checkShape,
  checkInRange,
  setView,
  clearView,
  pruneOutOfBounds,
  snapshot,
  onDisconnect,
};
