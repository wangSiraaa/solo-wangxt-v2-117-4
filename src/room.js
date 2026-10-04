'use strict';

const Y = require('yjs');
const db = require('./db');
const yutil = require('./yutil');
const config = require('./config');

// One Room per document:
//  - in-memory Y.Doc (gc=false) rebuilt from snapshot + tail on first use
//  - a strict serial async queue so apply order == persisted seq order
//  - set of connected sockets, each carrying authenticated context
//  - ephemeral "view position" selections, memory-only: never part of the
//    Yjs update log, snapshots, or any other persisted state

class Room {
  constructor(docId) {
    this.docId = docId;
    this.doc = yutil.createDoc();
    this.sockets = new Set();
    this.tail = Promise.resolve();
    this.loadedSeq = 0;
    this.compactions = 0;

    // userId -> { user:{id,name,role}, start, end, expiresAt }
    // Selections are keyed by user (a user owns one current range per doc).
    this.selections = new Map();
    this._selectionTimers = new Map(); // userId -> timeout handle
    this._selectionTtlMs = config.selectionTtlMs;
    this._reconcileQueued = false;

    // Any Yjs change can shrink the text and invalidate saved offsets.
    // Reconciliation is deferred to a microtask so the originating 'update'
    // frame is broadcast first; peers learn the new length before the clear.
    // During load() replay the selection map is empty, so this is a no-op.
    this.doc.on('update', () => {
      if (this._reconcileQueued || this.selections.size === 0) return;
      this._reconcileQueued = true;
      queueMicrotask(() => {
        this._reconcileQueued = false;
        this.reconcileSelections('out-of-bounds');
      });
    });
  }

  enqueue(job) {
    const run = this.tail.then(() => job());
    // Keep the chain alive even if this job rejects; the job itself is
    // expected to convert failures into error frames / rows.
    this.tail = run.then(
      () => {},
      () => {},
    );
    return run;
  }

  broadcast(message, exceptSocket = null) {
    const data = JSON.stringify(message);
    for (const ws of this.sockets) {
      if (ws === exceptSocket) continue;
      if (ws.readyState === ws.OPEN) {
        ws.send(data);
      }
    }
  }

  contentLength() {
    return this.doc.getText('content').length;
  }

  // Store/refresh a user's selection. Offsets are UTF-16 indices into the
  // shared `content` text, exactly as Yjs's Y.Text uses them. Out-of-range
  // offsets are rejected without touching stored state. Returns
  // { ok:true } or { ok:false, code, message }.
  setSelection(user, start, end, originSocket = null) {
    if (!Number.isInteger(start) || !Number.isInteger(end)
        || start < 0 || end < start) {
      return { ok: false, code: 'BAD_SELECTION',
        message: 'selection requires 0 <= start <= end (integer offsets)' };
    }
    const len = this.contentLength();
    if (end > len) {
      return { ok: false, code: 'BAD_SELECTION',
        message: `selection end ${end} exceeds content length ${len}` };
    }
    const now = Date.now();
    this.selections.set(user.id, {
      user: { id: user.id, name: user.name, role: user.role },
      start, end, expiresAt: now + this._selectionTtlMs,
    });
    this._armSelectionTtl(user.id);
    // Peers only: the origin already knows its own range, and echoing it
    // back would be useless noise (echoing an 'update' was a real divergence
    // bug, so broadcasts here also exclude the source).
    this.broadcast({
      type: 'selection', docId: this.docId,
      user: { id: user.id, name: user.name, role: user.role },
      start, end,
    }, originSocket);
    return { ok: true };
  }

  // Remove a selection and announce it. out-of-bounds clears are sent to
  // EVERY socket (including the editing peer that shrank the text) so no
  // client can keep rendering a stale, wrong range.
  clearSelection(userId, reason, { exceptSocket = null } = {}) {
    if (!this.selections.has(userId)) return false;
    this.selections.delete(userId);
    const handle = this._selectionTimers.get(userId);
    if (handle) { clearTimeout(handle); this._selectionTimers.delete(userId); }
    this.broadcast({
      type: 'selection-clear', docId: this.docId, userId, reason,
    }, exceptSocket);
    return true;
  }

  // Snapshot for a newly joined member; only still-valid (non-expired)
  // entries are handed out.
  currentSelections(now = Date.now()) {
    const out = [];
    for (const s of this.selections.values()) {
      if (s.expiresAt > now) {
        out.push({
          user: s.user, start: s.start, end: s.end,
        });
      }
    }
    return out;
  }

  // Drop every selection a disconnecting user still owns here. Keyed by
  // user: a user with another live socket in the same room keeps its range.
  clearSelectionsOfUser(userId, reason) {
    let stillHere = false;
    for (const ws of this.sockets) {
      if (ws.ctx && ws.ctx.userId === userId) { stillHere = true; break; }
    }
    if (stillHere) return false;
    return this.clearSelection(userId, reason);
  }

  // After a Yjs update, every stored range whose end is now past the text is
  // invalid and must be removed; the room then announces the removal.
  reconcileSelections(reason = 'out-of-bounds') {
    if (this.selections.size === 0) return 0;
    const len = this.contentLength();
    const invalid = [];
    for (const [userId, s] of this.selections) {
      if (s.end > len) invalid.push(userId);
    }
    for (const userId of invalid) {
      // exceptSocket null: the peer that performed the shrinking edit must
      // also stop showing a position that no longer exists.
      this.clearSelection(userId, reason, { exceptSocket: null });
    }
    return invalid.length;
  }

  _armSelectionTtl(userId) {
    const existing = this._selectionTimers.get(userId);
    if (existing) clearTimeout(existing);
    const handle = setTimeout(() => {
      this._selectionTimers.delete(userId);
      if (!this.selections.has(userId)) return;
      this.selections.delete(userId);
      this.broadcast({
        type: 'selection-clear', docId: this.docId,
        userId, reason: 'expired',
      });
    }, this._selectionTtlMs);
    // Don't keep the process alive just for selection expiry.
    if (handle.unref) handle.unref();
    this._selectionTimers.set(userId, handle);
  }

  // Load authoritative state: latest snapshot + every update past it.
  async load() {
    const snapRes = await db.query(
      `SELECT id, through_seq, state_bytes
         FROM doc_snapshots
        WHERE doc_id = $1
        ORDER BY id DESC
        LIMIT 1`,
      [this.docId],
    );
    const snap = snapRes.rows[0] || null;
    const fromSeq = snap ? Number(snap.through_seq) : 0;
    const updRes = await db.query(
      `SELECT seq, update_bytes
         FROM doc_updates
        WHERE doc_id = $1 AND seq > $2
        ORDER BY seq ASC`,
      [this.docId, fromSeq],
    );
    if (snap) {
      Y.applyUpdate(this.doc, new Uint8Array(snap.state_bytes), 'snapshot');
    }
    for (const row of updRes.rows) {
      Y.applyUpdate(this.doc, new Uint8Array(row.update_bytes), `seq:${row.seq}`);
    }
    this.loadedSeq = updRes.rows.length
      ? Number(updRes.rows[updRes.rows.length - 1].seq)
      : fromSeq;
    return { fromSeq, applied: updRes.rows.length };
  }
}

const rooms = new Map();

async function getRoom(docId) {
  let room = rooms.get(docId);
  if (!room) {
    room = new Room(docId);
    rooms.set(docId, room);
    await room.load();
  }
  return room;
}

function roomCount() {
  return rooms.size;
}

module.exports = { getRoom, roomCount, Room };
