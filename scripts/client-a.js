#!/usr/bin/env node
'use strict';

// Demo client A: connects, inserts a paragraph, then keeps editing while
// connected. Used together with client-b.js to show live convergence.

const { DocClient } = require('./lib-client');

const URL = process.env.WS_URL || 'ws://127.0.0.1:7777/ws';
const DOC = process.env.DOC_ID || 'doc-demo';
const TOKEN = process.env.TOKEN_A || 'user-alice';

async function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function main() {
  const a = new DocClient({ url: URL, token: TOKEN, docId: DOC, name: 'A', verbose: true });
  await a.connect();
  console.log('A hello-ok role=', a.role, 'seq=', a.seq);

  // Show peers where A is looking. Ephemeral only: lives in gateway memory,
  // expires shortly after the last refresh, never enters the Yjs log.
  a.on('view-clear', (m) => console.log(`A: peer view cleared (${m.userId}, ${m.reason})`));

  a.localEdit((t) => t.insert(0, 'Hello from A. '));
  await a.flush();
  a.sendView(0, 12);
  console.log('A announced view [0,12) ("Hello from A")');

  await sleep(400);
  a.localEdit((t) => t.insert(t.length, 'A adds line two.\n'));
  await a.flush();
  // Move the selection to the new line; peers must see the latest position.
  a.sendView(14, 27);
  console.log('A moved view to [14,27) ("A adds line t")');

  await sleep(800);
  console.log('A final text:', JSON.stringify(a.text));
  console.log('A stateHash:', a.stateHash());
  a.close();
  await sleep(100);
  process.exit(0);
}

main().catch((e) => { console.error('A failed:', e); process.exit(1); });
