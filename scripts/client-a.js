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

  // View-position channel: A reports where it is looking, and prints any
  // peer ranges/collaborator clears the gateway relays back.
  a.on('selections', (list) => {
    for (const s of list) {
      console.log(`A sees ${s.user.name} view [${s.start},${s.end}) on join`);
    }
  });
  a.on('selection', (m) => {
    console.log(`A sees ${m.user.name} moved view to [${m.start},${m.end})`);
  });
  a.on('selection-clear', (m) => {
    console.log(`A sees ${m.userId} view cleared (${m.reason})`);
  });

  await a.connect();
  console.log('A hello-ok role=', a.role, 'seq=', a.seq);

  a.localEdit((t) => t.insert(0, 'Hello from A. '));
  await a.flush();

  // Point at the word "Hello" (temporary, memory-only).
  a.sendSelection(0, 5);
  console.log('A announced view [0,5)');

  await sleep(400);
  a.localEdit((t) => t.insert(t.length, 'A adds line two.\n'));
  await a.flush();

  // Move the view to the newly appended line (refreshes the TTL too).
  const second = a.text.indexOf('A adds');
  a.sendSelection(second, a.text.length);
  console.log(`A moved view to [${second},${a.text.length})`);

  await sleep(800);
  console.log('A final text:', JSON.stringify(a.text));
  console.log('A stateHash:', a.stateHash());
  a.close();
  await sleep(100);
  process.exit(0);
}

main().catch((e) => { console.error('A failed:', e); process.exit(1); });
