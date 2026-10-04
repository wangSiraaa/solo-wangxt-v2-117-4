#!/usr/bin/env node
'use strict';

// Demo client B: connects, deletes/changes overlapping ranges with A, then
// prints final state. Both clients print the structural state hash, which
// must be identical (true CRDT convergence, not just equal-looking strings).

const { DocClient } = require('./lib-client');

const URL = process.env.WS_URL || 'ws://127.0.0.1:7777/ws';
const DOC = process.env.DOC_ID || 'doc-demo';
const TOKEN = process.env.TOKEN_B || 'user-bob';

async function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function main() {
  const b = new DocClient({ url: URL, token: TOKEN, docId: DOC, name: 'B', verbose: true });

  // Show where collaborators are looking. Ranges are temporary presence:
  // disconnect, TTL expiry or an edit that pushes them out of bounds clear
  // them via selection-clear.
  b.on('selections', (list) => {
    for (const s of list) {
      console.log(`B sees ${s.user.name} view [${s.start},${s.end}) on join`);
    }
  });
  b.on('selection', (m) => {
    console.log(`B sees ${m.user.name} view [${m.start},${m.end})`);
  });
  b.on('selection-clear', (m) => {
    console.log(`B sees ${m.userId} view cleared (${m.reason})`);
  });

  await b.connect();
  console.log('B hello-ok role=', b.role, 'seq=', b.seq, 'text on join=', JSON.stringify(b.text));

  await sleep(150);
  b.localEdit((t) => t.insert(t.length, 'B was here too. '));
  await b.flush();

  // A cursor at the end of B's own sentence.
  b.sendSelection(b.text.length, b.text.length);
  console.log(`B announced cursor at ${b.text.length}`);

  await sleep(300);
  // Concurrent edit against A's second insertion: replace prefix.
  b.localEdit((t) => {
    if (t.length >= 5) t.delete(0, 5);
    t.insert(0, 'HELLO');
  });
  await b.flush();

  await sleep(900);
  console.log('B final text:', JSON.stringify(b.text));
  console.log('B stateHash:', b.stateHash());
  b.close();
  await sleep(100);
  process.exit(0);
}

main().catch((e) => { console.error('B failed:', e); process.exit(1); });
