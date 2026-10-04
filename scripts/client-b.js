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
  await b.connect();
  console.log('B hello-ok role=', b.role, 'seq=', b.seq, 'text on join=', JSON.stringify(b.text));

  // Ephemeral peer view positions (snapshot on join, then live updates).
  const logViews = (tag) => {
    const all = [...b.peerViews.entries()]
      .map(([u, v]) => `${u}=[${v.start},${v.end})`)
      .join(' ') || '(none)';
    console.log(`B views ${tag}: ${all}`);
  };
  b.on('views', () => logViews('on join'));
  b.on('view', (m) => console.log(
    `B: ${m.name || m.userId} is viewing [${m.start},${m.end}) -> `
    + JSON.stringify(b.text.slice(m.start, m.end))));
  b.on('view-clear', (m) => console.log(`B: peer view cleared (${m.userId}, ${m.reason})`));

  await sleep(150);
  b.localEdit((t) => t.insert(t.length, 'B was here too. '));
  await b.flush();

  await sleep(300);
  // Concurrent edit against A's second insertion: replace prefix.
  b.localEdit((t) => {
    if (t.length >= 5) t.delete(0, 5);
    t.insert(0, 'HELLO');
  });
  await b.flush();

  await sleep(400);
  // Shorten the document far enough that A's saved [14,27) view is out of
  // bounds: the gateway must drop it and tell the whole room rather than
  // keep advertising a wrong position.
  b.localEdit((t) => {
    if (t.length > 6) t.delete(6, t.length - 6);
  });
  await b.flush();
  console.log('B shortened text to', JSON.stringify(b.text), '- out-of-range views must clear');
  await sleep(500);
  logViews('after shortening');

  await sleep(400);
  console.log('B final text:', JSON.stringify(b.text));
  console.log('B stateHash:', b.stateHash());
  b.close();
  await sleep(100);
  process.exit(0);
}

main().catch((e) => { console.error('B failed:', e); process.exit(1); });
