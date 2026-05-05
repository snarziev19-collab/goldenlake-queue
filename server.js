const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(path.join(__dirname, 'public')));

// Shared state
let state = {
  counter: 0,
  queue: [],
  mgrServing: {},
  conServing: {},
  conQueue: [],
  mgrHistory: [],
  conHistory: [],
  lastMgr: null,
  lastCon: null
};

function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(client => {
    if (client.readyState === 1) client.send(msg);
  });
}

wss.on('connection', ws => {
  // Send current state to new client
  ws.send(JSON.stringify({ type: 'state', state }));

  ws.on('message', msg => {
    try {
      const data = JSON.parse(msg);
      handleAction(data);
    } catch(e) {}
  });
});

function handleAction(data) {
  const { type, payload } = data;

  if (type === 'ADD_CLIENT') {
    state.counter++;
    const ticket = 'A' + String(state.counter).padStart(3, '0');
    state.queue.push({ ticket, addedTs: Date.now() });
  }
  else if (type === 'NEXT_CLIENT') {
    const { mid } = payload;
    if (state.mgrServing[mid] || state.queue.length === 0) return;
    const item = state.queue.shift();
    state.mgrServing[mid] = { ticket: item.ticket, startTs: Date.now() };
    state.lastMgr = { ticket: item.ticket, desk: payload.desk, manager: payload.manager };
  }
  else if (type === 'DONE_CLIENT') {
    const { mid, desk, manager } = payload;
    const s = state.mgrServing[mid];
    if (!s) return;
    state.mgrHistory.unshift({ ticket: s.ticket, desk, manager, dur: Math.floor((Date.now()-s.startTs)/60000), time: nowStr() });
    if (state.mgrHistory.length > 100) state.mgrHistory.pop();
    delete state.mgrServing[mid];
  }
  else if (type === 'SEND_CONTRACT') {
    const { mid, desk, manager, contractNum, apt } = payload;
    const s = state.mgrServing[mid];
    if (!s) return;
    state.conQueue.push({ ticket: s.ticket, contractNum, apt, from: desk, manager, time: nowStr() });
    state.mgrHistory.unshift({ ticket: s.ticket, desk, manager, dur: Math.floor((Date.now()-s.startTs)/60000), contractNum, apt, time: nowStr(), sent: true });
    if (state.mgrHistory.length > 100) state.mgrHistory.pop();
    delete state.mgrServing[mid];
  }
  else if (type === 'TAKE_CONTRACT') {
    const { cid } = payload;
    if (state.conServing[cid] || state.conQueue.length === 0) return;
    state.conServing[cid] = { ...state.conQueue.shift(), status: 'prep', startTs: Date.now() };
  }
  else if (type === 'MARK_READY') {
    const { cid, desk } = payload;
    const s = state.conServing[cid];
    if (!s) return;
    s.status = 'ready';
    state.lastCon = { ticket: s.ticket, desk, contractNum: s.contractNum };
  }
  else if (type === 'CLIENT_SIGNED') {
    const { cid, desk } = payload;
    const s = state.conServing[cid];
    if (!s) return;
    state.conHistory.unshift({ ticket: s.ticket, desk, contractNum: s.contractNum, dur: Math.floor((Date.now()-s.startTs)/60000), time: nowStr() });
    if (state.conHistory.length > 100) state.conHistory.pop();
    delete state.conServing[cid];
  }
  else if (type === 'RESET') {
    state = { counter:0, queue:[], mgrServing:{}, conServing:{}, conQueue:[], mgrHistory:[], conHistory:[], lastMgr:null, lastCon:null };
  }

  broadcast({ type: 'state', state });
}

function nowStr() {
  return new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Golden Lake Queue running on port ${PORT}`));
