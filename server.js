const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');
const path = require('path');
const fs = require('fs');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

app.use(express.static(__dirname));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

// ── Database (simple JSON file) ──────────────────────────────
const DB_FILE = process.env.DATA_PATH || path.join(__dirname, 'data.json');

function loadState() {
  try {
    if (fs.existsSync(DB_FILE)) {
      const raw = fs.readFileSync(DB_FILE, 'utf8');
      return JSON.parse(raw);
    }
  } catch(e) { console.log('DB load error:', e.message); }
  return defaultState();
}

function saveState() {
  try {
    fs.writeFileSync(DB_FILE, JSON.stringify(state, null, 2));
  } catch(e) { console.log('DB save error:', e.message); }
}

function defaultState() {
  return {
    counter: 0,
    queue: [],
    mgrServing: {},
    conServing: {},
    conQueue: [],
    mgrHistory: [],
    conHistory: [],
    lastMgr: null,
    lastCon: null,
    date: new Date().toDateString()
  };
}

// Auto-reset at start of new day
function checkDayReset() {
  const today = new Date().toDateString();
  if (state.date !== today) {
    console.log('New day - resetting queue');
    state = defaultState();
    state.date = today;
    saveState();
  }
}

let state = loadState();
checkDayReset();
setInterval(checkDayReset, 60 * 1000); // check every minute

// ── WebSocket ────────────────────────────────────────────────
function broadcast(data) {
  const msg = JSON.stringify(data);
  wss.clients.forEach(c => { if (c.readyState === 1) c.send(msg); });
}

wss.on('connection', ws => {
  ws.send(JSON.stringify({ type: 'state', state }));
  ws.on('message', msg => {
    try { handleAction(JSON.parse(msg)); } catch(e) {}
  });
});

function nowStr() {
  return new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
}

function handleAction({ type, payload }) {
  if (type === 'ADD_CLIENT') {
    state.counter++;
    const ticket = 'A' + String(state.counter).padStart(3, '0');
    state.queue.push({ ticket, addedTs: Date.now() });
  }
  else if (type === 'NEXT_CLIENT') {
    const { mid, desk, manager } = payload;
    if (state.mgrServing[mid] || state.queue.length === 0) return;
    const item = state.queue.shift();
    state.mgrServing[mid] = { ticket: item.ticket, startTs: Date.now() };
    state.lastMgr = { ticket: item.ticket, desk, manager };
  }
  else if (type === 'DONE_CLIENT') {
    const { mid, desk, manager } = payload;
    const s = state.mgrServing[mid];
    if (!s) return;
    state.mgrHistory.unshift({ ticket: s.ticket, desk, manager, dur: Math.floor((Date.now()-s.startTs)/60000), time: nowStr() });
    if (state.mgrHistory.length > 200) state.mgrHistory.pop();
    delete state.mgrServing[mid];
  }
  else if (type === 'SEND_CONTRACT') {
    const { mid, desk, manager, contractNum, apt } = payload;
    const s = state.mgrServing[mid];
    if (!s) return;
    state.conQueue.push({ ticket: s.ticket, contractNum, apt, from: desk, manager, time: nowStr() });
    state.mgrHistory.unshift({ ticket: s.ticket, desk, manager, dur: Math.floor((Date.now()-s.startTs)/60000), contractNum, apt, time: nowStr(), sent: true });
    if (state.mgrHistory.length > 200) state.mgrHistory.pop();
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
    if (state.conHistory.length > 200) state.conHistory.pop();
    delete state.conServing[cid];
  }
  else if (type === 'RESET') {
    state = defaultState();
    state.date = new Date().toDateString();
  }

  saveState();
  broadcast({ type: 'state', state });
}

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Golden Lake Queue running on port ${PORT}`));
