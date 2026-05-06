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
app.get('/report', (req, res) => res.sendFile(path.join(__dirname, 'report.html')));

const DATA_DIR = process.env.DATA_PATH ? path.dirname(process.env.DATA_PATH) : __dirname;
const STATE_FILE = process.env.DATA_PATH || path.join(__dirname, 'data.json');
const HISTORY_FILE = path.join(DATA_DIR, 'history.json');

// Load/save current state
function loadState() {
  try {
    if (fs.existsSync(STATE_FILE)) return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch(e) {}
  return defaultState();
}

function saveState() {
  try { fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2)); } catch(e) {}
}

// Load/save history (per day)
function loadHistory() {
  try {
    if (fs.existsSync(HISTORY_FILE)) return JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
  } catch(e) {}
  return {};
}

function saveHistory() {
  try { fs.writeFileSync(HISTORY_FILE, JSON.stringify(history, null, 2)); } catch(e) {}
}

function defaultState() {
  const now = new Date();
  const tashkent = new Date(now.getTime() + 5 * 60 * 60 * 1000);
  return {
    counter: 0, queue: [], mgrServing: {}, conServing: {},
    conQueue: [], mgrHistory: [], conHistory: [],
    lastMgr: null, lastCon: null, date: tashkent.toDateString()
  };
}

function todayKey() {
  const now = new Date();
  const d = new Date(now.getTime() + 5 * 60 * 60 * 1000);
  return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
}

function checkDayReset() {
  // Use Tashkent time (UTC+5)
  const now = new Date();
  const tashkent = new Date(now.getTime() + 5 * 60 * 60 * 1000);
  const today = tashkent.toDateString();
  if (state.date !== today) {
    // Save today's data to history before reset
    const key = todayKey();
    history[key] = {
      date: state.date,
      counter: state.counter,
      mgrHistory: state.mgrHistory,
      conHistory: state.conHistory
    };
    saveHistory();
    console.log(`New day - saved ${key} to history, resetting queue`);
    state = defaultState();
    state.date = today;
    saveState();
    broadcast({ type: 'state', state });
  }
}

let state = loadState();
let history = loadHistory();
checkDayReset();
setInterval(checkDayReset, 60 * 1000);

// History API endpoint
app.get('/api/history', (req, res) => {
  // Also include today
  const key = todayKey();
  const all = { ...history };
  all[key] = {
    date: new Date().toDateString(),
    counter: state.counter,
    mgrHistory: state.mgrHistory,
    conHistory: state.conHistory
  };
  res.json(all);
});

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
    state.queue.push({ ticket: 'A'+String(state.counter).padStart(3,'0'), addedTs: Date.now() });
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
    const s = state.mgrServing[mid]; if (!s) return;
    state.mgrHistory.unshift({ ticket: s.ticket, desk, manager, dur: Math.floor((Date.now()-s.startTs)/60000), time: nowStr() });
    if (state.mgrHistory.length > 500) state.mgrHistory.pop();
    delete state.mgrServing[mid];
  }
  else if (type === 'SEND_CONTRACT') {
    const { mid, desk, manager, contractNum, apt } = payload;
    const s = state.mgrServing[mid]; if (!s) return;
    state.conQueue.push({ ticket: s.ticket, contractNum, apt, from: desk, manager, time: nowStr() });
    state.mgrHistory.unshift({ ticket: s.ticket, desk, manager, dur: Math.floor((Date.now()-s.startTs)/60000), contractNum, apt, time: nowStr(), sent: true });
    if (state.mgrHistory.length > 500) state.mgrHistory.pop();
    delete state.mgrServing[mid];
  }
  else if (type === 'TAKE_CONTRACT') {
    const { cid } = payload;
    if (state.conServing[cid] || state.conQueue.length === 0) return;
    state.conServing[cid] = { ...state.conQueue.shift(), status: 'prep', startTs: Date.now() };
  }
  else if (type === 'MARK_READY') {
    const { cid, desk } = payload;
    const s = state.conServing[cid]; if (!s) return;
    s.status = 'ready';
    state.lastCon = { ticket: s.ticket, desk, contractNum: s.contractNum };
  }
  else if (type === 'CLIENT_SIGNED') {
    const { cid, desk } = payload;
    const s = state.conServing[cid]; if (!s) return;
    state.conHistory.unshift({ ticket: s.ticket, desk, name: CON_NAMES_SERVER[cid], contractNum: s.contractNum, dur: Math.floor((Date.now()-s.startTs)/60000), time: nowStr() });
    if (state.conHistory.length > 500) state.conHistory.pop();
    delete state.conServing[cid];
  }
  else if (type === 'RESET') {
    state = defaultState(); state.date = new Date().toDateString();
  }
  saveState();
  broadcast({ type: 'state', state });
}

const CON_NAMES_SERVER = ['Viktoria','Samira','Rayxona','Gulmira','Aziz'];

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Golden Lake Queue running on port ${PORT}`));
