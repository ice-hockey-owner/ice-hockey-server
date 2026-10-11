const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
// Every Express route needs CORS too, not just Socket.IO -- the client lives on a
// different origin and calls /api/leaderboard and /random-id-photo directly.
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.header('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});
app.get('/', (req, res) => {
  res.status(200).json({
    ok: true,
    service: 'Ice Hockey Multiplayer Server',
    websocket: '/socket.io',
    status: 'online'
  });
});
app.get('/health', (req, res) => res.status(200).json({ ok: true }));
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
  maxHttpBufferSize: 8e6,
  // A dead connection used to take up to 85s to be noticed, freezing the
  // opponent's game. Detect it within ~25s instead.
  pingInterval: 10000,
  pingTimeout: 15000,
  // Tiny realtime packets (positions) are not worth compressing; it only adds latency/CPU.
  perMessageDeflate: false
});

app.use(express.static(__dirname));
app.get('/api/leaderboard', (req,res) => {
  res.set('Cache-Control','no-store, no-cache, must-revalidate, proxy-revalidate');
  res.json(getLeaderboardRows(req.query.period));
});

const idPhotoDir = path.join(__dirname, 'assets', 'images', 'IdPhotos');
app.get('/random-id-photo', (req, res) => {
  try {
    const files = ['1.svg','2.png','3.png','4.png'].filter(f => fs.existsSync(path.join(idPhotoDir, f)));
    if (!files.length) return res.status(404).end();
    const file = files[Math.floor(Math.random() * files.length)];
    res.sendFile(path.join(idPhotoDir, file));
  } catch {
    res.status(404).end();
  }
});

let waitingPlayer = null;
const rooms = {};
const onlineByName = new Map();
const friendRequests = new Map(); // name -> Set(sender names)
const friends = new Map();        // name -> Set(friend names)
const chatHistory = new Map();   // sorted pair -> recent messages
const rematchChallenges = new Map(); // target name -> challenger name
const MAX_FRIENDS = 250;
const GOAL_COOLDOWN_MS = 1200; // ignore duplicate goal reports for the same puck crossing
const dataDir = process.env.DATA_DIR || __dirname;
try { fs.mkdirSync(dataDir, { recursive: true }); } catch {}
const statsFile = path.join(dataDir, 'player-stats.json');
const goalEventsFile = path.join(dataDir, 'goal-events.json');
const namesFile = path.join(dataDir, 'reserved-names.json');
const rewardFile = path.join(dataDir, 'rewarded-ads.json');

// ---- Persistence --------------------------------------------------------
// The old code called fs.writeFileSync (pretty-printed, up to 200k goal events)
// on EVERY goal. That blocks the single Node event loop for every connected
// player and shows up as sudden lag spikes. Saves are now debounced, async,
// compact and atomic (temp file + rename), and flushed synchronously on shutdown.
const savers = [];
function makeSaver(file, getData) {
  let dirty = false, writing = false, timer = null;
  const flushAsync = () => {
    timer = null;
    if (!dirty || writing) { if (dirty && !timer) timer = setTimeout(flushAsync, 1000); return; }
    dirty = false; writing = true;
    const tmp = file + '.tmp';
    fs.writeFile(tmp, JSON.stringify(getData()), err => {
      if (err) { writing = false; dirty = true; return; }
      fs.rename(tmp, file, () => { writing = false; if (dirty && !timer) timer = setTimeout(flushAsync, 3000); });
    });
  };
  const saver = {
    schedule() { dirty = true; if (!timer && !writing) timer = setTimeout(flushAsync, 3000); },
    flushSync() { if (!dirty) return; dirty = false; try { fs.writeFileSync(file, JSON.stringify(getData())); } catch {} }
  };
  savers.push(saver);
  return saver;
}
function flushAllSync() { for (const s of savers) s.flushSync(); }
process.on('SIGTERM', () => { flushAllSync(); process.exit(0); });
process.on('SIGINT', () => { flushAllSync(); process.exit(0); });

let goalEvents = [];
try { goalEvents = JSON.parse(fs.readFileSync(goalEventsFile, 'utf8')) || []; } catch { goalEvents = []; }
const goalEventsSaver = makeSaver(goalEventsFile, () => goalEvents.slice(-200000));
function saveGoalEvents(){ goalEventsSaver.schedule(); }
function recordGoal(name){
  const playerName = normalizeName(name);
  if(!playerName) return;
  goalEvents.push({name:playerName, at:Date.now()});
  if(goalEvents.length > 200000) goalEvents = goalEvents.slice(-200000);
  saveGoalEvents();
}
function leaderboardWindow(period){
  const now = new Date();
  if(period === 'weekly') return Date.now() - 7*24*60*60*1000;
  if(period === 'monthly') return new Date(now.getFullYear(), now.getMonth(), 1).getTime();
  return 0;
}
let reservedNames = {};
let rewardedAds = {};
try { reservedNames = JSON.parse(fs.readFileSync(namesFile, 'utf8')) || {}; } catch { reservedNames = {}; }
try { rewardedAds = JSON.parse(fs.readFileSync(rewardFile, 'utf8')) || {}; } catch { rewardedAds = {}; }
const namesSaver = makeSaver(namesFile, () => reservedNames);
const rewardSaver = makeSaver(rewardFile, () => rewardedAds);
function saveReservedNames(){ namesSaver.schedule(); }
function saveRewardedAds(){ rewardSaver.schedule(); }
function rewardDay(){ return new Intl.DateTimeFormat('en-CA',{timeZone:'Asia/Kolkata',year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date()); }
function profileKey(data){ return String(data?.profileId || '').trim().slice(0,80); }
let playerStats = {};
try { playerStats = JSON.parse(fs.readFileSync(statsFile, 'utf8')) || {}; } catch { playerStats = {}; }
const statsSaver = makeSaver(statsFile, () => playerStats);
function cleanStats(st){ const games=Number(st?.gamesPlayed)||0,wins=Number(st?.wins)||0,losses=Number(st?.losses)||0,goals=Number(st?.goals)||0; return {gamesPlayed:games,wins,losses,goals,winRate:games?Math.round(wins/games*100):0}; }
function saveStats(){ statsSaver.schedule(); }
function getLeaderboardRows(period='all'){
  const normalized = String(period || 'all').toLowerCase();
  let rows;
  if(normalized === 'weekly' || normalized === 'monthly'){
    const since = leaderboardWindow(normalized);
    const counts = new Map();
    for(const event of goalEvents){
      if(Number(event?.at) < since) continue;
      const name = normalizeName(event?.name);
      if(!name) continue;
      const k = key(name);
      const entry = counts.get(k) || {name, goals:0};
      entry.goals++; counts.set(k, entry);
    }
    rows = Array.from(counts.values());
  } else {
    rows = Object.entries(playerStats).map(([k, st]) => ({ name: st.name || k, goals: Number(st.goals)||0 }));
  }
  return rows.sort((a,b)=>b.goals-a.goals || a.name.localeCompare(b.name)).slice(0,100);
}

function chatKey(a,b){ return [key(a),key(b)].sort().join('::'); }
function pushChatMessage(a,b,payload){
  const k=chatKey(a,b); const list=chatHistory.get(k)||[]; list.push(payload);
  if(list.length>200) list.splice(0,list.length-200); chatHistory.set(k,list);
}
function startMatch(p1,p2,roomId){
  const startAt=Date.now()+2100;
  const p1Profile={name:p1.playerName||'Player 1',avatar:p1.playerAvatar||'',customization:cleanCustomization(p1.playerCustomization||{})};
  const p2Profile={name:p2.playerName||'Player 2',avatar:p2.playerAvatar||'',customization:cleanCustomization(p2.playerCustomization||{})};
  rooms[roomId]={p1:p1.id,p2:p2.id,startAt,ended:false,lastGoalAt:0,scores:{left:0,right:0},profiles:{left:p1Profile,right:p2Profile}};
  p1.join(roomId); p2.join(roomId);
  // The room keeps an immutable snapshot for this match. Each client receives
  // BOTH players' profiles, so the renderer never has to guess whose cosmetic belongs where.
  const payload=(side,self,opp)=>({
    room:roomId,side,startAt,
    // Canonical match identities: left is always p1, right is always p2.
    leftProfile:p1Profile,
    rightProfile:p2Profile,
    selfProfile:self,opponentProfile:opp,
    playerName:self.name,playerAvatar:self.avatar,playerCustomization:self.customization,
    opponentName:opp.name,opponentAvatar:opp.avatar,opponentCustomization:opp.customization,
    opponentSocketId:side==='left'?p2.id:p1.id
  });
  p1.emit('match_found',payload('left',p1Profile,p2Profile));
  p2.emit('match_found',payload('right',p2Profile,p1Profile));
  setTimeout(()=>{ const r=rooms[roomId]; if(!r||r.ended)return; r.ended=true; io.to(roomId).emit('game_ended',{reason:'Time up'}); setTimeout(()=>{ if(rooms[roomId]===r) delete rooms[roomId]; }, 5000); },2100+180000+250);
}

function normalizeName(name) {
  return String(name || '').trim().slice(0, 20);
}
function key(name) { return normalizeName(name).toLowerCase(); }
function ensureUser(name) {
  const k = key(name);
  if (!friends.has(k)) friends.set(k, new Set());
  if (!friendRequests.has(k)) friendRequests.set(k, new Set());
}
const CUSTOMIZATION_OPTIONS = {
  frame: new Set(['frame-classic','frame-ice','frame-ruby','frame-gold','frame-neon','frame-aurora','frame-frost','frame-cosmic']),
  player: new Set(['player-blue','player-red','player-violet','player-mint','player-gold']),
  font: new Set(['font-ocean','font-royal','font-tropical','font-sunset','font-aqua','font-aurora','font-candy','font-icefire'])
};
function cleanCustomization(c={}) {
  return {
    frame: CUSTOMIZATION_OPTIONS.frame.has(c.frame) ? c.frame : 'frame-classic',
    player: CUSTOMIZATION_OPTIONS.player.has(c.player) ? c.player : 'player-blue',
    font: CUSTOMIZATION_OPTIONS.font.has(c.font) ? c.font : 'font-ocean'
  };
}
function emitFriendState(name) {
  const k = key(name);
  const socketId = onlineByName.get(k);
  const s = socketId && io.sockets.sockets.get(socketId);
  if (!s) return;
  ensureUser(name);
  const friendNames = [...friends.get(k)];
  const requests = [...friendRequests.get(k)];
  const list = friendNames.map(n => {
    const sid = onlineByName.get(key(n));
    const fsocket = sid && io.sockets.sockets.get(sid);
    return {
      name: n,
      avatar: fsocket?.playerAvatar || '',
      customization: fsocket?.playerCustomization || cleanCustomization(),
      online: !!fsocket
    };
  });
  s.emit('friend_state', { friends: list, requests });
}

// Single place that validates + applies a profile (used by set_profile, find_match,
// create_room and join_room, which previously had two diverging copies).
function acceptProfileForSocket(socket, data={}) {
  const oldName=socket.playerName;
  const name=normalizeName(data.name)||socket.playerName||'Player';
  const pid=profileKey(data)||socket.profileId||('legacy_'+socket.id);
  const nk=key(name);
  const reserved=reservedNames[nk];
  const ownsCurrentName = !!oldName && key(oldName) === nk;
  // FIX: a reserved name must be usable by the profile that reserved it. Previously a
  // page reload / reconnect (new socket, no playerName yet) was told its OWN username
  // was taken. A different profileId still can never claim a reserved name.
  const ownsReservation = !!reserved && reserved === pid;
  if (reserved && !ownsCurrentName && !ownsReservation) return {ok:false,message:'That username is already taken. Please choose another one.'};
  const onlineOwner=onlineByName.get(nk);
  if (onlineOwner && onlineOwner!==socket.id) {
    const stale = io.sockets.sockets.get(onlineOwner);
    // Same profile reconnecting while its old (dead/half-open) socket is still
    // registered: drop the stale socket instead of locking the player out.
    if (stale && stale.profileId === pid) { stale.disconnect(true); }
    else if (stale) return {ok:false,message:'That username is already taken. Please choose another one.'};
  }
  if(!reserved){ reservedNames[nk]=pid; saveReservedNames(); }
  socket.profileId=pid;
  if(oldName && key(oldName)!==nk && onlineByName.get(key(oldName))===socket.id) onlineByName.delete(key(oldName));
  socket.playerName=name; socket.playerAvatar=data.avatar||socket.playerAvatar||''; socket.playerCustomization=cleanCustomization(data.customization||socket.playerCustomization||{}); onlineByName.set(nk,socket.id); ensureUser(name);
  return {ok:true};
}

function inActiveRoom(socket) {
  for (const id in rooms) {
    const r = rooms[id];
    if (!r.ended && r.p2 && (r.p1 === socket.id || r.p2 === socket.id)) return true;
  }
  return false;
}
// Remove any not-yet-started room this socket created (so abandoned codes don't pile up).
function dropOwnPendingRooms(socket) {
  for (const id in rooms) {
    const r = rooms[id];
    if (r.p1 === socket.id && !r.p2) { delete rooms[id]; socket.leave(id); }
  }
}

io.on('connection', (socket) => {
  console.log('Player connected:', socket.id);

  socket.on('set_profile', (data = {}) => {
    const accepted = acceptProfileForSocket(socket, data);
    if (!accepted.ok) return socket.emit('name_taken', {message:'Username already exists.'});
    // Match cosmetics are intentionally immutable snapshots captured by startMatch().
    // Do not broadcast later profile edits into an active match.
    emitFriendState(socket.playerName);
    socket.emit('profile_accepted', {name:socket.playerName, profileId:socket.profileId});
  });

  socket.on('get_friend_state', () => emitFriendState(socket.playerName));

  socket.on('get_player_stats', ({name} = {}) => {
    const target = normalizeName(name); if (!target) return;
    const sid = onlineByName.get(key(target)); const targetSocket = sid && io.sockets.sockets.get(sid);
    socket.emit('player_stats', { name: target, stats: cleanStats(playerStats[key(target)]), avatar: targetSocket?.playerAvatar || '', online: !!targetSocket });
  });

  socket.on('get_leaderboard', (payload, callback) => {
    // Backward-compatible with old clients that emit only a callback.
    if(typeof payload === 'function'){ callback = payload; payload = {}; }
    const rows = getLeaderboardRows(payload?.period || 'all');
    if (typeof callback === 'function') callback(rows);
    socket.emit('leaderboard_data', rows);
  });

  socket.on('stats_update', ({stats} = {}) => {
    const name = normalizeName(socket.playerName); if (!name) return;
    const incoming=cleanStats(stats); const existing=cleanStats(playerStats[key(name)]); incoming.goals=Math.max(existing.goals,incoming.goals); incoming.name=name; playerStats[key(name)]=incoming; saveStats();
    socket.emit('stats_updated', {name, stats: playerStats[key(name)]});
  });

  socket.on('friend_request', ({ name } = {}) => {
    const target = normalizeName(name);
    const from = normalizeName(socket.playerName);
    if (!from || !target) return socket.emit('friend_error', { message: 'Enter a player name.' });
    if (key(from) === key(target)) return socket.emit('friend_error', { message: 'You cannot add yourself.' });
    const targetSocketId = onlineByName.get(key(target));
    if (!targetSocketId) return socket.emit('friend_error', { message: 'Player not found or offline.' });

    ensureUser(from); ensureUser(target);
    if (friends.get(key(from)).size >= MAX_FRIENDS) return socket.emit('friend_error', { message: 'Friend limit reached (250).' });
    if (friends.get(key(target)).size >= MAX_FRIENDS) return socket.emit('friend_error', { message: 'That player has reached the 250 friend limit.' });
    if (friends.get(key(from)).has(target)) return socket.emit('friend_error', { message: 'Already friends.' });
    if (friendRequests.get(key(target)).has(from)) return socket.emit('friend_error', { message: 'Request already sent.' });

    friendRequests.get(key(target)).add(from);
    socket.emit('friend_request_sent', { name: target });
    emitFriendState(target);
  });

  socket.on('friend_accept', ({ name } = {}) => {
    const me = normalizeName(socket.playerName);
    const other = normalizeName(name);
    if (!me || !other) return;
    ensureUser(me); ensureUser(other);
    if (friends.get(key(me)).size >= MAX_FRIENDS || friends.get(key(other)).size >= MAX_FRIENDS) return socket.emit('friend_error',{message:'Friend limit reached (250).'});
    friendRequests.get(key(me)).delete(other);
    friends.get(key(me)).add(other);
    friends.get(key(other)).add(me);
    emitFriendState(me);
    emitFriendState(other);
  });

  socket.on('friend_decline', ({ name } = {}) => {
    const me = normalizeName(socket.playerName);
    if (!me || !name) return;
    ensureUser(me);
    friendRequests.get(key(me)).delete(normalizeName(name));
    emitFriendState(me);
  });

  socket.on('chat_message', (data = {}) => {
    const from = normalizeName(socket.playerName);
    const to = normalizeName(data.to);
    if (!from || !to) return;
    ensureUser(from); ensureUser(to);
    if (!friends.get(key(from)).has(to)) return;
    const targetId = onlineByName.get(key(to));
    if (!targetId) return socket.emit('chat_error', { message: 'Friend is offline.' });

    const payload = {
      id: `${Date.now()}_${Math.random().toString(36).slice(2)}`,
      from,
      to,
      text: String(data.text || '').slice(0, 2000),
      attachment: data.attachment || null,
      avatar: socket.playerAvatar || '',
      customization: socket.playerCustomization || cleanCustomization(),
      time: Date.now()
    };
    if (payload.attachment?.data && payload.attachment.data.length > 7e6) {
      return socket.emit('chat_error', { message: 'Attachment is too large.' });
    }
    pushChatMessage(from,to,payload);
    socket.emit('chat_message', payload);
    io.sockets.sockets.get(targetId)?.emit('chat_message', payload);
  });

  socket.on('chat_history', ({with: other}={}) => {
    const me=normalizeName(socket.playerName), target=normalizeName(other);
    if(!me||!target) return; ensureUser(me);
    if(!friends.get(key(me)).has(target)) return socket.emit('chat_history',[]);
    socket.emit('chat_history', chatHistory.get(chatKey(me,target)) || []);
  });

  socket.on('rematch_challenge', ({name}={}) => {
    const from=normalizeName(socket.playerName), target=normalizeName(name);
    if(!from||!target) return; ensureUser(from);
    const targetId=onlineByName.get(key(target));
    if(!targetId) return socket.emit('friend_error',{message:'Player is offline.'});
    rematchChallenges.set(key(target),from);
    setTimeout(() => { if (rematchChallenges.get(key(target)) === from) rematchChallenges.delete(key(target)); }, 30000);
    io.sockets.sockets.get(targetId)?.emit('rematch_challenge',{from});
    socket.emit('rematch_sent',{name:target});
  });

  socket.on('rematch_reject', ({from}={}) => {
    const me=normalizeName(socket.playerName), challenger=normalizeName(from);
    if(!me||!challenger) return; rematchChallenges.delete(key(me));
    const sid=onlineByName.get(key(challenger)); if(sid) io.sockets.sockets.get(sid)?.emit('rematch_rejected',{name:me});
  });

  socket.on('rematch_accept', ({from}={}) => {
    const me=normalizeName(socket.playerName), challenger=normalizeName(from);
    if(!me||!challenger) return;
    if(rematchChallenges.get(key(me))!==challenger) return;
    rematchChallenges.delete(key(me));
    const sid=onlineByName.get(key(challenger)); const other=sid&&io.sockets.sockets.get(sid);
    if(!other) return socket.emit('friend_error',{message:'Challenger went offline.'});
    const roomId='rematch_'+socket.id+'_'+other.id+'_'+Date.now();
    startMatch(other,socket,roomId);
    other.emit('rematch_started',{room:roomId}); socket.emit('rematch_started',{room:roomId});
  });

  // Quick Match
  socket.on('find_match', (data = {}) => {
    // Refresh the matchmaking profile atomically with the request so each
    // side's equipped cosmetics are the exact ones captured for this match.
    if (data.profile && typeof data.profile === 'object') {
      const accepted=acceptProfileForSocket(socket,data.profile);
      if(!accepted.ok) return socket.emit('name_taken',{message:accepted.message});
    }
    if (inActiveRoom(socket)) return;
    // FIX: never pair with a waiting player whose connection has already dropped.
    if (waitingPlayer && !waitingPlayer.connected) waitingPlayer = null;
    if (waitingPlayer && waitingPlayer.id !== socket.id) {
      const roomId = 'room_' + socket.id + '_' + waitingPlayer.id;
      const p1 = waitingPlayer, p2 = socket;
      waitingPlayer = null;
      dropOwnPendingRooms(p1); dropOwnPendingRooms(p2);
      startMatch(p1,p2,roomId);
    } else {
      waitingPlayer = socket;
      socket.emit('waiting');
    }
  });

  socket.on('cancel_match', () => { if (waitingPlayer === socket) waitingPlayer = null; });

  socket.on('create_room', (data = {}) => {
    if (data.profile && typeof data.profile === 'object') { const accepted=acceptProfileForSocket(socket,data.profile); if(!accepted.ok) return socket.emit('name_taken',{message:accepted.message}); }
    if (inActiveRoom(socket)) return;
    if (waitingPlayer === socket) waitingPlayer = null;
    dropOwnPendingRooms(socket); // FIX: previous unused room codes were leaked
    let code;
    do { code = Math.random().toString(36).substring(2, 8).toUpperCase(); } while (!code || rooms[code]);
    socket.join(code);
    rooms[code] = { p1: socket.id, p2: null, startAt: null, ended: false, lastGoalAt: 0, scores: { left: 0, right: 0 } };
    socket.emit('room_created', { code });
  });

  socket.on('join_room', ({ code, profile } = {}) => {
    if (profile && typeof profile === 'object') { const accepted=acceptProfileForSocket(socket,profile); if(!accepted.ok) return socket.emit('name_taken',{message:accepted.message}); }
    const roomCode = String(code || '').trim().toUpperCase();
    const room = rooms[roomCode];
    const err = () => socket.emit('join_error', { message: 'Room not found or game already full!' });
    if (!room || room.p2 || room.ended) return err();
    // FIX: could join your own room, and used to mark the room full even when the host was gone.
    if (room.p1 === socket.id) return socket.emit('join_error', { message: 'You cannot join your own room.' });
    const p1Socket = io.sockets.sockets.get(room.p1);
    if (!p1Socket || !p1Socket.connected) { delete rooms[roomCode]; return err(); }
    if (waitingPlayer === socket) waitingPlayer = null;
    startMatch(p1Socket, socket, roomCode);
  });

  // --- Realtime packets: volatile (dropped, not queued, when the link is congested)
  // and only accepted from the correct player.
  socket.on('player_move', data => {
    const room=rooms[data?.room]; if(!room||room.ended) return;
    if(socket.id!==room.p1 && socket.id!==room.p2) return;
    const x=Number(data.x), y=Number(data.y); if(!Number.isFinite(x)||!Number.isFinite(y)) return;
    socket.to(data.room).volatile.emit('opponent_move',{x,y});
  });
  socket.on('puck_update', data => {
    const room=rooms[data?.room]; if(!room||room.ended) return;
    // FIX: only the puck authority (left / p1) may broadcast puck state. Previously any
    // client in the room could overwrite the puck for everybody.
    if(socket.id!==room.p1) return;
    const x=Number(data.x), y=Number(data.y), vx=Number(data.vx), vy=Number(data.vy);
    if(![x,y,vx,vy].every(Number.isFinite)) return;
    socket.to(data.room).volatile.emit('puck_update',{room:data.room,x,y,vx,vy});
  });
  socket.on('score_update', data => {
    const room=rooms[data?.room]; if(!room || room.ended || !room.p2) return;
    // Only the left-side client simulates puck physics and may report a goal.
    // This avoids both clients incrementing the score for the same puck crossing.
    if(socket.id !== room.p1) return;
    const scorerSide=data?.scorer;
    if(scorerSide!=='left' && scorerSide!=='right') return;
    // FIX: ignore duplicate goal reports for the same crossing (lag / double emit).
    const now=Date.now();
    if(now-(room.lastGoalAt||0) < GOAL_COOLDOWN_MS) return;
    room.lastGoalAt=now;
    if(!room.scores) room.scores={left:0,right:0};
    // Accept exactly one goal at a time; ignore stale, duplicate, or fabricated score totals.
    room.scores[scorerSide] += 1;
    const scorerId=scorerSide==='left'?room.p1:room.p2;
    const scorer=io.sockets.sockets.get(scorerId);
    if(scorer?.playerName){
      const k=key(scorer.playerName); const st=cleanStats(playerStats[k]);
      st.goals++; st.name=scorer.playerName; playerStats[k]=st; saveStats(); recordGoal(scorer.playerName);
      scorer.emit('stats_updated',{name:scorer.playerName,stats:st});
    }
    io.to(data.room).emit('score_update', {room:data.room, scores:{...room.scores}, scorer:scorerSide});
  });

  socket.on('rewarded_ad_status', () => {
    const name=normalizeName(socket.playerName); if(!name) return socket.emit('rewarded_ad_status',{count:0,remaining:2});
    const day=rewardDay(); const entry=rewardedAds[key(name)];
    const count=entry?.day===day ? Number(entry.count)||0 : 0;
    socket.emit('rewarded_ad_status',{count,remaining:Math.max(0,2-count),day});
  });

  socket.on('claim_rewarded_ad', (data={},ack) => {
    const name=normalizeName(socket.playerName); if(!name){ if(typeof ack==='function')ack({granted:false,remaining:0,message:'Profile not ready.'}); return; }
    const day=rewardDay(); const k=key(name); const entry=rewardedAds[k];
    const count=entry?.day===day ? Number(entry.count)||0 : 0;
    if(count>=2){ if(typeof ack==='function')ack({granted:false,remaining:0,message:'Daily limit reached. You can watch 2 rewarded ads per day.'}); return; }
    rewardedAds[k]={day,count:count+1,profileId:socket.profileId||''}; saveRewardedAds();
    if(typeof ack==='function')ack({granted:true,remaining:1-count});
  });

  socket.on('disconnect', () => {
    if (waitingPlayer === socket) waitingPlayer = null;
    if (socket.playerName && onlineByName.get(key(socket.playerName)) === socket.id) {
      onlineByName.delete(key(socket.playerName));
      emitFriendState(socket.playerName);
      // Drop rematch challenges involving this player so they can't be accepted later.
      const nk = key(socket.playerName);
      rematchChallenges.delete(nk);
      for (const [target, challenger] of rematchChallenges) if (key(challenger) === nk) rematchChallenges.delete(target);
    }
    for (const roomId in rooms) {
      const r = rooms[roomId];
      if (r.p1 === socket.id || r.p2 === socket.id) {
        // FIX: only tell the opponent when the match was actually still running. Sending it
        // after the match ended made the client alert + reload on the game-over screen.
        if (!r.ended && r.p2) io.to(roomId).emit('opponent_disconnected');
        delete rooms[roomId];
      }
    }
    console.log('Player disconnected:', socket.id);
  });
});

const PORT = process.env.PORT || 3000;

// A Vercel Function needs the http.Server handed back via module.exports, but Vercel
// cannot host persistent WebSockets or shared in-memory state. This server is meant to
// run as a normal long-lived Node process (Render/Railway/Fly/VPS).
if (!process.env.VERCEL) {
  server.listen(PORT, () => console.log(`Ice Hockey server running on ${PORT}`));
}

module.exports = server;
