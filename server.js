require('dotenv').config();
const express    = require('express');
const http       = require('http');
const { Server } = require('socket.io');
const cors       = require('cors');
const { v4: uuidv4 } = require('uuid');

const app    = express();
const server = http.createServer(app);
const io     = new Server(server, {
  cors: { origin:'*', methods:['GET','POST'] },
  pingTimeout: 60000,
  pingInterval: 25000,
});
app.use(cors());
app.use(express.json());
app.get('/', (_,res) => res.json({ status:'🃏 Ace Spades v2.0', features:17 }));

// ═══════════════════════════════════════════════════════════════
// DATA STORES
// ═══════════════════════════════════════════════════════════════
const rooms        = new Map();
const playerRooms  = new Map();
const profiles     = new Map();
const friendships  = new Map();
const chatHistory  = new Map();
const weeklyBoard  = new Map();
const globalBoard  = new Map();
const tournaments  = new Map();
const replays      = new Map();
const spectators   = new Map(); // roomId -> Set of socketIds
const rankedQueue  = [];
const missionsDB   = new Map();
const notifications= new Map(); // uid -> []

// ═══════════════════════════════════════════════════════════════
// GAME ENGINE
// ═══════════════════════════════════════════════════════════════
const SUITS  = ['♠','♥','♦','♣'];
const RANKS  = ['2','3','4','5','6','7','8','9','10','J','Q','K','A'];
const RANK_V = {'2':2,'3':3,'4':4,'5':5,'6':6,'7':7,'8':8,'9':9,'10':10,'J':11,'Q':12,'K':13,'A':14};

const buildDeck  = () => SUITS.flatMap(s => RANKS.map(r => ({suit:s,rank:r,val:RANK_V[r]})));
const shuffled   = a  => { const b=[...a]; for(let i=b.length-1;i>0;i--){const j=Math.floor(Math.random()*(i+1));[b[i],b[j]]=[b[j],b[i]];} return b; };
const sortHand   = h  => [...h].sort((a,b) => ({'♠':0,'♥':1,'♦':2,'♣':3}[a.suit]-{'♠':0,'♥':1,'♦':2,'♣':3}[b.suit])||b.val-a.val);
const cKey       = c  => c.suit+c.rank;
const highestOf  = (played,suit) => { const m=played.filter(p=>p.card.suit===suit); return m.length?m.reduce((b,p)=>p.card.val>b.card.val?p:b):null; };
const goneSuits  = h  => SUITS.filter(s=>Object.values(h).every(hand=>!hand.some(c=>c.suit===s)));
const scorePts   = (pos,n) => pos===n-1?-15:([1,2,4,6,8,10,12,14][pos]??1);

function dealCards(n) {
  const deck  = shuffled(buildDeck());
  const hands = Array.from({length:n},()=>[]);
  deck.forEach((c,i) => hands[i%n].push(c));
  return hands.map(h => sortHand(h));
}

function validCards(hand, leadSuit, isLeader) {
  if (isLeader) return hand.map(cKey);
  const has = hand.some(c=>c.suit===leadSuit);
  return has ? hand.filter(c=>c.suit===leadSuit).map(cKey) : hand.map(cKey);
}

// ═══════════════════════════════════════════════════════════════
// RANK SYSTEM
// ═══════════════════════════════════════════════════════════════
const RANK_TIERS = [
  {min:0,    title:'Rookie',     icon:'🌱', color:'#94a3b8'},
  {min:1100, title:'Amateur',    icon:'🃏', color:'#4ade80'},
  {min:1300, title:'Skilled',    icon:'⚔️', color:'#60a5fa'},
  {min:1500, title:'Expert',     icon:'🎯', color:'#f59e0b'},
  {min:1700, title:'Master',     icon:'👑', color:'#a78bfa'},
  {min:1900, title:'Grandmaster',icon:'💎', color:'#f472b6'},
  {min:2200, title:'Legend',     icon:'🏆', color:'#f87171'},
];
const getRankTier = rp => [...RANK_TIERS].reverse().find(t=>rp>=t.min)||RANK_TIERS[0];

// ═══════════════════════════════════════════════════════════════
// ACHIEVEMENTS
// ═══════════════════════════════════════════════════════════════
const ACHIEVEMENTS = [
  {id:'first_game',   name:'First Game',      desc:'Play your first game',            icon:'🃏', reward:50,   type:'games',   threshold:1},
  {id:'ten_games',    name:'Veteran',         desc:'Play 10 games',                   icon:'⚔️', reward:100,  type:'games',   threshold:10},
  {id:'fifty_games',  name:'Dedicated',       desc:'Play 50 games',                   icon:'🎯', reward:300,  type:'games',   threshold:50},
  {id:'first_best',   name:'Top of the Table',desc:'Finish 2nd to last (best score)', icon:'🏆', reward:100,  type:'wins',    threshold:1},
  {id:'ten_wins',     name:'Sharp Mind',      desc:'Get best score 10 times',         icon:'👑', reward:500,  type:'wins',    threshold:10},
  {id:'no_collect',   name:'Clean Streak',    desc:'Finish a game without collecting',icon:'✨', reward:200,  type:'special', threshold:1},
  {id:'streak_3',     name:'On Fire',         desc:'Get best score 3 games in a row', icon:'🔥', reward:300,  type:'streak',  threshold:3},
  {id:'streak_5',     name:'Unstoppable',     desc:'5 best scores in a row',          icon:'⚡', reward:500,  type:'streak',  threshold:5},
  {id:'transfer_win', name:'Card Master',     desc:'Win via hand transfer',           icon:'🤝', reward:150,  type:'special', threshold:1},
  {id:'tournament_1', name:'Contender',       desc:'Enter a tournament',              icon:'🏟️', reward:100,  type:'special', threshold:1},
  {id:'tournament_win',name:'Champion',       desc:'Win a tournament',                icon:'🥇', reward:1000, type:'special', threshold:1},
  {id:'friend_5',     name:'Social Butterfly',desc:'Add 5 friends',                   icon:'👥', reward:100,  type:'friends', threshold:5},
  {id:'coins_1000',   name:'Coin Collector',  desc:'Earn 1000 coins total',           icon:'🪙', reward:0,    type:'coins',   threshold:1000},
  {id:'legend_rank',  name:'Living Legend',   desc:'Reach Legend rank',               icon:'💫', reward:2000, type:'rank',    threshold:2200},
];

function checkAchievements(uid) {
  const p = profiles.get(uid);
  if (!p) return [];
  const unlocked = [];
  ACHIEVEMENTS.forEach(ach => {
    if (p.achievements?.includes(ach.id)) return;
    let earned = false;
    if (ach.type==='games'   && p.gamesPlayed >= ach.threshold)   earned=true;
    if (ach.type==='wins'    && p.wins        >= ach.threshold)   earned=true;
    if (ach.type==='streak'  && p.bestStreak  >= ach.threshold)   earned=true;
    if (ach.type==='friends' && (p.friendCount||0) >= ach.threshold) earned=true;
    if (ach.type==='coins'   && p.totalCoinsEarned >= ach.threshold) earned=true;
    if (ach.type==='rank'    && p.rankPoints  >= ach.threshold)   earned=true;
    if (earned) {
      if (!p.achievements) p.achievements = [];
      p.achievements.push(ach.id);
      p.coins += ach.reward;
      p.totalCoinsEarned = (p.totalCoinsEarned||0) + ach.reward;
      unlocked.push(ach);
    }
  });
  return unlocked;
}

// ═══════════════════════════════════════════════════════════════
// DAILY MISSIONS
// ═══════════════════════════════════════════════════════════════
const MISSION_POOL = [
  {id:'play_3',      name:'Play 3 Games',          desc:'Complete 3 games today',          reward:150,  type:'games',    target:3},
  {id:'play_5',      name:'Play 5 Games',          desc:'Complete 5 games today',          reward:300,  type:'games',    target:5},
  {id:'win_1',       name:'Best Score',            desc:'Finish 2nd to last once',         reward:200,  type:'wins',     target:1},
  {id:'win_3',       name:'Triple Crown',          desc:'Get best score 3 times',          reward:500,  type:'wins',     target:3},
  {id:'clean_2',     name:'Clean Sweep',           desc:'Trigger 2 clean rounds',          reward:200,  type:'clean',    target:2},
  {id:'transfer_1',  name:'Deal Maker',            desc:'Accept or request 1 transfer',    reward:150,  type:'transfer', target:1},
  {id:'ranked_1',    name:'Ranked Player',         desc:'Play 1 ranked game',              reward:200,  type:'ranked',   target:1},
  {id:'chat_5',      name:'Chatterbox',            desc:'Send 5 chat messages',            reward:50,   type:'chat',     target:5},
  {id:'survive_10',  name:'Survivor',              desc:'Survive 10+ rounds in one game',  reward:250,  type:'survive',  target:10},
  {id:'no_last_3',   name:'Not Last!',             desc:'Avoid last place 3 games in a row',reward:300, type:'notlast',  target:3},
];

function getDailyMissions(uid) {
  const today = new Date().toDateString();
  const key   = `${uid}_${today}`;
  if (missionsDB.has(key)) return missionsDB.get(key);
  // Pick 3 random missions
  const pool    = shuffled([...MISSION_POOL]).slice(0,3);
  const missions = pool.map(m => ({...m, progress:0, completed:false, claimed:false}));
  missionsDB.set(key, missions);
  return missions;
}

function updateMissionProgress(uid, type, amount=1) {
  const today    = new Date().toDateString();
  const key      = `${uid}_${today}`;
  const missions = missionsDB.get(key);
  if (!missions) return [];
  const justCompleted = [];
  missions.forEach(m => {
    if (m.completed || m.type !== type) return;
    m.progress = Math.min(m.target, m.progress + amount);
    if (m.progress >= m.target) { m.completed = true; justCompleted.push(m); }
  });
  return justCompleted;
}

// ═══════════════════════════════════════════════════════════════
// SHOP ITEMS
// ═══════════════════════════════════════════════════════════════
const SHOP_ITEMS = [
  // Card Skins (coins)
  {id:'skin_gold',      type:'skin',   name:'Gold Cards',     price:500,   currency:'coins', preview:'🟡', rarity:'rare',      desc:'Shimmering gold card backs'},
  {id:'skin_neon',      type:'skin',   name:'Neon Cards',     price:800,   currency:'coins', preview:'⚡', rarity:'rare',      desc:'Electric neon glow'},
  {id:'skin_royal',     type:'skin',   name:'Royal Cards',    price:1200,  currency:'coins', preview:'👑', rarity:'legendary', desc:'Majestic royal design'},
  {id:'skin_midnight',  type:'skin',   name:'Midnight Cards', price:600,   currency:'coins', preview:'🌙', rarity:'rare',      desc:'Dark midnight theme'},
  {id:'skin_fire',      type:'skin',   name:'Fire Cards',     price:15,    currency:'gems',  preview:'🔥', rarity:'epic',      desc:'Blazing fire card backs'},
  {id:'skin_diamond',   type:'skin',   name:'Diamond Cards',  price:25,    currency:'gems',  preview:'💎', rarity:'epic',      desc:'Crystalline diamond pattern'},
  {id:'skin_galaxy',    type:'skin',   name:'Galaxy Cards',   price:40,    currency:'gems',  preview:'🌌', rarity:'legendary', desc:'Cosmic galaxy theme'},
  // Avatars (coins)
  {id:'avatar_ninja',   type:'avatar', name:'Ninja',          price:300,   currency:'coins', preview:'🥷', rarity:'rare',      desc:'Stealthy card ninja'},
  {id:'avatar_wizard',  type:'avatar', name:'Wizard',         price:400,   currency:'coins', preview:'🧙', rarity:'rare',      desc:'Mystical card wizard'},
  {id:'avatar_robot',   type:'avatar', name:'Robot',          price:250,   currency:'coins', preview:'🤖', rarity:'common',    desc:'AI card player'},
  {id:'avatar_alien',   type:'avatar', name:'Alien',          price:350,   currency:'coins', preview:'👽', rarity:'rare',      desc:'Extraterrestrial strategist'},
  {id:'avatar_pirate',  type:'avatar', name:'Pirate',         price:300,   currency:'coins', preview:'🏴‍☠️',rarity:'rare',     desc:'Swashbuckling card shark'},
  {id:'avatar_king',    type:'avatar', name:'King',           price:20,    currency:'gems',  preview:'🤴', rarity:'epic',      desc:'Royalty at the table'},
  {id:'avatar_demon',   type:'avatar', name:'Demon',          price:30,    currency:'gems',  preview:'😈', rarity:'epic',      desc:'Diabolical card master'},
  // Coin packs (IAP)
  {id:'coins_500',      type:'coins',  name:'500 Coins',      price:0.99,  currency:'usd',   preview:'🪙', rarity:'common',    desc:'Starter coin pack'},
  {id:'coins_2500',     type:'coins',  name:'2500 Coins',     price:3.99,  currency:'usd',   preview:'💰', rarity:'common',    desc:'Value coin pack'},
  {id:'coins_10000',    type:'coins',  name:'10,000 Coins',   price:9.99,  currency:'usd',   preview:'💎', rarity:'rare',      desc:'Best value!'},
  // Gem packs (IAP)
  {id:'gems_50',        type:'gems',   name:'50 Gems',        price:1.99,  currency:'usd',   preview:'💎', rarity:'common',    desc:'Small gem pack'},
  {id:'gems_200',       type:'gems',   name:'200 Gems',       price:6.99,  currency:'usd',   preview:'💎', rarity:'rare',      desc:'Popular gem pack'},
  {id:'gems_500',       type:'gems',   name:'500 Gems',       price:14.99, currency:'usd',   preview:'💎', rarity:'epic',      desc:'Mega gem pack'},
];

// ═══════════════════════════════════════════════════════════════
// ANTI-CHEAT
// ═══════════════════════════════════════════════════════════════
const suspiciousActivity = new Map(); // uid -> {count, lastFlag, flags:[]}
const BANNED_UIDS = new Set();

function flagSuspicious(uid, reason, roomId) {
  if (!suspiciousActivity.has(uid)) suspiciousActivity.set(uid, {count:0, flags:[]});
  const rec = suspiciousActivity.get(uid);
  rec.count++;
  rec.flags.push({reason, roomId, time:Date.now()});
  rec.lastFlag = Date.now();
  console.warn(`[AntiCheat] Flagged ${uid}: ${reason} (count: ${rec.count})`);
  if (rec.count >= 5) {
    BANNED_UIDS.add(uid);
    console.warn(`[AntiCheat] BANNED ${uid}`);
    return true; // banned
  }
  return false;
}

function antiCheatCheck(socket, uid, action, data, room) {
  // Check banned
  if (BANNED_UIDS.has(uid)) {
    socket.emit('banned', {message:'You have been banned for suspicious activity'});
    socket.disconnect();
    return false;
  }
  // Check it's actually their turn
  if (action==='play_card' && room?.currentTurn !== uid) {
    flagSuspicious(uid, 'played_out_of_turn', room?.id);
    return false;
  }
  // Check card actually in hand
  if (action==='play_card' && room) {
    const hand = room.hands[uid]||[];
    if (!hand.some(c=>cKey(c)===data.cardKey)) {
      flagSuspicious(uid, 'played_card_not_in_hand', room?.id);
      return false;
    }
  }
  // Rate limiting — max 10 actions per second
  const now = Date.now();
  const rec = suspiciousActivity.get(uid)||{count:0,flags:[],lastAction:0,actionCount:0};
  if (now - rec.lastAction < 1000) {
    rec.actionCount = (rec.actionCount||0)+1;
    if (rec.actionCount > 10) {
      flagSuspicious(uid, 'rate_limit_exceeded', room?.id);
      return false;
    }
  } else { rec.actionCount=0; }
  rec.lastAction = now;
  suspiciousActivity.set(uid, rec);
  return true;
}

// ═══════════════════════════════════════════════════════════════
// PROFILE HELPERS
// ═══════════════════════════════════════════════════════════════
function getOrCreateProfile(uid, name, avatar) {
  if (!profiles.has(uid)) {
    profiles.set(uid, {
      uid, name: name||'Player', avatar: avatar||'🧑',
      createdAt:Date.now(), lastSeen:Date.now(),
      gamesPlayed:0, wins:0, losses:0, totalPoints:0,
      bestStreak:0, currentStreak:0, notLastStreak:0,
      rankPoints:1000,
      coins:500, gems:0,
      totalCoinsEarned:500,
      ownedSkins:['default'], ownedAvatars:[],
      activeSkin:'default',
      achievements:[],
      friendCount:0,
      recentGames:[],
    });
  }
  const p = profiles.get(uid);
  p.lastSeen = Date.now();
  if(name)   p.name   = name;
  if(avatar) p.avatar = avatar;
  return p;
}

function recordGameResult(uid, position, totalPlayers, points, roundsPlayed, isRanked, cleanRounds, transfers) {
  const p = profiles.get(uid);
  if (!p) return {newAchievements:[]};

  const isLast = position === totalPlayers-1;
  const isBest = position === totalPlayers-2;

  p.gamesPlayed++;
  p.totalPoints += points;

  // Streak tracking
  if (isBest) {
    p.wins++;
    p.currentStreak++;
    p.bestStreak = Math.max(p.bestStreak, p.currentStreak);
    p.notLastStreak++;
  } else if (isLast) {
    p.losses++;
    p.currentStreak  = 0;
    p.notLastStreak  = 0;
  } else {
    p.currentStreak = 0;
    p.notLastStreak++;
  }

  // ELO rank update
  const exp = 1/(1+Math.pow(10,(1200-p.rankPoints)/400));
  const act = isBest?1:isLast?0:0.5;
  const k   = p.gamesPlayed<10?40:20;
  p.rankPoints = Math.max(0, Math.round(p.rankPoints + k*(act-exp)));

  // Coins reward
  const baseCoins = isBest?120:isLast?15:60;
  const rankBonus = isRanked?50:0;
  const totalCoins = baseCoins + rankBonus;
  p.coins += totalCoins;
  p.totalCoinsEarned = (p.totalCoinsEarned||0) + totalCoins;

  // Record game
  p.recentGames.unshift({
    date:Date.now(), position:position+1,
    totalPlayers, points, roundsPlayed,
    result:isBest?'best':isLast?'loss':'mid',
    coinsEarned:totalCoins, isRanked,
  });
  if(p.recentGames.length>20) p.recentGames.pop();

  // Mission progress
  updateMissionProgress(uid, 'games', 1);
  if(isBest)   updateMissionProgress(uid, 'wins', 1);
  if(!isLast)  updateMissionProgress(uid, 'notlast', 1);
  if(isRanked) updateMissionProgress(uid, 'ranked', 1);
  updateMissionProgress(uid, 'clean', cleanRounds||0);
  updateMissionProgress(uid, 'survive', roundsPlayed||0);

  // Leaderboards
  updateLeaderboards(uid, p.name, p.avatar, points, isBest);

  // Achievements
  const newAchievements = checkAchievements(uid);

  return { newAchievements, coinsEarned:totalCoins };
}

function updateLeaderboards(uid, name, avatar, points, isBest) {
  // Weekly
  const wb = weeklyBoard.get(uid)||{uid,name,avatar,points:0,wins:0,gamesPlayed:0};
  wb.points+=points; wb.gamesPlayed++; if(isBest)wb.wins++;
  wb.name=name; wb.avatar=avatar;
  weeklyBoard.set(uid,wb);
  // Global
  const gb = globalBoard.get(uid)||{uid,name,avatar,points:0,wins:0,gamesPlayed:0};
  gb.points+=points; gb.gamesPlayed++; if(isBest)gb.wins++;
  gb.name=name; gb.avatar=avatar;
  globalBoard.set(uid,gb);
}

// Weekly reset every Monday
function scheduleWeeklyReset() {
  const now  = new Date();
  const next = new Date(now);
  next.setDate(now.getDate() + (1+7-now.getDay())%7||7);
  next.setHours(0,0,0,0);
  setTimeout(()=>{ weeklyBoard.clear(); scheduleWeeklyReset(); }, next-now);
}
scheduleWeeklyReset();

// ═══════════════════════════════════════════════════════════════
// ROOM HELPERS
// ═══════════════════════════════════════════════════════════════
function genCode() {
  const c='ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  return Array.from({length:6},()=>c[Math.floor(Math.random()*c.length)]).join('');
}

function broadcastRoom(room) {
  // Send to players
  room.players.forEach(p => {
    const state = buildRoomState(room, p.uid);
    io.to(p.socketId).emit('room_state', state);
  });
  // Send to spectators
  const specs = spectators.get(room.id);
  if (specs?.size) {
    const specState = buildRoomState(room, null);
    specs.forEach(sid => io.to(sid).emit('spectate_state', specState));
  }
}

function buildRoomState(room, uid) {
  return {
    id:          room.id,
    isRanked:    room.isRanked,
    state:       room.state,
    phase:       room.phase,
    myHand:      uid ? room.hands[uid]||[] : [],
    table:       room.table,
    leadSuit:    room.leadSuit,
    leader:      room.leader,
    currentTurn: room.currentTurn,
    finishOrder: room.finishOrder,
    gone:        room.gone,
    roundNo:     room.roundNo,
    scores:      room.scores,
    players:     room.players.map(pl => ({
      uid:       pl.uid,
      name:      pl.name,
      avatar:    pl.avatar,
      ready:     pl.ready,
      connected: pl.connected,
      cardCount: (room.hands[pl.uid]||[]).length,
      finished:  room.finishOrder.includes(pl.uid),
      activeSkin:profiles.get(pl.uid)?.activeSkin||'default',
      rankTier:  getRankTier(profiles.get(pl.uid)?.rankPoints||1000),
    })),
  };
}

function systemChat(roomId, text) {
  const msg = {id:uuidv4(),uid:'system',name:'Game',avatar:'🃏',text,timestamp:Date.now(),type:'system'};
  if(!chatHistory.has(roomId)) chatHistory.set(roomId,[]);
  chatHistory.get(roomId).push(msg);
  io.to(roomId).emit('chat_message', msg);
}

function pushNotification(uid, title, body, data={}) {
  const notifs = notifications.get(uid)||[];
  notifs.unshift({id:uuidv4(),title,body,data,read:false,timestamp:Date.now()});
  if(notifs.length>50) notifs.pop();
  notifications.set(uid, notifs);
  // Find socket for this uid and emit
  for(const [sid,rid] of playerRooms) {
    const room = rooms.get(rid);
    const p = room?.players.find(pl=>pl.uid===uid);
    if(p?.socketId===sid) {
      io.to(sid).emit('notification', {title,body,data});
      break;
    }
  }
}

// ═══════════════════════════════════════════════════════════════
// SOCKET EVENTS
// ═══════════════════════════════════════════════════════════════
io.on('connection', socket => {
  console.log(`[+] ${socket.id}`);

  const err  = msg => socket.emit('error',{message:msg});
  const myRm = ()  => rooms.get(playerRooms.get(socket.id));
  const myPl = r   => r?.players.find(p=>p.socketId===socket.id);

  // ── PROFILE ──────────────────────────────────────────────────
  socket.on('get_profile', ({uid,name,avatar}) => {
    const p = getOrCreateProfile(uid,name,avatar);
    const rank = getRankTier(p.rankPoints);
    socket.emit('profile_data', {profile:{...p, rankTier:rank}});
  });

  socket.on('get_player_profile', ({uid}) => {
    const p = profiles.get(uid);
    if(!p) return socket.emit('player_profile',{error:'Not found'});
    socket.emit('player_profile',{profile:{
      uid:p.uid,name:p.name,avatar:p.avatar,activeSkin:p.activeSkin,
      gamesPlayed:p.gamesPlayed,wins:p.wins,losses:p.losses,
      totalPoints:p.totalPoints,rankPoints:p.rankPoints,
      rankTier:getRankTier(p.rankPoints),
      bestStreak:p.bestStreak,achievements:p.achievements||[],
      recentGames:p.recentGames.slice(0,10),
    }});
  });

  socket.on('update_profile', ({uid,name,avatar,activeSkin,activeAvatar}) => {
    const p = profiles.get(uid); if(!p) return;
    if(name?.trim())   p.name = name.trim().slice(0,16);
    if(avatar)         p.avatar = avatar;
    if(activeSkin && (p.ownedSkins||[]).includes(activeSkin)) p.activeSkin = activeSkin;
    if(activeAvatar && (p.ownedAvatars||[]).includes(activeAvatar)) p.avatar = activeAvatar;
    socket.emit('profile_updated',{profile:p});
  });

  // ── FRIENDS ──────────────────────────────────────────────────
  socket.on('send_friend_request', ({fromUid,toUid,fromName,fromAvatar}) => {
    const key = [fromUid,toUid].sort().join('_');
    if(friendships.has(key)) return err('Already friends or request pending');
    friendships.set(key, {user1:fromUid,user2:toUid,status:'pending',initiator:fromUid,createdAt:Date.now()});
    pushNotification(toUid,'Friend Request',`${fromName} sent you a friend request`,{type:'friend_request',fromUid,fromName,fromAvatar});
    socket.emit('friend_request_sent',{toUid});
  });

  socket.on('respond_friend_request', ({uid,fromUid,accept}) => {
    const key = [uid,fromUid].sort().join('_');
    const fs  = friendships.get(key); if(!fs) return err('Request not found');
    if(accept) {
      fs.status = 'friends';
      fs.acceptedAt = Date.now();
      const p1 = profiles.get(uid);
      const p2 = profiles.get(fromUid);
      if(p1) p1.friendCount = (p1.friendCount||0)+1;
      if(p2) p2.friendCount = (p2.friendCount||0)+1;
      checkAchievements(uid); checkAchievements(fromUid);
      pushNotification(fromUid,'Friend Accepted',`${p1?.name||uid} accepted your friend request`,{type:'friend_accepted',uid});
      socket.emit('friend_accepted',{friendUid:fromUid,friendName:p2?.name});
    } else {
      friendships.delete(key);
      socket.emit('friend_declined',{fromUid});
    }
  });

  socket.on('get_friends', ({uid}) => {
    const friends = [];
    for(const [,fs] of friendships) {
      if(fs.status!=='friends') continue;
      const friendUid = fs.user1===uid?fs.user2:fs.user1;
      if(fs.user1!==uid && fs.user2!==uid) continue;
      const fp = profiles.get(friendUid);
      if(fp) friends.push({uid:friendUid,name:fp.name,avatar:fp.avatar,rankTier:getRankTier(fp.rankPoints),online:false});
    }
    socket.emit('friends_list',{friends});
  });

  socket.on('invite_friend', ({fromUid,toUid,roomId,fromName}) => {
    pushNotification(toUid,'Game Invite',`${fromName} invited you to a game!`,{type:'room_invite',roomId,fromUid,fromName});
    socket.emit('invite_sent',{toUid});
  });

  // ── CHAT ─────────────────────────────────────────────────────
  socket.on('chat_send', ({text,roomId}) => {
    if(!text?.trim()||text.length>200) return;
    const room = rooms.get(roomId); if(!room) return;
    const player = myPl(room); if(!player) return;
    updateMissionProgress(player.uid,'chat',1);
    const msg = {id:uuidv4(),uid:player.uid,name:player.name,avatar:player.avatar,text:text.trim(),timestamp:Date.now(),type:'player'};
    if(!chatHistory.has(roomId)) chatHistory.set(roomId,[]);
    const hist = chatHistory.get(roomId);
    hist.push(msg);
    if(hist.length>100) hist.shift();
    io.to(roomId).emit('chat_message',msg);
  });

  socket.on('chat_emoji', ({emoji,roomId}) => {
    const ALLOWED=['👍','👎','😂','😮','😢','🔥','♠','🃏','💀','🏆','🤝','😡'];
    if(!ALLOWED.includes(emoji)) return;
    const room = rooms.get(roomId); if(!room) return;
    const player = myPl(room); if(!player) return;
    io.to(roomId).emit('chat_emoji',{uid:player.uid,name:player.name,avatar:player.avatar,emoji,timestamp:Date.now()});
  });

  socket.on('chat_history', ({roomId}) => {
    socket.emit('chat_history',{messages:chatHistory.get(roomId)||[]});
  });

  // ── NOTIFICATIONS ─────────────────────────────────────────────
  socket.on('get_notifications', ({uid}) => {
    socket.emit('notifications_list',{notifications:notifications.get(uid)||[]});
  });

  socket.on('mark_notifications_read', ({uid}) => {
    const n = notifications.get(uid)||[];
    n.forEach(notif => notif.read=true);
    notifications.set(uid,n);
  });

  socket.on('register_push_token', ({uid,token}) => {
    const p = profiles.get(uid); if(p) p.pushToken = token;
  });

  // ── MISSIONS ─────────────────────────────────────────────────
  socket.on('get_missions', ({uid}) => {
    const missions = getDailyMissions(uid);
    socket.emit('missions_data',{missions});
  });

  socket.on('claim_mission', ({uid,missionId}) => {
    const today    = new Date().toDateString();
    const key      = `${uid}_${today}`;
    const missions = missionsDB.get(key); if(!missions) return;
    const m        = missions.find(m=>m.id===missionId);
    if(!m||!m.completed||m.claimed) return err('Cannot claim');
    m.claimed = true;
    const p = profiles.get(uid); if(!p) return;
    p.coins += m.reward;
    p.totalCoinsEarned = (p.totalCoinsEarned||0)+m.reward;
    socket.emit('mission_claimed',{missionId,reward:m.reward,newCoins:p.coins});
  });

  // ── SHOP ─────────────────────────────────────────────────────
  socket.on('get_shop', ({uid}) => {
    const p = profiles.get(uid);
    const items = SHOP_ITEMS.map(item => ({
      ...item,
      owned: item.type==='skin'
        ? (p?.ownedSkins||[]).includes(item.id)
        : item.type==='avatar'
        ? (p?.ownedAvatars||[]).includes(item.id)
        : false,
    }));
    socket.emit('shop_data',{items, coins:p?.coins||0, gems:p?.gems||0});
  });

  socket.on('purchase_item', ({uid,itemId}) => {
    if(BANNED_UIDS.has(uid)) return err('Account suspended');
    const p    = profiles.get(uid); if(!p) return err('Profile not found');
    const item = SHOP_ITEMS.find(i=>i.id===itemId); if(!item) return err('Item not found');
    if(item.currency==='usd') return err('Use in-app purchase');
    if((p.ownedSkins||[]).includes(itemId)||(p.ownedAvatars||[]).includes(itemId)) return err('Already owned');
    if(item.currency==='coins' && p.coins<item.price) return err('Not enough coins');
    if(item.currency==='gems'  && p.gems <item.price) return err('Not enough gems');
    if(item.currency==='coins') p.coins -= item.price;
    if(item.currency==='gems')  p.gems  -= item.price;
    if(item.type==='skin')   p.ownedSkins   = [...(p.ownedSkins||[]),  itemId];
    if(item.type==='avatar') p.ownedAvatars = [...(p.ownedAvatars||[]),itemId];
    socket.emit('purchase_success',{item,profile:{coins:p.coins,gems:p.gems,ownedSkins:p.ownedSkins,ownedAvatars:p.ownedAvatars}});
  });

  // IAP grant (called after Google Play billing verification)
  socket.on('iap_grant', ({uid,itemId,receiptToken}) => {
    // In production: verify receipt with Google Play API first
    const p    = profiles.get(uid); if(!p) return;
    const item = SHOP_ITEMS.find(i=>i.id===itemId); if(!item) return;
    if(item.type==='coins') { p.coins += parseInt(item.name);  p.totalCoinsEarned=(p.totalCoinsEarned||0)+parseInt(item.name); }
    if(item.type==='gems')  p.gems += parseInt(item.name);
    socket.emit('iap_granted',{item,coins:p.coins,gems:p.gems});
  });

  // Ad reward
  socket.on('ad_reward', ({uid}) => {
    const p = profiles.get(uid); if(!p) return;
    const reward = 50;
    p.coins += reward;
    p.totalCoinsEarned = (p.totalCoinsEarned||0)+reward;
    socket.emit('ad_rewarded',{coins:reward,newTotal:p.coins});
  });

  // ── LEADERBOARDS ─────────────────────────────────────────────
  socket.on('get_leaderboard', ({type='weekly',limit=50}) => {
    const board = type==='weekly' ? weeklyBoard : globalBoard;
    const data  = Array.from(board.values())
      .sort((a,b)=>b.points-a.points)
      .slice(0,limit)
      .map((e,i)=>({...e, rank:i+1, rankTier:getRankTier(profiles.get(e.uid)?.rankPoints||1000)}));
    socket.emit('leaderboard_data',{type,entries:data});
  });

  // ── TOURNAMENTS ───────────────────────────────────────────────
  socket.on('get_tournaments', () => {
    const open = Array.from(tournaments.values())
      .filter(t=>t.state==='registering')
      .map(t=>({id:t.id,name:t.name,format:t.format,prizeCoins:t.prizeCoins,participants:t.participants.length,maxPlayers:t.maxPlayers,createdAt:t.createdAt}));
    socket.emit('tournaments_list',{tournaments:open});
  });

  socket.on('create_tournament', ({format,playerData}) => {
    const fmt = {QUICK:{name:'Quick',maxPlayers:7,prizeCoins:500,prizeGems:5},STANDARD:{name:'Standard',maxPlayers:14,prizeCoins:1500,prizeGems:15},GRAND:{name:'Grand',maxPlayers:21,prizeCoins:5000,prizeGems:50}}[format]||{name:'Standard',maxPlayers:14,prizeCoins:1500,prizeGems:15};
    const t = {id:uuidv4(),format,name:`${fmt.name} Tournament`,maxPlayers:fmt.maxPlayers,prizeCoins:fmt.prizeCoins,prizeGems:fmt.prizeGems,state:'registering',host:playerData.uid,participants:[],leaderboard:[],matches:[],winner:null,createdAt:Date.now()};
    tournaments.set(t.id,t);
    t.participants.push(playerData);
    checkAchievements(playerData.uid);
    io.emit('tournament_available',{id:t.id,name:t.name,format:t.format,prizeCoins:t.prizeCoins});
    socket.emit('tournament_created',{tournament:t});
  });

  socket.on('join_tournament', ({tournamentId,playerData}) => {
    const t = tournaments.get(tournamentId); if(!t) return err('Not found');
    if(t.state!=='registering') return err('Registration closed');
    if(t.participants.length>=t.maxPlayers) return err('Full');
    if(t.participants.find(p=>p.uid===playerData.uid)) return err('Already registered');
    t.participants.push(playerData);
    updateMissionProgress(playerData.uid,'tournament',1);
    checkAchievements(playerData.uid);
    io.emit('tournament_update',{id:tournamentId,participants:t.participants.length});
    socket.emit('tournament_joined',{tournament:t});
    if(t.participants.length>=t.maxPlayers) startTournament(t);
  });

  function startTournament(t) {
    t.state = 'in_progress';
    t.startedAt = Date.now();
    const shuffledP = shuffled([...t.participants]);
    for(let i=0;i<shuffledP.length;i+=7) {
      const matchPlayers = shuffledP.slice(i,i+7);
      const matchId = uuidv4();
      t.matches.push({id:matchId,players:matchPlayers.map(p=>p.uid),status:'pending',results:null});
      // Create room for this match
      const roomCode = genCode();
      const room = createRoom(matchPlayers[0], 7, false, roomCode);
      matchPlayers.slice(1).forEach(p2 => joinRoom(p2,roomCode));
      t.matches[t.matches.length-1].roomId = roomCode;
      // Notify players
      matchPlayers.forEach(mp => {
        pushNotification(mp.uid,'Tournament Match!','Your tournament match is starting!',{type:'tournament_match',roomId:roomCode});
      });
      io.to(roomCode).emit('tournament_match_start',{tournamentId:t.id,matchId,roomId:roomCode});
    }
    io.emit('tournament_started',{id:t.id});
  }

  // ── SPECTATOR ─────────────────────────────────────────────────
  socket.on('spectate_room', ({roomId}) => {
    const room = rooms.get(roomId); if(!room) return err('Room not found');
    if(!spectators.has(roomId)) spectators.set(roomId, new Set());
    spectators.get(roomId).add(socket.id);
    socket.join(roomId+'_spec');
    const state = buildRoomState(room, null);
    socket.emit('spectate_state',{...state,spectating:true});
    socket.emit('chat_history',{messages:chatHistory.get(roomId)||[]});
  });

  socket.on('stop_spectating', ({roomId}) => {
    spectators.get(roomId)?.delete(socket.id);
    socket.leave(roomId+'_spec');
  });

  socket.on('get_spectatable_rooms', () => {
    const list = Array.from(rooms.values())
      .filter(r=>r.state==='playing'&&r.players.length>0)
      .map(r=>({id:r.id,playerCount:r.players.length,roundNo:r.roundNo,isRanked:r.isRanked,spectatorCount:spectators.get(r.id)?.size||0}));
    socket.emit('spectatable_rooms',{rooms:list});
  });

  // ── REPLAYS ───────────────────────────────────────────────────
  socket.on('get_replays', ({uid}) => {
    const userReplays = Array.from(replays.values()).filter(r=>r.playerUids.includes(uid)).slice(0,20);
    socket.emit('replays_list',{replays:userReplays.map(r=>({id:r.id,date:r.date,players:r.playerNames,rounds:r.totalRounds,duration:r.duration}))});
  });

  socket.on('get_replay', ({replayId}) => {
    const r = replays.get(replayId);
    if(!r) return err('Replay not found');
    socket.emit('replay_data',{replay:r});
  });

  // ── RANKED QUEUE ──────────────────────────────────────────────
  socket.on('join_ranked', ({playerData}) => {
    if(BANNED_UIDS.has(playerData.uid)) return err('Account suspended');
    const existing = rankedQueue.findIndex(p=>p.uid===playerData.uid);
    if(existing!==-1) rankedQueue.splice(existing,1);
    rankedQueue.push({socketId:socket.id,...playerData,joinedAt:Date.now()});
    socket.emit('queue_update',{position:rankedQueue.length,needed:7});
    if(rankedQueue.length>=7) {
      const players = rankedQueue.splice(0,7);
      const code = genCode();
      const room = createRoom(players[0],7,true,code);
      players.slice(1).forEach(p=>joinRoom(p,code));
      players.forEach(p => {
        io.sockets.sockets.get(p.socketId)?.join(code);
        io.to(p.socketId).emit('ranked_match_found',{roomId:code,players:players.map(pl=>({name:pl.name,avatar:pl.avatar,uid:pl.uid}))});
      });
      setTimeout(()=>triggerStartGame(code),3000);
    }
  });

  socket.on('leave_ranked', ({uid}) => {
    const i = rankedQueue.findIndex(p=>p.uid===uid);
    if(i!==-1) rankedQueue.splice(i,1);
  });

  // ── ROOM: CREATE / JOIN ───────────────────────────────────────
  socket.on('create_room', ({playerData,maxPlayers}) => {
    if(BANNED_UIDS.has(playerData.uid)) return err('Account suspended');
    getOrCreateProfile(playerData.uid, playerData.name, playerData.avatar);
    const code = genCode();
    const room = createRoom(playerData, maxPlayers||4, false, code);
    socket.join(code);
    socket.emit('room_created',{roomId:code});
    broadcastRoom(room);
  });

  socket.on('join_room', ({roomCode,playerData}) => {
    if(BANNED_UIDS.has(playerData.uid)) return err('Account suspended');
    getOrCreateProfile(playerData.uid, playerData.name, playerData.avatar);
    const code = roomCode.toUpperCase();
    const room = rooms.get(code); if(!room) return err('Room not found');
    if(room.state!=='waiting') return err('Game already started');
    if(room.players.length>=room.maxPlayers) return err('Room full');
    if(room.players.find(p=>p.uid===playerData.uid)) return err('Already in room');
    room.players.push({socketId:socket.id,uid:playerData.uid,name:playerData.name,avatar:playerData.avatar,ready:false,connected:true});
    playerRooms.set(socket.id,code);
    socket.join(code);
    socket.emit('room_joined',{roomId:code});
    broadcastRoom(room);
    io.to(code).emit('player_joined',{name:playerData.name,avatar:playerData.avatar});
    systemChat(code,`${playerData.name} joined the room`);
  });

  function createRoom(playerData, maxPlayers, isRanked, code) {
    const room = {
      id:code, isRanked, maxPlayers,
      state:'waiting', phase:'waiting',
      host:playerData.uid,
      players:[{socketId:playerData.socketId||socket.id,uid:playerData.uid,name:playerData.name,avatar:playerData.avatar,ready:false,connected:true}],
      hands:{}, table:[], leadSuit:null,
      leader:null, currentTurn:null,
      turnOrder:[], finishOrder:[], gone:[],
      roundNo:1, scores:{},
      transferRequests:{},
      replayLog:[], startTime:null,
    };
    rooms.set(code,room);
    playerRooms.set(playerData.socketId||socket.id, code);
    return room;
  }

  function joinRoom(playerData, code) {
    const room = rooms.get(code); if(!room) return;
    room.players.push({socketId:playerData.socketId||'',uid:playerData.uid,name:playerData.name,avatar:playerData.avatar,ready:false,connected:true});
    playerRooms.set(playerData.socketId||'',code);
  }

  socket.on('set_ready', ({ready}) => {
    const room = myRm(); if(!room) return;
    const p    = myPl(room); if(p) p.ready=ready;
    broadcastRoom(room);
  });

  socket.on('start_game', () => {
    const room = myRm(); if(!room) return;
    if(room.host!==myPl(room)?.uid) return err('Only host can start');
    if(room.players.length<2) return err('Need at least 2 players');
    triggerStartGame(room.id);
  });

  function triggerStartGame(roomId) {
    const room = rooms.get(roomId); if(!room) return;
    const n    = room.players.length;
    const handsArr = dealCards(n);
    const uids     = room.players.map(p=>p.uid);
    uids.forEach((uid,i) => room.hands[uid]=handsArr[i]);
    let starter = 0;
    handsArr.forEach((h,i)=>{ if(h.some(c=>c.suit==='♠'&&c.rank==='A')) starter=i; });
    room.leader      = uids[starter];
    room.currentTurn = uids[starter];
    room.turnOrder   = uids;
    room.leadSuit    = '♠';
    room.table       = [];
    room.finishOrder = [];
    room.gone        = [];
    room.roundNo     = 1;
    room.phase       = 'playing';
    room.state       = 'playing';
    room.scores      = Object.fromEntries(uids.map(u=>[u,0]));
    room.startTime   = Date.now();
    room.replayLog   = [{type:'game_start',uids,timestamp:Date.now()}];
    broadcastRoom(room);
    systemChat(roomId,`Game started! ${room.players[starter].name} has ♠A and leads first!`);
    // Push notification for non-leader players
    uids.forEach(uid => {
      if(uid!==room.leader) pushNotification(uid,'Game Started!','The game has started. Get ready!',{type:'game_started',roomId});
    });
  }

  // ── PLAY CARD ─────────────────────────────────────────────────
  socket.on('play_card', ({cardKey}) => {
    const room = myRm(); if(!room) return;
    const p    = myPl(room); if(!p) return;
    if(!antiCheatCheck(socket,p.uid,'play_card',{cardKey},room)) return;
    if(room.currentTurn!==p.uid||room.phase!=='playing') return err('Not your turn');
    const hand = room.hands[p.uid]||[];
    const card = hand.find(c=>cKey(c)===cardKey); if(!card) return err('Card not in hand');
    const isFirst = room.table.length===0;
    const valid   = validCards(hand, room.leadSuit, isFirst);
    if(!valid.includes(cardKey)) return err('Invalid card');

    // Anti-cheat: record action
    room.replayLog.push({type:'play_card',uid:p.uid,card,timestamp:Date.now(),roundNo:room.roundNo});

    room.hands[p.uid] = hand.filter(c=>cKey(c)!==cardKey);
    room.table.push({uid:p.uid,card});
    if(isFirst) room.leadSuit = card.suit;

    broadcastRoom(room);
    // Push turn notification to next player
    const offsuit = !isFirst && card.suit!==room.leadSuit;

    if(offsuit) {
      // Broken round
      const top = highestOf(room.table.map(t=>({playerIdx:t.uid,card:t.card})), room.leadSuit);
      systemChat(room.id,`${p.name} has no ${room.leadSuit}! Round ends.`);
      setTimeout(()=>resolveRound(room,false,top),1200);
    } else {
      const active = room.turnOrder.filter(uid=>!room.finishOrder.includes(uid)&&(room.hands[uid].length>0||uid===p.uid));
      if(active.every(uid=>room.table.some(t=>t.uid===uid))) {
        const top = highestOf(room.table.map(t=>({playerIdx:t.uid,card:t.card})),room.leadSuit);
        setTimeout(()=>resolveRound(room,true,top),1000);
      } else {
        advanceTurn(room);
        broadcastRoom(room);
        // Notify next player
        const nextP = room.players.find(p2=>p2.uid===room.currentTurn);
        if(nextP) pushNotification(nextP.uid,'Your Turn!',`It's your turn in room ${room.id}`,{type:'your_turn',roomId:room.id});
      }
    }
  });

  function resolveRound(room, isClean, topCard) {
    const collectorUid = topCard?.playerIdx || room.leader;
    const takenCards   = room.table.map(t=>t.card);
    let cleanRounds = 0;

    if(isClean) {
      // Cards eliminated
      cleanRounds = 1;
      systemChat(room.id,`✅ Clean round! ${takenCards.length} cards eliminated.`);
    } else {
      // Give to collector
      room.hands[collectorUid] = sortHand([...(room.hands[collectorUid]||[]),...takenCards]);
      systemChat(room.id,`📦 ${room.players.find(p=>p.uid===collectorUid)?.name} collects ${takenCards.length} cards!`);
    }

    // Check finishers
    room.turnOrder.forEach(uid => {
      if(!room.finishOrder.includes(uid)&&(room.hands[uid]||[]).length===0) room.finishOrder.push(uid);
    });

    const result = {type:isClean?'clean':'broken',collectorUid,cards:takenCards,topCard:topCard?.card,leadSuit:room.leadSuit};
    io.to(room.id).emit('round_ended',{result,roundNo:room.roundNo,finishOrder:room.finishOrder,gone:room.gone});
    room.replayLog.push({type:'round_end',result,roundNo:room.roundNo,timestamp:Date.now()});

    room.table     = [];
    room.leadSuit  = null;
    room.roundNo++;
    room.leader    = collectorUid;
    room.currentTurn = collectorUid;
    room.gone      = goneSuits(room.hands);

    // Game over check
    const active = room.turnOrder.filter(uid=>!room.finishOrder.includes(uid)&&(room.hands[uid]||[]).length>0);
    if(active.length<=1) {
      active.forEach(uid=>{if(!room.finishOrder.includes(uid))room.finishOrder.push(uid);});
      finalizeGame(room);
      return;
    }

    room.phase = 'picking_suit';
    broadcastRoom(room);
    pushNotification(collectorUid,'Pick a Suit!','You won the round. Pick the next suit to lead.',{type:'pick_suit',roomId:room.id});
  }

  function advanceTurn(room) {
    const idx  = room.turnOrder.indexOf(room.currentTurn);
    let next   = (idx+1)%room.turnOrder.length;
    let safety = 0;
    while((room.finishOrder.includes(room.turnOrder[next])||!(room.hands[room.turnOrder[next]]||[]).length)&&safety++<room.turnOrder.length)
      next=(next+1)%room.turnOrder.length;
    room.currentTurn = room.turnOrder[next];
  }

  function finalizeGame(room) {
    const n = room.players.length;
    const roundsPlayed = room.roundNo-1;
    const duration = Date.now()-(room.startTime||Date.now());

    // Calculate scores & update profiles
    const gameResults = {};
    room.finishOrder.forEach((uid,pos)=>{
      const pts = scorePts(pos,n);
      room.scores[uid] = pts;
      const result = recordGameResult(uid,pos,n,pts,roundsPlayed,room.isRanked,0,0);
      gameResults[uid] = result;
    });

    room.state = 'finished';
    room.phase = 'finished';

    io.to(room.id).emit('game_over',{
      finishOrder:room.finishOrder,
      scores:room.scores,
      players:room.players.map(p=>({uid:p.uid,name:p.name,avatar:p.avatar})),
      gameResults,
    });

    // Save replay
    const replayId = uuidv4();
    replays.set(replayId,{
      id:replayId,
      date:Date.now(),
      duration,
      playerUids:room.players.map(p=>p.uid),
      playerNames:room.players.map(p=>p.name),
      totalRounds:roundsPlayed,
      finishOrder:room.finishOrder,
      scores:room.scores,
      log:room.replayLog,
    });

    // Notify all players with results
    room.finishOrder.forEach((uid,pos)=>{
      const pts = room.scores[uid];
      const isBest = pos===n-2;
      pushNotification(uid,'Game Over!',isBest?`🏆 Great game! +${pts} coins earned`:`Game finished. ${pts>0?'+':''}${pts} points`,{type:'game_over',roomId:room.id,replayId});
    });

    // Broadcast achievements earned
    room.finishOrder.forEach(uid=>{
      const result = gameResults[uid];
      if(result?.newAchievements?.length) {
        const p = room.players.find(pl=>pl.uid===uid);
        if(p) io.to(p.socketId).emit('achievements_unlocked',{achievements:result.newAchievements});
      }
    });

    systemChat(room.id,`Game over! ${room.players.find(p=>p.uid===room.finishOrder[n-2])?.name||'?'} wins with the highest score!`);
  }

  // ── PICK SUIT ─────────────────────────────────────────────────
  socket.on('pick_suit', ({suit}) => {
    const room = myRm(); if(!room) return;
    const p    = myPl(room); if(!p) return;
    if(room.leader!==p.uid||room.phase!=='picking_suit') return;
    if(room.gone.includes(suit)) return err('Suit eliminated');
    room.leadSuit  = suit;
    room.phase     = 'playing';
    broadcastRoom(room);
    io.to(room.id).emit('suit_picked',{suit,leader:p.uid});
    systemChat(room.id,`${p.name} leads with ${suit}`);
  });

  // ── TRANSFER ─────────────────────────────────────────────────
  socket.on('request_transfer', ({targetUid}) => {
    const room = myRm(); if(!room) return;
    const p    = myPl(room); if(!p) return;
    if(room.table.length>0) return err('Cannot request mid-round');
    if(!antiCheatCheck(socket,p.uid,'request_transfer',{targetUid},room)) return;

    const target = room.players.find(pl=>pl.uid===targetUid); if(!target) return err('Player not found');
    const reqId  = uuidv4();
    room.transferRequests[reqId] = {id:reqId,fromUid:p.uid,fromName:p.name,fromAvatar:p.avatar,toUid:targetUid,toSocketId:target.socketId,timestamp:Date.now()};

    socket.emit('transfer_sent',{reqId,targetUid});
    io.to(target.socketId).emit('transfer_incoming',{reqId,fromUid:p.uid,fromName:p.name,fromAvatar:p.avatar,cardCount:(room.hands[targetUid]||[]).length});

    setTimeout(()=>{
      if(room.transferRequests[reqId]) {
        delete room.transferRequests[reqId];
        socket.emit('transfer_auto_rejected',{reqId,targetUid});
        io.to(target.socketId).emit('transfer_expired',{reqId});
      }
    },10000);
  });

  socket.on('respond_transfer', ({reqId,accept}) => {
    const room = myRm(); if(!room) return;
    const req  = room.transferRequests[reqId]; if(!req) return;
    delete room.transferRequests[reqId];

    const requesterSocket = room.players.find(p=>p.uid===req.fromUid)?.socketId;

    if(!accept) {
      if(requesterSocket) io.to(requesterSocket).emit('transfer_refused',{reqId,toUid:req.toUid});
      return;
    }

    // Transfer cards
    const taken = [...(room.hands[req.toUid]||[])];
    room.hands[req.toUid]  = [];
    room.hands[req.fromUid]= sortHand([...(room.hands[req.fromUid]||[]),...taken]);
    if(!room.finishOrder.includes(req.toUid)) room.finishOrder.push(req.toUid);

    updateMissionProgress(req.toUid,'transfer',1);
    updateMissionProgress(req.fromUid,'transfer',1);

    if(requesterSocket) io.to(requesterSocket).emit('transfer_accepted',{reqId,toUid:req.toUid,cardCount:taken.length});
    broadcastRoom(room);
    systemChat(room.id,`${room.players.find(p=>p.uid===req.toUid)?.name} gave all cards to ${room.players.find(p=>p.uid===req.fromUid)?.name}!`);

    // Check game over
    const active = room.turnOrder.filter(uid=>!room.finishOrder.includes(uid)&&(room.hands[uid]||[]).length>0);
    if(active.length<=1) {
      active.forEach(uid=>{if(!room.finishOrder.includes(uid))room.finishOrder.push(uid);});
      finalizeGame(room);
    }
  });

  // ── RECONNECT ────────────────────────────────────────────────
  socket.on('reconnect_room', ({uid,roomId}) => {
    const room = rooms.get(roomId); if(!room) return err('Room not found');
    const p    = room.players.find(p=>p.uid===uid); if(!p) return err('Not in room');
    playerRooms.delete(p.socketId);
    p.socketId  = socket.id;
    p.connected = true;
    playerRooms.set(socket.id,roomId);
    socket.join(roomId);
    const state = buildRoomState(room,uid);
    socket.emit('room_state',{...state,reconnected:true});
    socket.emit('chat_history',{messages:chatHistory.get(roomId)||[]});
    io.to(roomId).emit('player_reconnected',{uid});
    systemChat(roomId,`${p.name} reconnected!`);
  });

  // ── DISCONNECT ────────────────────────────────────────────────
  socket.on('disconnect', () => {
    const roomId = playerRooms.get(socket.id);
    if(roomId) {
      const room = rooms.get(roomId);
      const p    = room?.players.find(p=>p.socketId===socket.id);
      if(p) {
        p.connected = false;
        io.to(roomId).emit('player_disconnected',{uid:p.uid,name:p.name});
        systemChat(roomId,`${p.name} disconnected`);
      }
      playerRooms.delete(socket.id);
      // Remove from spectators
      for(const [,specs] of spectators) specs.delete(socket.id);
    }
    // Remove from queue
    const qi = rankedQueue.findIndex(p=>p.socketId===socket.id);
    if(qi!==-1) rankedQueue.splice(qi,1);
    console.log(`[-] ${socket.id}`);
  });
});

const PORT = process.env.PORT||3000;
server.listen(PORT,()=>console.log(`\n🃏 Ace Spades v2.0 on port ${PORT}\n   17 features active\n`));
