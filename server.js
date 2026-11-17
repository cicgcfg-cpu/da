// Chaos Descent: room codes + relay. Host browser runs the sim. Rooms live in memory: run ONE machine.
const express=require('express'),http=require('http'),path=require('path'),{randomInt}=require('crypto'),{Server}=require('socket.io');
const app=express();app.use(express.static(path.join(__dirname,'public')));app.get('/healthz',(_,r)=>r.send('ok'));
const srv=http.createServer(app);
const io=new Server(srv,{transports:['websocket'],maxHttpBufferSize:2e5,pingInterval:10000,pingTimeout:10000});
const CLASSES=['Tank','Wizard','Trickster','Engineer'],MAX=8,MAXROOMS=300,ABC='ABCDEFGHJKMNPQRSTUVWXYZ'; // no I, L, O
const rooms=new Map();
const newCode=()=>{for(;;){let c='';for(let i=0;i<4;i++)c+=ABC[randomInt(ABC.length)];if(!rooms.has(c))return c}};
const pub=r=>({code:r.code,hostId:r.hostId,started:r.started,players:[...r.members.values()].map(m=>({id:m.id,name:m.name,cls:m.cls,ready:m.ready}))});
const clean=n=>String(n||'').replace(/[^\w \-]/g,'').trim().slice(0,12)||'Player';
const ack=f=>typeof f==='function'?f:()=>{};
io.on('connection',s=>{
  let room=null,n=0;const tick=setInterval(()=>n=0,1000);
  s.use((_,next)=>{if(++n<=200)next()}); // drop spam
  const isHost=()=>room&&room.hostId===s.id;
  const lobby=r=>io.to(r.code).emit('lobby',pub(r));
  const add=(r,name)=>{r.members.set(s.id,{id:s.id,name:clean(name),cls:'Tank',ready:false});s.join(r.code);room=r;lobby(r)};
  s.on('create',(d,a)=>{a=ack(a);d=d||{};if(room)return a({err:'Already in a room.'});if(rooms.size>=MAXROOMS)return a({err:'Server busy. Try later.'});
    const r={code:newCode(),hostId:s.id,members:new Map(),started:false,slots:[]};rooms.set(r.code,r);add(r,d.name);a({code:r.code,id:s.id})});
  s.on('join',(d,a)=>{a=ack(a);d=d||{};if(room)return a({err:'Already in a room.'});
    const r=rooms.get(String(d.code||'').toUpperCase().slice(0,4));
    if(!r)return a({err:'Room not found.'});if(r.started)return a({err:'Run already started.'});if(r.members.size>=MAX)return a({err:'Room is full (8 max).'});
    add(r,d.name);a({code:r.code,id:s.id})});
  s.on('setClass',c=>{if(!room||room.started||!CLASSES.includes(c))return;room.members.get(s.id).cls=c;lobby(room)});
  s.on('ready',v=>{if(!room||room.started)return;room.members.get(s.id).ready=!!v;lobby(room)});
  s.on('start',(_,a)=>{a=ack(a);if(!isHost()||room.started)return;
    if([...room.members.values()].some(m=>m.id!==s.id&&!m.ready))return a({err:'Everyone must press ready.'});
    room.started=true;room.slots=[...room.members.keys()];
    const players=room.slots.map(id=>{const m=room.members.get(id);return{name:m.name,cls:m.cls}});
    room.slots.forEach((id,i)=>io.to(id).emit('started',{players,you:i}));lobby(room);a({})});
  s.on('input',k=>{if(!room||!room.started||isHost()||!Number.isInteger(k))return;io.to(room.hostId).emit('input',{slot:room.slots.indexOf(s.id),k:k&31})});
  s.on('snap',d=>{if(isHost()&&room.started&&d&&typeof d==='object')s.to(room.code).volatile.emit('snap',d)});
  s.on('h2p',m=>{if(!isHost()||!m||typeof m.t!=='string')return;const p={t:m.t.slice(0,10),d:m.d};
    if(p.t==='ov'&&(typeof p.d!=='string'||p.d.length>8000))return;
    if(m.to==='all')s.to(room.code).emit('h2p',p);else{const id=room.slots[m.to];if(id)io.to(id).emit('h2p',p)}});
  s.on('p2h',m=>{if(!room||!room.started||isHost()||!m||typeof m.t!=='string')return;
    io.to(room.hostId).emit('p2h',{slot:room.slots.indexOf(s.id),t:m.t.slice(0,10),d:Number.isInteger(m.d)?m.d:0})});
  s.on('toLobby',()=>{if(!isHost()||!room.started)return;room.started=false;room.slots=[];room.members.forEach(m=>m.ready=false);io.to(room.code).emit('toLobby');lobby(room)});
  s.on('disconnect',()=>{clearInterval(tick);if(!room)return;const r=room;
    if(r.hostId===s.id){io.to(r.code).emit('closed','Host left. Room closed.');io.in(r.code).socketsLeave(r.code);rooms.delete(r.code)}
    else{r.members.delete(s.id);if(r.started)io.to(r.hostId).emit('left',{slot:r.slots.indexOf(s.id)});lobby(r)}});
});
setInterval(()=>{for(const c of rooms.keys())if(!io.sockets.adapter.rooms.get(c))rooms.delete(c)},60000);
const port=process.env.PORT||8080;srv.listen(port,'0.0.0.0',()=>console.log('listening',port));
process.on('SIGTERM',()=>{io.close();srv.close(()=>process.exit(0))});
