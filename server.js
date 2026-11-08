// Small WebSocket multiplayer server. Run with: npm install && npm start
'use strict';
const http=require('node:http');
const fs=require('node:fs');
const path=require('node:path');
const crypto=require('node:crypto');
const {WebSocketServer,WebSocket}=require('ws');
const PORT=Number(process.env.PORT)||3000;
const root=__dirname;
const server=http.createServer((req,res)=>{
 const pathname=(req.url||'/').split('?')[0];
 if(pathname!=='/'&&pathname!=='/index.html'){res.writeHead(404);return res.end('Not found')}
 res.writeHead(200,{'Content-Type':'text/html; charset=utf-8','Cache-Control':'no-store','X-Content-Type-Options':'nosniff'});
 fs.createReadStream(path.join(root,'index.html')).pipe(res);
});
const wss=new WebSocketServer({server,path:'/ws',maxPayload:2048});
const rooms=new Map();
const send=(ws,msg)=>{if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(msg))};
const broadcast=(room,msg,exclude)=>{for(const p of rooms.get(room)?.values()||[])if(p.ws!==exclude)send(p.ws,msg)};
const cleanName=(s)=>String(s||'Detective').replace(/[<>\x00-\x1f]/g,'').trim().slice(0,18)||'Detective';
const cleanRoom=(s)=>String(s||'MAPLEWOOD').toUpperCase().replace(/[^A-Z0-9_-]/g,'').slice(0,12)||'MAPLEWOOD';
const publicPlayer=(p)=>({id:p.id,name:p.name,x:p.x,y:p.y,dir:p.dir,moving:p.moving,style:p.style});
function leave(p){if(!p.room)return;const group=rooms.get(p.room);if(group){group.delete(p.id);broadcast(p.room,{type:'leave',id:p.id});if(group.size===0)rooms.delete(p.room)}p.room=null}
wss.on('connection',ws=>{
 const p={ws,id:crypto.randomUUID(),room:null,name:'Detective',x:398,y:270,dir:'down',moving:false,style:0,lastMove:0,lastChat:0};
 ws.on('message',raw=>{
  let m;try{m=JSON.parse(String(raw))}catch{return}if(!m||typeof m!=='object')return;
  if(m.type==='join'){
   leave(p);const room=cleanRoom(m.room),group=rooms.get(room)||new Map();
   if(group.size>=24)return send(ws,{type:'error',message:'This room is full (24 players).'});
   p.name=cleanName(m.name);p.x=398;p.y=270;p.room=room;
   send(ws,{type:'welcome',id:p.id,room,players:[...group.values()].map(publicPlayer)});
   if(!rooms.has(room))rooms.set(room,group);group.set(p.id,p);broadcast(room,{type:'join',player:publicPlayer(p)},ws);
  } else if(m.type==='move'&&p.room){
   const now=Date.now();if(now-p.lastMove<45)return;p.lastMove=now;
   const x=Number(m.x),y=Number(m.y);if(!Number.isFinite(x)||!Number.isFinite(y))return;
   const nx=Math.max(22,Math.min(1158,x)),ny=Math.max(40,Math.min(806,y));
   // Prevent sudden teleports; generous for lag spikes.
   const maxJump=70+Math.min(200,(now-(p.prevAcceptedMove||now))*.2);
   if(Math.hypot(nx-p.x,ny-p.y)>maxJump)return;
   p.x=nx;p.y=ny;p.dir=['up','down','left','right'].includes(m.dir)?m.dir:'down';p.moving=!!m.moving;p.style=0;p.prevAcceptedMove=now;
   broadcast(p.room,{type:'move',...publicPlayer(p)},ws);
  } else if(m.type==='chat'&&p.room){
   const now=Date.now();if(now-p.lastChat<600)return;p.lastChat=now;
   const txt=String(m.text||'').replace(/[\x00-\x1f]/g,' ').trim().slice(0,180);
   if(txt)broadcast(p.room,{type:'chat',name:p.name,id:p.id,text:txt});
  }
 });
 ws.on('close',()=>leave(p));ws.on('error',()=>{});
});
server.listen(PORT,'0.0.0.0',()=>console.log(`Maplewood multiplayer at http://localhost:${PORT}`));
