function makeFlip(el, digits=4){
    el.innerHTML = "";
    for(let i=0;i<digits;i++){
        const d=document.createElement('div'); d.className='digit';
        d.innerHTML = `
      <div class="d-top">0</div>
      <div class="d-bottom">0</div>
      <div class="d-next-top">0</div>
      <div class="d-next-bottom">0</div>`;
        el.appendChild(d);
    }
}
function setNumber(el, num, pad){
    const digs = el.querySelectorAll('.digit');
    const n = pad ?? digs.length;                // если pad не передали — берем digs.length
    const s = String(num).padStart(n,'0').slice(-n);
    for (let i = 0; i < n && i < digs.length; i++){
        const dig = digs[i];
        const curTop = dig.querySelector('.d-top');
        const curBottom = dig.querySelector('.d-bottom');
        const nextTop = dig.querySelector('.d-next-top');
        const nextBottom = dig.querySelector('.d-next-bottom');

        const cur = curTop.textContent;
        const nxt = s[i];
        if (cur === nxt) continue;

        nextTop.textContent = nxt;
        nextBottom.textContent = nxt;
        dig.classList.add('anim');
        setTimeout(()=>{
            curTop.textContent = nxt;
            curBottom.textContent = nxt;
            dig.classList.remove('anim');
        }, 350);
    }
}

const canvas = document.getElementById('field');
const ctx = canvas.getContext('2d');
function dpi(canvas){
    const dpr = window.devicePixelRatio || 1;
    const {width, height} = canvas.getBoundingClientRect();
    canvas.width = width * dpr; canvas.height = height * dpr; ctx.scale(dpr,dpr);
}
dpi(canvas); addEventListener('resize', ()=>dpi(canvas));

let arrows=[];
function spawnArrows(intensity){
    const n = Math.min(60, Math.max(2, Math.floor(intensity/120))); // 1..60
    for(let i=0;i<n;i++){
        arrows.push({
            x:-10 - Math.random()*40,
            y:10 + Math.random()*(canvas.clientHeight-20),
            v: 2 + Math.random()*3,
            s: 4 + Math.random()*3,
            hue: Math.random()<.85? 130: 190 
        });
    }
    if(arrows.length>1200) arrows.splice(0, arrows.length-1200);
}
function tickArrows(){
    ctx.clearRect(0,0,canvas.clientWidth,canvas.clientHeight);
    // faint grid glow
    ctx.globalAlpha=.12; ctx.fillStyle="#0e1518"; ctx.fillRect(0,0,canvas.clientWidth,canvas.clientHeight); ctx.globalAlpha=1;
    arrows.forEach(a=>{
        a.x += a.v + (Math.random()-.5)*.5;
        // pixelated arrow (triangle + tail)
        const y=a.y, x=a.x, s=a.s;
        ctx.fillStyle = `hsl(${a.hue}, 95%, 70%)`;
        ctx.beginPath();
        ctx.moveTo(x, y); ctx.lineTo(x+s*2, y-s/1.6); ctx.lineTo(x+s*2, y+s/1.6); ctx.closePath(); ctx.fill();
        ctx.fillRect(x-s*1.2, y-1, s*1.2, 2);
        // trailing glow
        ctx.globalAlpha=.25; ctx.fillRect(x-s*2.2, y-1, s*1.8, 2); ctx.globalAlpha=1;
    });
    arrows = arrows.filter(a=>a.x < canvas.clientWidth+30);
    requestAnimationFrame(tickArrows);
}
requestAnimationFrame(tickArrows);

/* ========== controls & data ========== */
const flipRps = document.getElementById('flipRps');
const flipP95 = document.getElementById('flipP95');
const flipBatch = document.getElementById('flipBatch');
makeFlip(flipRps,4); makeFlip(flipP95,3); makeFlip(flipBatch,3);
setNumber(flipRps,0); setNumber(flipP95,0); setNumber(flipBatch,0);

const connDot = document.getElementById('connDot');
const connText = document.getElementById('connText');
function setConn(ok,msg){
    connDot.className = 'dot '+(ok?'ok':'err');
    connText.textContent = msg || (ok?'подключено':'без соединения');
}

const rpsRange = document.getElementById('rps');
const rpsVal = document.getElementById('rpsVal');
rpsRange.addEventListener('input',()=> rpsVal.textContent = rpsRange.value);

document.getElementById('btnStart').onclick = async ()=>{
    try{
        await fetch('/api/v1/start',{method:'POST',headers:{'Content-Type':'application/json'}, body:JSON.stringify({rps:Number(rpsRange.value)})});
    }catch{}
};
document.getElementById('btnStop').onclick = async ()=>{ try{ await fetch('/api/v1/stop',{method:'POST'}); }catch{} };

let useMock=true, lastOk=0;
async function poll(){
    try{
        const res = await fetch('/api/v1/stats', {cache:'no-store'});
        if(!res.ok) throw new Error();
        const s = await res.json();
        const rps = Math.round(s.rps||0);
        const p95 = Math.round((s.p95_ms||0));
        const batch = Math.round(s.batch_p50||0);
        setNumber(flipRps, Math.min(9999, rps));
        setNumber(flipP95, Math.min(999, p95));
        setNumber(flipBatch, Math.min(999, batch));
        spawnArrows(rps);
        setConn(true, 'подключено к /api/v1/stats');
        useMock=false; lastOk=Date.now();
    }catch{
        if(Date.now()-lastOk>6000){ setConn(false,'мок-данные'); useMock=true; }
    }finally{
        setTimeout(poll, 1200);
    }
}
poll();
setInterval(()=>{
    if(!useMock) return;
    const rps = Number(rpsRange.value);
    const drift = (Math.sin(Date.now()/1200)+1)/2; // 0..1
    const simRps = Math.round(rps * (0.85 + 0.1*drift));
    const simP95 = Math.round(60 + 70*(1-drift)); // 60..130 ms
    const simBatch = Math.round(Math.min(999, (simRps/8) * (0.8+0.4*drift)));
    setNumber(flipRps, simRps);
    setNumber(flipP95, simP95);
    setNumber(flipBatch, simBatch);
    spawnArrows(simRps);
}, 1000);