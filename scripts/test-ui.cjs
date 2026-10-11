// Dependency-free behavior checks for the management page's chart and polling.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
class Element {
  constructor(tag='div') { this.tag=tag; this.children=[]; this.value=''; this.checked=true; this.attributes={}; }
  append(...nodes) { this.children.push(...nodes); }
  replaceChildren(...nodes) { this.children=nodes; if(this.tag==='select')this.value=nodes[0]?.value??''; }
  setAttribute(k,v) { this.attributes[k]=v; }
}
const elements=new Map();
const document={hidden:false,addEventListener(){},getElementById(id){if(!elements.has(id))elements.set(id,new Element(['chartParent','strategy'].includes(id)?'select':'div'));return elements.get(id)},createElement:tag=>new Element(tag),createElementNS:(_,tag)=>new Element(tag)};
let fetches=0, timer, pending=[];
const context=vm.createContext({document,console,URLSearchParams,TextDecoder,AbortController,crypto:{randomUUID:()=> 'test'},setTimeout:fn=>{timer=fn;return 1},clearTimeout:()=>{timer=undefined},fetch:()=>{fetches++;return new Promise(resolve=>pending.push(resolve))}});
const html=fs.readFileSync('engine/src/main/resources/management.html','utf8');
vm.runInContext(html.match(/<script>([\s\S]*?)<\/script>/)[1],context);
const run=code=>vm.runInContext(code,context);
const spec={startNanos:0,endNanos:100e9,quantity:1000,strategy:'twap',parameters:{aheadSeconds:'10',behindSeconds:'20'}};
context.spec=spec;
assert.equal(run('planFraction(spec,-1)'),0);
assert.equal(run('planFraction(spec,50e9)'),.5);
assert.equal(run('planFraction(spec,101e9)'),1);
assert.deepEqual(Array.from(run('planBand(spec,50e9)')),[.3,.6]);
spec.strategy='is';spec.parameters.urgency='2';
assert.ok(run('planFraction(spec,50e9)')>.5);
assert.equal(run('planBand(spec,100e9)[0]'),1);
spec.parameters.urgency='0';assert.equal(run('planFraction(spec,50e9)'),.5);
spec.strategy='passive';assert.equal(run('planBand(spec,100e9)[0]'),.8);
const parent={id:1,strategy:'passive',state:'ACTIVE',strategyState:'PASSIVE_PACING',specification:spec,venueFilled:200,washFilled:50,reserved:100,version:1,executionCosts:{}};
context.parent=parent;
run('recordSample(parent,40e9);recordSample(parent,40e9)');assert.equal(run('histories.get(1).length'),1);
parent.venueFilled=300;run('recordSample(parent,50e9)');
run('state={logicalNanos:50e9,parents:[parent]};');document.getElementById('chartParent').value='1';run('drawChart()');
const chart=document.getElementById('trajectoryChart');
assert.equal(chart.children.filter(n=>n.tag==='polygon').length,1);
assert.equal(chart.children.filter(n=>n.tag==='path').length,3);
assert.ok(chart.children.find(n=>n.tag==='path').attributes.d.includes(' H '));
assert.ok(document.getElementById('chartSummary').textContent.includes('Filled 350'));
run('for(let i=0;i<5000;i++)recordSample(parent,51e9+i)');assert.ok(run('histories.get(1).length')<=2048);assert.equal(run('histories.get(1)[0].time'),40e9);
(async()=>{
  const first=run('refresh()'),second=run('refresh()');assert.equal(fetches,2,'overlapping refreshes share state and health requests');
  pending.shift()({ok:true,json:async()=>({logicalNanos:60e9,parents:[parent]})});pending.shift()({ok:true,json:async()=>({ready:true})});await Promise.all([first,second]);
  assert.equal(document.getElementById('chartParent').value,'1');assert.ok(document.getElementById('refreshStatus').textContent.includes('60.00s'));
  run('connected=true;scheduleRefresh()');assert.equal(typeof timer,'function');
  document.getElementById('autoRefresh').checked=false;run('scheduleRefresh()');assert.equal(timer,undefined);
  document.getElementById('autoRefresh').checked=true;run('scheduleRefresh()');const tick=timer;const polling=tick();assert.equal(fetches,4);
  pending.shift()({ok:false,text:async()=> 'temporary failure'});pending.shift()({ok:true,json:async()=>({})});await polling;
  assert.ok(document.getElementById('refreshStatus').textContent.includes('temporary failure'));assert.equal(typeof timer,'function','polling retries after transient failures');
  console.log('UI chart and auto-refresh checks passed');
})().catch(error=>{console.error(error);process.exitCode=1});
