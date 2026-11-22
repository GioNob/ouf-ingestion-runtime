import {readFileSync,writeFileSync} from 'node:fs';
const source=readFileSync('scripts/ci/predecessor-fixture.mjs','utf8').replace('function publication(', 'export function publication(');
const {publication}=await import('data:text/javascript;base64,'+Buffer.from(source).toString('base64'));
const inputs=[[1,'Library'],[2,'Station','45.6501234567890123450'],[3,'Museum']];
writeFileSync('.task43-check/fixture.json',JSON.stringify(inputs.map(a=>{const f=publication(...a);const [header,row]=f.csv.toString().trimEnd().split('\n').map(s=>s.split(';'));return {envelope:f.envelope,row:Object.fromEntries(header.map((k,i)=>[k,row[i]]))};})));