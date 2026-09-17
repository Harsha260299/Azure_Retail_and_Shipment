import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {validateShipments} from '../src/shipments.mjs';
const orders=[{order_id:'1'},{order_id:'2'}], accepted=[orders[0]], statuses=['CREATED','DELIVERED'];
const shipment=(id,order='1',status='CREATED')=>({consignment_id:id,order_id:order,carrier:'Demo Carrier',shipment_status:status});
const run=rows=>validateShipments(rows,orders,accepted,statuses,'test','fixed');
test('split shipments remain distinct consignments',()=>{
 const result=run([shipment('A'),shipment('B')]);
 assert.equal(result.accepted.length,2);
 assert.equal(new Set(result.accepted.map(s=>s.order_id)).size,1);
});
test('every duplicate consignment is quarantined',()=>{
 const result=run([shipment('A'),shipment('A','1','DELIVERED')]);
 assert.equal(result.accepted.length,0);
 assert.ok(result.rejected.every(s=>s.rejection_reason==='DUPLICATE_CONSIGNMENT_ID'));
});
test('unknown and rejected orders have different shipment reasons',()=>{
 assert.deepEqual(run([shipment('A','9'),shipment('B','2')]).rejected.map(s=>s.rejection_reason),
 ['UNKNOWN_ORDER','ORDER_NOT_APPROVED']);
});
test('missing fields, blank status and status normalisation',()=>{
 assert.equal(run([shipment(null)]).rejected[0].rejection_reason,'MISSING_SHIPMENT_FIELD');
 assert.equal(run([shipment('A','1',null)]).rejected[0].rejection_reason,'INVALID_SHIPMENT_STATUS');
 assert.equal(run([shipment('A','1',' delivered ')]).accepted[0].shipment_status,'DELIVERED');
 assert.throws(()=>validateShipments([],orders,accepted,[]),/Empty/);
});
test('sample shipment reconciliation and deterministic reruns',async()=>{
 const read=name=>readFile(new URL('../data/'+name,import.meta.url),'utf8');
 const rows=(await read('shipments.json')).trim().split(/\r?\n/).map(s=>JSON.parse(s));
 const ref=JSON.parse(await read('valid_shipment_status.json'));
 const source=['1001','1002','1003','1004','1005','1006','1007','1009'].map(order_id=>({order_id}));
 const good=['1001','1005','1006','1009'].map(order_id=>({order_id}));
 const result=validateShipments(rows,source,good,ref,'test','fixed');
 assert.deepEqual(result,validateShipments(rows,source,good,ref,'test','fixed'));
 assert.deepEqual([result.audit.input_shipments,result.accepted.length,result.rejected.length],[9,4,5]);
 assert.equal(result.audit.input_shipments,result.accepted.length+result.rejected.length);
 assert.deepEqual(result.audit.reasons,{ORDER_NOT_APPROVED:1,UNKNOWN_ORDER:1,DUPLICATE_CONSIGNMENT_ID:2,INVALID_SHIPMENT_STATUS:1});
});
