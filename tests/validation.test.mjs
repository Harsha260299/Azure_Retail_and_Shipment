import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import {validate} from '../src/validation.mjs';
const order = (id,status='COMPLETE') => ({order_id:id,order_date:'2026-09-01',customer_id:'C001',order_status:status,store_id:'DEMO'});
const item = (id,orderId,qty=1,price=155) => ({item_id:id,order_id:orderId,quantity:qty,unit_price_pence:price});
const customers=[{customer_id:'C001'}], statuses=['COMPLETE'];
const run=(o,i=[])=>validate(o,i,customers,statuses,'test','2026-09-01T00:00:00Z');
test('all duplicate occurrences rejected, including a conflicting status',()=>{
 const r=run([order('1'),order('1','UNKNOWN')]);
 assert.equal(r.accepted.length,0); assert.deepEqual(r.rejected.map(x=>x.rejection_reason),['DUPLICATE_ORDER_ID','DUPLICATE_ORDER_ID']);
});
test('null status is rejected instead of disappearing in SQL null logic',()=>{
 assert.equal(run([order('1',null)]).rejected[0].rejection_reason,'INVALID_ORDER_STATUS');
});
test('multiple items create one order and integer-pence total',()=>{
 const r=run([order('1',' complete ')],[item('a','1',2),item('b','1',1,120)]);
 assert.equal(r.accepted.length,1); assert.equal(r.accepted[0].total_pence,430);
});
test('missing required field wins and unknown customers are quarantined',()=>{
 const r=run([{...order(null)}, {...order('2'),customer_id:'missing'}]);
 assert.deepEqual(r.rejected.map(x=>x.rejection_reason),['MISSING_REQUIRED_FIELD','UNKNOWN_CUSTOMER']);
});
test('invalid and orphan items fail closed',()=>{
 for (const items of [[item('a','9')],[item('a','1',-1)],[item('a','1',1,1.5)],[item('a','1'),item('a','1')]])
   assert.throws(()=>run([order('1')],items),/Invalid/);
 assert.throws(()=>run([order('1')]),/Missing items/);
});
test('reference duplicates and empty statuses fail closed',()=>{
 assert.throws(()=>validate([],[],[...customers,...customers],statuses),/reference/);
 assert.throws(()=>validate([],[],customers,[]),/reference/);
});
test('sample reconciliation and deterministic reruns',async()=>{
 const read = name=>readFile(new URL('../data/'+name,import.meta.url),'utf8');
 const lines=(await read('orders.csv')).trimEnd().split(/\r?\n/), headers=lines.shift().split(',');
 const orders=lines.map(l=>Object.fromEntries(l.split(',').map((v,i)=>[headers[i],v])));
 const items=(await read('order_items.json')).trim().split(/\r?\n/).map(l=>JSON.parse(l));
 const cs=JSON.parse(await read('customers.json')), ss=JSON.parse(await read('valid_order_status.json'));
 const first=validate(orders,items,cs,ss,'test','fixed'), second=validate(orders,items,cs,ss,'test','fixed');
 assert.deepEqual(first,second);
 assert.equal(first.audit.input_orders,10); assert.equal(first.audit.accepted_orders,4); assert.equal(first.audit.rejected_orders,6);
 assert.equal(first.audit.total_pence,1750);
 assert.deepEqual(first.accepted.map(r=>r.order_id),['1001','1005','1006','1009']);
 assert.equal(first.accepted.length+first.rejected.length,orders.length);
});
