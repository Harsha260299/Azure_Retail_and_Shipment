import {readFile,mkdir,writeFile} from 'node:fs/promises';
import {validate} from '../src/validation.mjs';
import {validateShipments} from '../src/shipments.mjs';
const root = new URL('../',import.meta.url);
const read = name => readFile(new URL('data/'+name,root),'utf8');
// The controlled sample uses simple unquoted CSV. Use a full CSV parser for arbitrary files.
const csv = (await read('orders.csv')).trimEnd().split(/\r?\n/);
const headers = csv.shift().split(',');
const orders = csv.map(line => Object.fromEntries(line.split(',').map((v,i)=>[headers[i],v])));
const items = (await read('order_items.json')).trim().split(/\r?\n/).map(line=>JSON.parse(line));
const result = validate(orders,items,JSON.parse(await read('customers.json')),JSON.parse(await read('valid_order_status.json')));
const shipments = (await read('shipments.json')).trim().split(/\r?\n/).map(line=>JSON.parse(line));
const shipmentResult = validateShipments(shipments,orders,result.accepted,JSON.parse(await read('valid_shipment_status.json')));
result.audit.shipments = shipmentResult.audit;
const output = new URL('output/',root);
await mkdir(output,{recursive:true});
for (const [name,value] of Object.entries({'sales_reporting.json':result.accepted,'rejected_orders.json':result.rejected,'audit.json':result.audit,'shipment_reporting.json':shipmentResult.accepted,'rejected_shipments.json':shipmentResult.rejected}))
  await writeFile(new URL(name,output),JSON.stringify(value,null,2)+'\n');
console.log(JSON.stringify(result.audit,null,2));
