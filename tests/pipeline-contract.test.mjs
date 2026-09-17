import test from 'node:test';
import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
const read = name => readFile(new URL('../'+name, import.meta.url),'utf8');
const json = async name => JSON.parse(await read(name));
test('ADF converts CSV into Parquet with a complete string column mapping',async()=>{
 const p=await json('adf/pipeline.json');
 const activity=p.properties.activities.find(a=>a.name==='CopyOrders');
 assert.equal(activity.typeProperties.source.type,'DelimitedTextSource');
 assert.equal(activity.typeProperties.sink.type,'ParquetSink');
 const input=activity.inputs[0], output=activity.outputs[0];
 assert.equal(input.parameters.file,'orders.csv');
 assert.equal(output.parameters.file,'orders.parquet');
 const source=await json('adf/datasets/'+input.referenceName+'.json');
 const sink=await json('adf/datasets/'+output.referenceName+'.json');
 assert.equal(source.properties.type,'DelimitedText');
 assert.equal(source.properties.typeProperties.firstRowAsHeader,true);
 assert.equal(sink.properties.type,'Parquet');
 assert.equal(sink.properties.typeProperties.compressionCodec,'snappy');
 assert.deepEqual(activity.typeProperties.translator.mappings.map(m=>m.source.name),source.properties.schema.map(c=>c.name));
 assert.ok(activity.typeProperties.translator.mappings.every(m=>m.source.name===m.sink.name && m.source.type==='String' && m.sink.type==='String'));
});
test('notebook consumes the ADF landing filename and quarantines in Parquet',async()=>{
 const notebook=await read('notebooks/asda_retail_validation.py');
 const p=await json('adf/pipeline.json');
 const file=p.properties.activities.find(a=>a.name==='CopyOrders').outputs[0].parameters.file;
 assert.ok(notebook.includes('.parquet(landing + "/'+file+'")'));
 assert.ok(!notebook.includes('.csv('));
 assert.ok(notebook.includes('.parquet(output + "/rejected_orders")'));
 assert.ok(notebook.includes('F.lit(landing + "/'+file+'")'));
});
test('item JSON is unchanged and validation waits for all three ingestion activities',async()=>{
 const p=await json('adf/pipeline.json');
 const items=p.properties.activities.find(a=>a.name==='CopyOrderItems');
 assert.equal(items.typeProperties.source.type,'BinarySource');
 assert.equal(items.typeProperties.sink.type,'BinarySink');
 assert.equal(items.inputs[0].parameters.file,'order_items.json');
 assert.equal(items.outputs[0].parameters.file,'order_items.json');
 const shipment=p.properties.activities.find(a=>a.name==='CopyShipments');
 assert.equal(shipment.inputs[0].parameters.file,'shipments.json');
 assert.equal(shipment.outputs[0].parameters.file,'shipments.json');
 assert.equal(shipment.typeProperties.source.type,'BinarySource');
 const validation=p.properties.activities.find(a=>a.name==='ValidateAndReport');
 assert.deepEqual(validation.dependsOn.map(d=>d.activity).sort(),['CopyOrderItems','CopyOrders','CopyShipments']);
 assert.ok(validation.dependsOn.every(d=>d.dependencyConditions.includes('Succeeded')));
});

test('shipment fields and reference values match SQL and notebook contracts',async()=>{
 const sql=await read('sql/002_shipments.sql'), notebook=await read('notebooks/asda_retail_validation.py');
 const fields=['consignment_id','order_id','carrier','service_level','tracking_reference','shipment_status','dispatched_at','expected_delivery_date','delivered_at'];
 for (const field of fields) {assert.ok(sql.includes(field+' varchar'));assert.ok(notebook.includes('"'+field+'"'));}
 const statuses=await json('data/valid_shipment_status.json');
 for(const status of statuses) assert.ok(sql.includes("('"+status+"')"));
 assert.ok(notebook.includes('"dbo.shipment_reporting"'));
 assert.ok(notebook.includes('.parquet(output + "/rejected_shipments")'));
});
