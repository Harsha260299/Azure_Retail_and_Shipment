const clean = value => String(value ?? '').trim();

// One reporting row per consignment; never join this one-to-many table into basket sums.
export function validateShipments(shipments, orders, acceptedOrders, statuses,
    runId = 'local-demo', timestamp = new Date().toISOString()) {
  const knownOrders = new Set(orders.map(o=>clean(o.order_id)).filter(Boolean));
  const approvedOrders = new Set(acceptedOrders.map(o=>clean(o.order_id)));
  const knownStatuses = new Set(statuses.map(s=>clean(s).toUpperCase()).filter(Boolean));
  if (!knownStatuses.size) throw new Error('Empty shipment status reference');
  const rows = shipments.map(s=>({...s,consignment_id:clean(s.consignment_id),
    order_id:clean(s.order_id),carrier:clean(s.carrier),shipment_status:clean(s.shipment_status).toUpperCase()}));
  const counts = new Map();
  for (const row of rows) counts.set(row.consignment_id,(counts.get(row.consignment_id)??0)+1);
  const accepted=[], rejected=[];
  for (const row of rows) {
    let reason='';
    if (!row.consignment_id || !row.order_id || !row.carrier) reason='MISSING_SHIPMENT_FIELD';
    else if (counts.get(row.consignment_id)>1) reason='DUPLICATE_CONSIGNMENT_ID';
    else if (!knownStatuses.has(row.shipment_status)) reason='INVALID_SHIPMENT_STATUS';
    else if (!knownOrders.has(row.order_id)) reason='UNKNOWN_ORDER';
    else if (!approvedOrders.has(row.order_id)) reason='ORDER_NOT_APPROVED';
    const output={...row,pipeline_run_id:runId,source_file:'data/shipments.json',processed_at:timestamp};
    if (reason) rejected.push({...output,rejection_reason:reason});
    else accepted.push(output);
  }
  const reasons={};
  for (const row of rejected) reasons[row.rejection_reason]=(reasons[row.rejection_reason]??0)+1;
  return {accepted,rejected,audit:{input_shipments:rows.length,accepted_shipments:accepted.length,
    rejected_shipments:rejected.length,reasons}};
}
