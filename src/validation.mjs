const clean = value => String(value ?? '').trim();

export function validate(orders, items, customers, statuses, runId = 'local-demo', timestamp = new Date().toISOString()) {
  const customerIds = new Set(customers.map(c => clean(c.customer_id)));
  if (customerIds.size !== customers.length || customerIds.has('')) throw new Error('Invalid customer reference');
  const approvedStatuses = new Set(statuses.map(s => clean(s).toUpperCase()).filter(Boolean));
  if (!approvedStatuses.size) throw new Error('Empty status reference');
  const rows = orders.map(r => ({...r, order_id:clean(r.order_id), customer_id:clean(r.customer_id),
    order_date:clean(r.order_date), order_status:clean(r.order_status).toUpperCase()}));
  const counts = new Map();
  for (const r of rows) counts.set(r.order_id, (counts.get(r.order_id) ?? 0) + 1);
  const orderIds = new Set(rows.map(r => r.order_id).filter(Boolean));
  const totals = new Map(), itemIds = new Set();
  for (const item of items) {
    const id = clean(item.item_id), orderId = clean(item.order_id);
    if (!id || itemIds.has(id) || !orderIds.has(orderId) ||
        !Number.isSafeInteger(item.quantity) || item.quantity <= 0 ||
        !Number.isSafeInteger(item.unit_price_pence) || item.unit_price_pence < 0)
      throw new Error('Invalid, duplicate or orphan item: ' + id);
    itemIds.add(id);
    const total = (totals.get(orderId) ?? 0) + item.quantity * item.unit_price_pence;
    if (!Number.isSafeInteger(total)) throw new Error('Money overflow');
    totals.set(orderId, total);
  }
  const accepted = [], rejected = [];
  for (const row of rows) {
    let reason = '';
    if (!row.order_id || !row.order_date || !row.customer_id) reason = 'MISSING_REQUIRED_FIELD';
    else if (counts.get(row.order_id) > 1) reason = 'DUPLICATE_ORDER_ID';
    else if (!approvedStatuses.has(row.order_status)) reason = 'INVALID_ORDER_STATUS';
    else if (!customerIds.has(row.customer_id)) reason = 'UNKNOWN_CUSTOMER';
    const audit = {pipeline_run_id:runId, source_file:'data/orders.csv', processed_at:timestamp};
    if (reason) rejected.push({...row,...audit,rejection_reason:reason});
    else {
      if (!totals.has(row.order_id)) throw new Error('Missing items for accepted order: ' + row.order_id);
      accepted.push({...row,total_pence:totals.get(row.order_id),currency:'GBP',...audit});
    }
  }
  const reasons = {};
  for (const r of rejected) reasons[r.rejection_reason] = (reasons[r.rejection_reason] ?? 0) + 1;
  return {accepted,rejected,audit:{pipeline_run_id:runId,input_orders:rows.length,accepted_orders:accepted.length,
    rejected_orders:rejected.length,total_pence:accepted.reduce((s,r)=>s+r.total_pence,0),reasons}};
}
