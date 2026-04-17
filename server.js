const express = require('express');
const path = require('path');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://rlgsnznwdsfhpnsscrxs.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJsZ3Nuem53ZHNmaHBuc3NjcnhzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjA0MTExOCwiZXhwIjoyMDkxNjE3MTE4fQ.9bJxrUqpxa4gMqP5F4nJGH7Zp6IIZE8rNZQYk9p3FHM';
const STORE_ID = parseInt(process.env.STORE_ID || '1');

const sbHeaders = {
  'apikey': SUPABASE_KEY,
  'Authorization': `Bearer ${SUPABASE_KEY}`,
  'Content-Type': 'application/json',
  'Prefer': 'return=representation'
};

async function sbFetch(url, options = {}) {
  const { default: fetch } = await import('node-fetch');
  return fetch(url, options);
}

async function sbGet(table, params = '') {
  const url = `${SUPABASE_URL}/rest/v1/${table}?store_id=eq.${STORE_ID}&${params}`;
  const r = await sbFetch(url, { headers: sbHeaders });
  if (!r.ok) throw new Error(`GET ${table}: ${r.status} ${await r.text()}`);
  return r.json();
}

async function sbGetAll(table, params = '') {
  let all = [], offset = 0;
  while (true) {
    const url = `${SUPABASE_URL}/rest/v1/${table}?store_id=eq.${STORE_ID}&limit=1000&offset=${offset}&${params}`;
    const r = await sbFetch(url, { headers: sbHeaders });
    if (!r.ok) throw new Error(`GET ${table}: ${r.status}`);
    const batch = await r.json();
    all = all.concat(batch);
    if (batch.length < 1000) break;
    offset += 1000;
  }
  return all;
}

async function sbPost(table, data, prefer = 'return=representation') {
  const url = `${SUPABASE_URL}/rest/v1/${table}`;
  const r = await sbFetch(url, {
    method: 'POST',
    headers: { ...sbHeaders, 'Prefer': prefer },
    body: JSON.stringify(data)
  });
  if (!r.ok) throw new Error(`POST ${table}: ${r.status} ${await r.text()}`);
  return prefer.includes('representation') ? r.json() : { ok: true };
}

async function sbPatch(table, id, data) {
  const url = `${SUPABASE_URL}/rest/v1/${table}?id=eq.${id}&store_id=eq.${STORE_ID}`;
  const r = await sbFetch(url, { method: 'PATCH', headers: sbHeaders, body: JSON.stringify(data) });
  if (!r.ok) throw new Error(`PATCH ${table}: ${r.status} ${await r.text()}`);
  return r.json();
}

async function sbCount(table, filter = '') {
  const url = `${SUPABASE_URL}/rest/v1/${table}?store_id=eq.${STORE_ID}&select=id&${filter}`;
  const r = await sbFetch(url, { headers: { ...sbHeaders, 'Prefer': 'count=exact' } });
  const range = r.headers.get('content-range') || '*/0';
  return parseInt(range.split('/')[1]) || 0;
}

// ── PAGE ROUTES ───────────────────────────────────────────────────────────────
app.get('/staff', (req, res) => res.sendFile(path.join(__dirname, 'public', 'staff.html')));
app.get('/staff/reports', (req, res) => res.sendFile(path.join(__dirname, 'public', 'staff-reports.html')));
app.get('/owner', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.use(express.static(path.join(__dirname, 'public')));

// ── STORE ─────────────────────────────────────────────────────────────────────
app.get('/api/store', async (req, res) => {
  try {
    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/stores?id=eq.${STORE_ID}`, { headers: sbHeaders });
    const data = await r.json();
    res.json(data[0] || {});
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── DASHBOARD ─────────────────────────────────────────────────────────────────
app.get('/api/dashboard', async (req, res) => {
  try {
    const [totalItems, activeItems, beerItems, wineItems, lowInv, recentOrders, priceChanges] = await Promise.all([
      sbCount('items'),
      sbCount('items', 'status=eq.Active'),
      sbCount('items', 'status=eq.Active&category=in.(Beer,Single Beer,Custom Beer,NA-Beer)'),
      sbCount('items', 'status=eq.Active&category=in.(Wine,NA-Wine)'),
      sbGet('items', 'status=eq.Active&inventory=lte.2&inventory=gte.0&order=inventory.asc&limit=10&select=id,gg_name,category,inventory,abs_code'),
      sbGet('orders', 'order=created_at.desc&limit=5&select=id,order_name,order_type,order_date,status,total_items,estimated_cost'),
      sbGet('price_history', 'order=created_at.desc&limit=10&select=id,gg_name,abs_code,field_changed,old_value,new_value,change_pct,change_date'),
    ]);
    res.json({ totalItems, activeItems, beerItems, wineItems, lowInv, recentOrders, priceChanges });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── ITEMS ─────────────────────────────────────────────────────────────────────
app.get('/api/items', async (req, res) => {
  try {
    const { search, category, status, limit = 100, offset = 0 } = req.query;
    let params = `limit=${limit}&offset=${offset}&order=gg_name.asc`;
    if (category) params += `&category=eq.${encodeURIComponent(category)}`;
    if (status) params += `&status=eq.${encodeURIComponent(status)}`;
    if (search) params += `&or=(gg_name.ilike.*${encodeURIComponent(search)}*,abs_code.ilike.*${encodeURIComponent(search)}*,upc.ilike.*${encodeURIComponent(search)}*)`;
    const url = `${SUPABASE_URL}/rest/v1/items?store_id=eq.${STORE_ID}&${params}`;
    const r = await sbFetch(url, { headers: { ...sbHeaders, 'Prefer': 'count=exact' } });
    const data = await r.json();
    const total = parseInt((r.headers.get('content-range') || '*/0').split('/')[1]) || 0;
    res.json({ items: data, total });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/items/all', async (req, res) => {
  try {
    res.json(await sbGetAll('items', 'order=gg_name.asc&select=id,gg_name,abs_code,upc,category,status,sell_price,cost,margin_pct,inventory,vendor,bpc,splitted,made_from_abs,cost_per_unit'));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/items', async (req, res) => {
  try {
    const item = { ...req.body, store_id: STORE_ID };
    if (item.cost && item.bpc) item.cost_per_unit = parseFloat((item.cost / item.bpc).toFixed(6));
    if (item.sell_price && item.cost_per_unit) item.margin_pct = parseFloat(((item.sell_price - item.cost_per_unit) / item.sell_price).toFixed(6));
    const result = await sbPost('items', item);
    res.json(Array.isArray(result) ? result[0] : result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/items/:id', async (req, res) => {
  try {
    const updates = { ...req.body, updated_at: new Date().toISOString() };
    if (updates.cost && updates.bpc) updates.cost_per_unit = parseFloat((updates.cost / updates.bpc).toFixed(6));
    if (updates.sell_price && updates.cost_per_unit) updates.margin_pct = parseFloat(((updates.sell_price - updates.cost_per_unit) / updates.sell_price).toFixed(6));
    const oldR = await sbFetch(`${SUPABASE_URL}/rest/v1/items?id=eq.${req.params.id}`, { headers: sbHeaders });
    const old = (await oldR.json())[0] || {};
    const priceChanges = [];
    if (updates.cost && old.cost && Math.abs(parseFloat(updates.cost) - parseFloat(old.cost)) > 0.001)
      priceChanges.push({ store_id: STORE_ID, abs_code: old.abs_code, gg_name: old.gg_name, change_date: new Date().toISOString().slice(0,10), field_changed: 'Cost', old_value: parseFloat(old.cost), new_value: parseFloat(updates.cost), change_pct: parseFloat(((updates.cost - old.cost) / old.cost).toFixed(6)) });
    if (updates.sell_price && old.sell_price && Math.abs(parseFloat(updates.sell_price) - parseFloat(old.sell_price)) > 0.001)
      priceChanges.push({ store_id: STORE_ID, abs_code: old.abs_code, gg_name: old.gg_name, change_date: new Date().toISOString().slice(0,10), field_changed: 'Sell Price', old_value: parseFloat(old.sell_price), new_value: parseFloat(updates.sell_price), change_pct: parseFloat(((updates.sell_price - old.sell_price) / old.sell_price).toFixed(6)) });
    const result = await sbPatch('items', req.params.id, updates);
    if (priceChanges.length) await sbPost('price_history', priceChanges, 'return=minimal');
    res.json(Array.isArray(result) ? result[0] : result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── VENDORS ───────────────────────────────────────────────────────────────────
app.get('/api/vendors', async (req, res) => {
  try { res.json(await sbGet('vendors', 'order=vendor_name.asc')); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/vendors', async (req, res) => {
  try {
    const result = await sbPost('vendors', { ...req.body, store_id: STORE_ID });
    res.json(Array.isArray(result) ? result[0] : result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── ORDERS ────────────────────────────────────────────────────────────────────
app.get('/api/orders', async (req, res) => {
  try { res.json(await sbGet('orders', 'order=created_at.desc&limit=50')); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/orders', async (req, res) => {
  try {
    const { order, lines } = req.body;
    const result = await sbPost('orders', { ...order, store_id: STORE_ID });
    const saved = Array.isArray(result) ? result[0] : result;
    if (lines && lines.length && saved.id)
      await sbPost('order_lines', lines.map(l => ({ ...l, order_id: saved.id, store_id: STORE_ID })), 'return=minimal');
    res.json(saved);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/orders/:id/lines', async (req, res) => {
  try {
    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/order_lines?order_id=eq.${req.params.id}&order=gg_name.asc`, { headers: sbHeaders });
    res.json(await r.json());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── PRICE HISTORY ─────────────────────────────────────────────────────────────
app.get('/api/price-history', async (req, res) => {
  try {
    const { abs_code } = req.query;
    let params = 'order=created_at.desc&limit=100';
    if (abs_code) params += `&abs_code=eq.${encodeURIComponent(abs_code)}`;
    res.json(await sbGet('price_history', params));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── DAILY REPORTS ─────────────────────────────────────────────────────────────
app.get('/api/daily-reports', async (req, res) => {
  try {
    const { month, year, limit = 31 } = req.query;
    let params = `order=report_date.desc&limit=${limit}`;
    if (month && year) {
      const from = `${year}-${String(month).padStart(2,'0')}-01`;
      const to = `${year}-${String(month).padStart(2,'0')}-31`;
      params += `&report_date=gte.${from}&report_date=lte.${to}`;
    }
    res.json(await sbGet('gg_daily_reports', params));
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.get('/api/daily-reports/:date', async (req, res) => {
  try {
    const reports = await sbGet('gg_daily_reports', `report_date=eq.${req.params.date}&limit=1`);
    const report = reports[0] || null;
    if (!report) { res.json(null); return; }
    const [lines, payouts, drops] = await Promise.all([
      sbFetch(`${SUPABASE_URL}/rest/v1/scratch_inventory_lines?report_id=eq.${report.id}&order=face_value.asc`, { headers: sbHeaders }).then(r=>r.json()),
      sbFetch(`${SUPABASE_URL}/rest/v1/gg_daily_payouts?report_id=eq.${report.id}`, { headers: sbHeaders }).then(r=>r.json()),
      sbFetch(`${SUPABASE_URL}/rest/v1/gg_safe_drops?report_id=eq.${report.id}&order=drop_time.asc`, { headers: sbHeaders }).then(r=>r.json()),
    ]);
    res.json({ ...report, scratch_lines: lines, payouts, safe_drops: drops });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.post('/api/daily-reports', async (req, res) => {
  try {
    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/gg_daily_reports`, {
      method: 'POST',
      headers: { ...sbHeaders, 'Prefer': 'return=representation,resolution=merge-duplicates' },
      body: JSON.stringify({ ...req.body, store_id: STORE_ID })
    });
    const result = await r.json();
    res.json(Array.isArray(result) ? result[0] : result);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── SCRATCH DENOMINATIONS ─────────────────────────────────────────────────────
app.get('/api/scratch-denominations', async (req, res) => {
  try { res.json(await sbGet('scratch_denominations', 'is_active=eq.true&order=sort_order.asc')); }
  catch (e) { res.status(500).json({ error: e.message }); }
});

// ── CATEGORY STATS ────────────────────────────────────────────────────────────
app.get('/api/stats/categories', async (req, res) => {
  try {
    const items = await sbGetAll('items', 'status=eq.Active&select=category,inventory,margin_pct,sell_price');
    const cats = {};
    for (const item of items) {
      const cat = item.category || 'Other';
      if (!cats[cat]) cats[cat] = { count: 0, totalInventory: 0, margins: [], revenue_potential: 0 };
      cats[cat].count++;
      cats[cat].totalInventory += Math.max(0, parseFloat(item.inventory || 0));
      if (item.margin_pct) cats[cat].margins.push(parseFloat(item.margin_pct));
      if (item.sell_price && parseFloat(item.inventory) > 0) cats[cat].revenue_potential += parseFloat(item.sell_price) * parseFloat(item.inventory);
    }
    for (const cat of Object.keys(cats)) {
      const m = cats[cat].margins;
      cats[cat].avg_margin = m.length ? parseFloat((m.reduce((a,b)=>a+b,0)/m.length*100).toFixed(1)) : null;
      delete cats[cat].margins;
    }
    res.json(cats);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ── CATCH ALL ─────────────────────────────────────────────────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Grape & Grain running on port ${PORT}`);
  console.log(`Owner: /owner | Staff: /staff | Store: ${STORE_ID}`);
});
