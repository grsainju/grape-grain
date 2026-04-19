const express = require('express');
const path = require('path');
const cors = require('cors');

const app = express();
app.use(cors());
app.use(express.json({ limit: '10mb' }));

const SUPABASE_URL = process.env.SUPABASE_URL || 'https://rlgsnznwdsfhpnsscrxs.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InJsZ3Nuem53ZHNmaHBuc3NjcnhzIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3NjA0MTExOCwiZXhwIjoyMDkxNjE3MTE4fQ.9bJxrUqpxa4gMqP5F4nJGH7Zp6IIZE8rNZQYk9p3FHM';
const STORE_ID = parseInt(process.env.STORE_ID || '1');
const SQUARE_TOKEN = process.env.SQUARE_TOKEN || 'EAAAl97orZ29ofOnCX88UeQ-WL96DpCM1BXg85gRiRO_0DWbkSEdaI_BbqyUiUxs';
const SQUARE_LOCATION = process.env.SQUARE_LOCATION || '9RXJEVJ2DHQGG';
const SQUARE_BASE = 'https://connect.squareup.com/v2';
const SQUARE_VERSION = '2024-01-18';

const sbHeaders = {
  'apikey': SUPABASE_KEY,
  'Authorization': `Bearer ${SUPABASE_KEY}`,
  'Content-Type': 'application/json',
  'Prefer': 'return=representation'
};

const sqHeaders = {
  'Authorization': `Bearer ${SQUARE_TOKEN}`,
  'Content-Type': 'application/json',
  'Square-Version': SQUARE_VERSION
};

async function sbFetch(url, options = {}) {
  const { default: fetch } = await import('node-fetch');
  return fetch(url, options);
}
async function sqFetch(url, options = {}) {
  const { default: fetch } = await import('node-fetch');
  return fetch(SQUARE_BASE + url, { ...options, headers: { ...sqHeaders, ...(options.headers||{}) } });
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
  const r = await sbFetch(url, { method: 'POST', headers: { ...sbHeaders, 'Prefer': prefer }, body: JSON.stringify(data) });
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
app.get('/dashboard', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.use(express.static(path.join(__dirname, 'public')));

// ── SQUARE API PROXY ──────────────────────────────────────────────────────────

// Get sales summary for a specific date
app.get('/api/square/day', async (req, res) => {
  try {
    const date = req.query.date || new Date().toISOString().slice(0, 10);
    const startAt = `${date}T05:00:00Z`; // 1am ET = 5am UTC (adjust for DST)
    const endAt = `${date}T04:59:59Z`;   // end of business day next day

    // Use Reporting API for daily totals
    const r = await sqFetch('/v1/settlements', {});

    // Use Orders API to get day's orders
    const ordersR = await sqFetch('/orders/search', {
      method: 'POST',
      body: JSON.stringify({
        location_ids: [SQUARE_LOCATION],
        query: {
          filter: {
            date_time_filter: {
              created_at: { start_at: `${date}T00:00:00-05:00`, end_at: `${date}T23:59:59-05:00` }
            },
            state_filter: { states: ['COMPLETED'] }
          },
          sort: { sort_field: 'CREATED_AT', sort_order: 'ASC' }
        },
        limit: 500
      })
    });
    const ordersData = await ordersR.json();
    const orders = ordersData.orders || [];

    // Aggregate totals
    let netSales = 0, grossSales = 0, tax = 0, cashTotal = 0, cardTotal = 0, txnCount = orders.length;
    const categoryMap = {};

    for (const order of orders) {
      const net = (order.net_amounts?.total_money?.amount || 0) / 100;
      const gross = (order.total_money?.amount || 0) / 100;
      const taxAmt = (order.total_tax_money?.amount || 0) / 100;
      netSales += net;
      grossSales += gross;
      tax += taxAmt;

      // Payment breakdown
      for (const tender of (order.tenders || [])) {
        const tamt = (tender.amount_money?.amount || 0) / 100;
        if (tender.type === 'CASH') cashTotal += tamt;
        else cardTotal += tamt;
      }

      // Category breakdown from line items
      for (const item of (order.line_items || [])) {
        const cat = item.variation_name || item.name || 'Other';
        // Try to get category from catalog data
        categoryMap[cat] = (categoryMap[cat] || 0) + ((item.gross_sales_money?.amount || 0) / 100);
      }
    }

    const categories = Object.entries(categoryMap)
      .map(([name, netSales]) => ({ name, netSales: parseFloat(netSales.toFixed(2)) }))
      .sort((a, b) => b.netSales - a.netSales)
      .slice(0, 10);

    res.json({
      date,
      netSales: parseFloat(netSales.toFixed(2)),
      grossSales: parseFloat(grossSales.toFixed(2)),
      tax: parseFloat(tax.toFixed(2)),
      cash: parseFloat(cashTotal.toFixed(2)),
      card: parseFloat(cardTotal.toFixed(2)),
      transactions: txnCount,
      categories
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Get sales for a date range with category breakdown
app.get('/api/square/range', async (req, res) => {
  try {
    const { start, end } = req.query;
    if (!start || !end) return res.status(400).json({ error: 'start and end required' });

    const allOrders = [];
    let cursor = null;

    // Paginate through all orders
    do {
      const body = {
        location_ids: [SQUARE_LOCATION],
        query: {
          filter: {
            date_time_filter: {
              created_at: { start_at: `${start}T00:00:00-05:00`, end_at: `${end}T23:59:59-05:00` }
            },
            state_filter: { states: ['COMPLETED'] }
          }
        },
        limit: 500,
        ...(cursor && { cursor })
      };
      const r = await sqFetch('/orders/search', { method: 'POST', body: JSON.stringify(body) });
      const data = await r.json();
      if (data.errors) throw new Error(JSON.stringify(data.errors));
      allOrders.push(...(data.orders || []));
      cursor = data.cursor;
    } while (cursor);

    // Group by date
    const byDate = {};
    for (const order of allOrders) {
      const date = order.created_at.slice(0, 10);
      if (!byDate[date]) byDate[date] = { date, netSales: 0, grossSales: 0, tax: 0, cash: 0, card: 0, transactions: 0, categories: {} };
      const d = byDate[date];
      d.netSales += (order.net_amounts?.total_money?.amount || 0) / 100;
      d.grossSales += (order.total_money?.amount || 0) / 100;
      d.tax += (order.total_tax_money?.amount || 0) / 100;
      d.transactions++;
      for (const tender of (order.tenders || [])) {
        const tamt = (tender.amount_money?.amount || 0) / 100;
        if (tender.type === 'CASH') d.cash += tamt;
        else d.card += tamt;
      }
    }

    const days = Object.values(byDate).map(d => ({
      ...d,
      netSales: parseFloat(d.netSales.toFixed(2)),
      grossSales: parseFloat(d.grossSales.toFixed(2)),
      tax: parseFloat(d.tax.toFixed(2)),
      cash: parseFloat(d.cash.toFixed(2)),
      card: parseFloat(d.card.toFixed(2)),
    })).sort((a, b) => a.date.localeCompare(b.date));

    res.json({ days, totalOrders: allOrders.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// (old categories route removed — use /api/square/categories from catalog)

// ── SQUARE SYNC — Pull sales data and store in Supabase ───────────────────────
app.post('/api/square/sync', async (req, res) => {
  try {
    const { start, end } = req.body;
    const startDate = start || new Date(Date.now() - 86400000).toISOString().slice(0,10);
    const endDate = end || new Date().toISOString().slice(0,10);

    console.log(`Square sync: ${startDate} → ${endDate}`);

    const allOrders = [];
    let cursor = null;
    let pageCount = 0;

    do {
      const body = {
        location_ids: [SQUARE_LOCATION],
        query: {
          filter: {
            date_time_filter: {
              created_at: { start_at: `${startDate}T00:00:00-05:00`, end_at: `${endDate}T23:59:59-05:00` }
            },
            state_filter: { states: ['COMPLETED'] }
          }
        },
        limit: 500,
        ...(cursor && { cursor })
      };
      const r = await sqFetch('/orders/search', { method: 'POST', body: JSON.stringify(body) });
      const data = await r.json();
      if (data.errors) throw new Error(data.errors[0]?.detail || JSON.stringify(data.errors));
      allOrders.push(...(data.orders || []));
      cursor = data.cursor;
      pageCount++;
      if (pageCount > 50) break; // Safety limit
    } while (cursor);

    console.log(`Fetched ${allOrders.length} orders`);

    // Aggregate by date
    const byDate = {};
    const categoryByDate = {};

    for (const order of allOrders) {
      // Square timestamps are in UTC, convert to ET
      const utcDate = new Date(order.created_at);
      // ET is UTC-5 (EST) or UTC-4 (EDT) — use simple offset
      const etOffset = -5 * 60 * 60 * 1000;
      const etDate = new Date(utcDate.getTime() + etOffset);
      const date = etDate.toISOString().slice(0, 10);

      if (!byDate[date]) byDate[date] = { date, netSales: 0, grossSales: 0, tax: 0, cash: 0, card: 0, transactions: 0 };
      if (!categoryByDate[date]) categoryByDate[date] = {};

      const d = byDate[date];
      d.netSales += (order.net_amounts?.total_money?.amount || 0) / 100;
      d.grossSales += (order.total_money?.amount || 0) / 100;
      d.tax += (order.total_tax_money?.amount || 0) / 100;
      d.transactions++;

      for (const tender of (order.tenders || [])) {
        const tamt = (tender.amount_money?.amount || 0) / 100;
        if (tender.type === 'CASH') d.cash += tamt;
        else d.card += tamt;
      }

      // Line items → categories
      for (const item of (order.line_items || [])) {
        const cat = item.catalog_object_id ? 'cat_' + item.catalog_object_id : (item.name || 'Other');
        const amt = (item.gross_sales_money?.amount || 0) / 100;
        categoryByDate[date][cat] = (categoryByDate[date][cat] || { name: item.name || cat, amount: 0 });
        categoryByDate[date][cat].amount += amt;
      }
    }

    // Upsert into square_daily_sales table
    const rows = Object.values(byDate).map(d => ({
      store_id: STORE_ID,
      sale_date: d.date,
      net_sales: parseFloat(d.netSales.toFixed(2)),
      gross_sales: parseFloat(d.grossSales.toFixed(2)),
      tax: parseFloat(d.tax.toFixed(2)),
      cash_amount: parseFloat(d.cash.toFixed(2)),
      card_amount: parseFloat(d.card.toFixed(2)),
      transaction_count: d.transactions,
      categories: categoryByDate[d.date] || {},
      synced_at: new Date().toISOString()
    }));

    if (rows.length > 0) {
      const upsertR = await sbFetch(`${SUPABASE_URL}/rest/v1/square_daily_sales?on_conflict=store_id,sale_date`, {
        method: 'POST',
        headers: { ...sbHeaders, 'Prefer': 'return=minimal,resolution=merge-duplicates' },
        body: JSON.stringify(rows)
      });
      if (!upsertR.ok) {
        const errText = await upsertR.text();
        throw new Error(`Supabase upsert failed: ${errText}`);
      }
    }

    res.json({ success: true, datesProcessed: rows.length, ordersProcessed: allOrders.length, dateRange: `${startDate} → ${endDate}` });
  } catch (e) {
    console.error('Square sync error:', e);
    res.status(500).json({ error: e.message });
  }
});

// Get stored Square data for a date
app.get('/api/square/stored/day', async (req, res) => {
  try {
    const date = req.query.date || new Date().toISOString().slice(0, 10);
    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/square_daily_sales?store_id=eq.${STORE_ID}&sale_date=eq.${date}&limit=1`, { headers: sbHeaders });
    const data = await r.json();
    res.json(data[0] || null);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Get stored Square data for a month
app.get('/api/square/stored/month', async (req, res) => {
  try {
    const { month, year } = req.query;
    const from = `${year}-${String(month).padStart(2,'0')}-01`;
    const daysInMonth = new Date(parseInt(year), parseInt(month), 0).getDate();
    const to = `${year}-${String(month).padStart(2,'0')}-${daysInMonth}`;
    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/square_daily_sales?store_id=eq.${STORE_ID}&sale_date=gte.${from}&sale_date=lte.${to}&order=sale_date.asc`, { headers: sbHeaders });
    res.json(await r.json());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

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
    const [totalItems, activeItems, beerItems, wineItems, lowInv, priceChanges] = await Promise.all([
      sbCount('items'),
      sbCount('items', 'status=eq.Active'),
      sbCount('items', 'status=eq.Active&category=in.(Beer,Single Beer,Custom Beer,NA-Beer)'),
      sbCount('items', 'status=eq.Active&category=in.(Wine,NA-Wine)'),
      sbGet('items', 'status=eq.Active&inventory=lte.2&inventory=gte.0&order=inventory.asc&limit=10&select=id,gg_name,category,inventory,abs_code'),
      sbGet('price_history', 'order=created_at.desc&limit=10&select=id,gg_name,abs_code,field_changed,old_value,new_value,change_pct,change_date'),
    ]);
    res.json({ totalItems, activeItems, beerItems, wineItems, lowInv, priceChanges });
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
      const daysInM = new Date(parseInt(year), parseInt(month), 0).getDate();
      const to = `${year}-${String(month).padStart(2,'0')}-${daysInM}`;
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


// ── SQUARE CATALOG SYNC ───────────────────────────────────────────────────────

// Sync catalog: categories + items → Supabase
app.post('/api/square/sync-catalog', async (req, res) => {
  try {
    console.log('Starting catalog sync...');
    const { default: fetch } = await import('node-fetch');

    // Step 1: Pull all catalog objects (CATEGORY + ITEM_VARIATION)
    const allObjects = [];
    let cursor = null;
    let page = 0;

    do {
      const url = `${SQUARE_BASE}/catalog/list?types=CATEGORY,ITEM,ITEM_VARIATION${cursor ? '&cursor=' + cursor : ''}`;
      const r = await fetch(url, { headers: sqHeaders });
      const data = await r.json();
      if (data.errors) throw new Error(data.errors[0]?.detail || JSON.stringify(data.errors));
      allObjects.push(...(data.objects || []));
      cursor = data.cursor;
      page++;
      if (page > 100) break; // safety
    } while (cursor);

    console.log(`Fetched ${allObjects.length} catalog objects`);

    // Step 2: Separate categories and items
    const categories = allObjects.filter(o => o.type === 'CATEGORY' && !o.is_deleted);
    const items = allObjects.filter(o => o.type === 'ITEM' && !o.is_deleted);

    // Step 3: Build category map
    const catMap = {};
    categories.forEach(c => {
      catMap[c.id] = {
        id: c.id,
        store_id: STORE_ID,
        name: c.category_data?.name || 'Unknown',
        ordinal: c.category_data?.ordinal || 0,
        updated_at: new Date().toISOString()
      };
    });

    // Step 4: Upsert categories
    if (categories.length > 0) {
      const catRows = Object.values(catMap);
      const catR = await sbFetch(`${SUPABASE_URL}/rest/v1/square_categories?on_conflict=id`, {
        method: 'POST',
        headers: { ...sbHeaders, 'Prefer': 'return=minimal,resolution=merge-duplicates' },
        body: JSON.stringify(catRows)
      });
      if (!catR.ok) throw new Error('Category upsert failed: ' + await catR.text());
    }

    // Step 5: Build item rows (one row per variation)
    const itemRows = [];
    for (const item of items) {
      // Square newer API: categories is an array; older API: category_id string
      const categoriesArr = item.item_data?.categories || [];
      const categoryId = categoriesArr.length > 0 
        ? categoriesArr[0].id 
        : (item.item_data?.category_id || item.item_data?.reporting_category?.id || null);
      const categoryName = categoryId ? (catMap[categoryId]?.name || null) : null;
      const name = item.item_data?.name || 'Unknown';
      const description = item.item_data?.description || null;
      const variations = item.item_data?.variations || [];

      if (variations.length === 0) {
        // Item with no variations
        itemRows.push({
          id: item.id,
          store_id: STORE_ID,
          name,
          category_id: categoryId,
          category_name: categoryName,
          description,
          sku: null,
          upc: null,
          price_cents: null,
          price: null,
          variation_name: null,
          is_deleted: false,
          updated_at: new Date().toISOString()
        });
      } else {
        for (const v of variations) {
          if (v.is_deleted) continue;
          const vData = v.item_variation_data || {};
          const priceCents = vData.price_money?.amount || null;
          itemRows.push({
            id: v.id,
            store_id: STORE_ID,
            name,
            category_id: categoryId,
            category_name: categoryName,
            description,
            sku: vData.sku || null,
            upc: vData.upc || null,
            price_cents: priceCents,
            price: priceCents ? parseFloat((priceCents / 100).toFixed(2)) : null,
            variation_name: variations.length > 1 ? (vData.name || null) : null,
            is_deleted: false,
            updated_at: new Date().toISOString()
          });
        }
      }
    }

    console.log(`Upserting ${itemRows.length} item variations...`);

    // Step 6: Upsert items in batches of 200
    const batchSize = 200;
    for (let i = 0; i < itemRows.length; i += batchSize) {
      const batch = itemRows.slice(i, i + batchSize);
      const itemR = await sbFetch(`${SUPABASE_URL}/rest/v1/square_items?on_conflict=id`, {
        method: 'POST',
        headers: { ...sbHeaders, 'Prefer': 'return=minimal,resolution=merge-duplicates' },
        body: JSON.stringify(batch)
      });
      if (!itemR.ok) throw new Error('Item upsert failed: ' + await itemR.text());
    }

    // Step 7: Re-aggregate daily sales by category using the new catalog
    // Build item_id → category_name map from what we just inserted
    const itemCatMap = {};
    itemRows.forEach(r => { itemCatMap[r.id] = r.category_name || 'Other'; });

    // Re-process square_daily_sales categories JSONB
    const salesR = await sbFetch(`${SUPABASE_URL}/rest/v1/square_daily_sales?store_id=eq.${STORE_ID}&select=id,sale_date,categories`, { headers: sbHeaders });
    const salesRows = await salesR.json();

    let reprocessed = 0;
    for (const row of salesRows) {
      const rawCats = row.categories || {};
      const newCats = {};
      for (const [itemId, itemData] of Object.entries(rawCats)) {
        const catName = itemCatMap[itemId] || itemData.category_name || 'Other';
        if (!newCats[catName]) newCats[catName] = { name: catName, amount: 0 };
        newCats[catName].amount += parseFloat(itemData.amount || 0);
      }
      // Update this row
      await sbFetch(`${SUPABASE_URL}/rest/v1/square_daily_sales?id=eq.${row.id}`, {
        method: 'PATCH',
        headers: sbHeaders,
        body: JSON.stringify({ categories: newCats })
      });
      reprocessed++;
    }

    res.json({
      success: true,
      categories: categories.length,
      items: items.length,
      variations: itemRows.length,
      salesRowsReprocessed: reprocessed
    });

  } catch (e) {
    console.error('Catalog sync error:', e);
    res.status(500).json({ error: e.message });
  }
});

// Get all categories from Square catalog
app.get('/api/square/categories', async (req, res) => {
  try {
    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/square_categories?store_id=eq.${STORE_ID}&order=name.asc`, { headers: sbHeaders });
    res.json(await r.json());
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Search Square items
app.get('/api/square/items', async (req, res) => {
  try {
    const { search, category, limit = 50, offset = 0 } = req.query;
    let params = `store_id=eq.${STORE_ID}&is_deleted=eq.false&limit=${limit}&offset=${offset}&order=name.asc`;
    if (category) params += `&category_name=eq.${encodeURIComponent(category)}`;
    if (search) params += `&name=ilike.*${encodeURIComponent(search)}*`;
    const url = `${SUPABASE_URL}/rest/v1/square_items?${params}`;
    const r = await sbFetch(url, { headers: { ...sbHeaders, 'Prefer': 'count=exact' } });
    const data = await r.json();
    const total = parseInt((r.headers.get('content-range') || '*/0').split('/')[1]) || 0;
    res.json({ items: data, total });
  } catch (e) { res.status(500).json({ error: e.message }); }
});


// Debug: show raw Square catalog structure for first item
app.get('/api/square/catalog/raw', async (req, res) => {
  try {
    const r = await sqFetch('/catalog/list?types=ITEM&limit=2');
    const data = await r.json();
    const items = data.objects || [];
    // Return full raw structure of first item
    res.json({
      count: items.length,
      first: items[0] || null,
      item_data_keys: items[0] ? Object.keys(items[0].item_data || {}) : [],
      categories_field: items[0]?.item_data?.categories,
      category_id_field: items[0]?.item_data?.category_id,
      reporting_category: items[0]?.item_data?.reporting_category,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── CATCH ALL ─────────────────────────────────────────────────────────────────
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

// ── NIGHTLY SYNC (called by cron or manually) ─────────────────────────────────
async function runNightlySync() {
  try {
    const yesterday = new Date(Date.now() - 86400000).toISOString().slice(0,10);
    const { default: fetch } = await import('node-fetch');
    const r = await fetch(`http://localhost:${PORT}/api/square/sync`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ start: yesterday, end: yesterday })
    });
    const result = await r.json();
    console.log('Nightly sync result:', result);
  } catch (e) {
    console.error('Nightly sync failed:', e.message);
  }
}

// Schedule nightly sync at 2am ET
function scheduleSyncs() {
  const now = new Date();
  const nextSync = new Date();
  nextSync.setHours(7, 0, 0, 0); // 2am ET = 7am UTC
  if (nextSync <= now) nextSync.setDate(nextSync.getDate() + 1);
  const msUntilSync = nextSync - now;
  console.log(`Next sync scheduled in ${Math.round(msUntilSync/1000/60)} minutes`);
  setTimeout(() => {
    runNightlySync();
    setInterval(runNightlySync, 24 * 60 * 60 * 1000); // then every 24h
  }, msUntilSync);
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Grape & Grain running on port ${PORT}`);
  console.log(`Square: location ${SQUARE_LOCATION}`);
  scheduleSyncs();
});
