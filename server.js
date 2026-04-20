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

// Get live sales summary for a specific date
// Triggers background sync then returns stored+live data
app.get('/api/square/day', async (req, res) => {
  try {
    const date = req.query.date || new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
    const isToday = date === new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });

    // For today: run a fresh sync in background, return stored data immediately
    // For past dates: just return stored data
    // Trigger background sync to keep stored data fresh (don't await)
    (async () => {
      try {
        const body = { start: date, end: date };
        const { default: fetch } = await import('node-fetch');
        await fetch(`http://localhost:${process.env.PORT || 3000}/api/square/sync`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(body)
        });
      } catch(e) { console.log('Background sync error:', e.message); }
    })();

    // Live pull - top 5 items by name directly from order line items
    try {
      const allOrders = [];
      let orderCursor = null;
      const nextDay = new Date(date + 'T06:00:00Z');
      nextDay.setDate(nextDay.getDate() + 1);
      const endTime = nextDay.toISOString().slice(0,19) + 'Z';

      do {
        const ordersR = await sqFetch('/orders/search', {
          method: 'POST',
          body: JSON.stringify({
            location_ids: [SQUARE_LOCATION],
            query: {
              filter: {
                date_time_filter: { created_at: { start_at: `${date}T06:00:00Z`, end_at: endTime } },
                state_filter: { states: ['COMPLETED'] }
              }
            },
            limit: 500,
            ...(orderCursor && { cursor: orderCursor })
          })
        });
        const od = await ordersR.json();
        if (od.errors) throw new Error(od.errors[0]?.detail);
        allOrders.push(...(od.orders || []));
        orderCursor = od.cursor;
      } while (orderCursor);

      let liveGross = 0, liveTax = 0, liveDisc = 0, liveRet = 0;
      let liveCash = 0, liveCard = 0;
      const itemMap = {};

      for (const order of allOrders) {
        liveGross += (order.total_money?.amount || 0) / 100;
        liveTax += (order.total_tax_money?.amount || 0) / 100;
        liveDisc += (order.total_discount_money?.amount || 0) / 100;
        liveRet += (order.return_amounts?.total_money?.amount || 0) / 100;
        for (const tender of (order.tenders || [])) {
          const ta = (tender.amount_money?.amount || 0) / 100;
          if (tender.type === 'CASH') liveCash += ta; else liveCard += ta;
        }
        for (const item of (order.line_items || [])) {
          const name = item.name || 'Unknown';
          const amt = (item.gross_sales_money?.amount || 0) / 100;
          if (amt <= 0) continue;
          itemMap[name] = (itemMap[name] || 0) + amt;
        }
      }

      const top5 = Object.entries(itemMap)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([name, sales]) => ({ name, netSales: parseFloat(sales.toFixed(2)) }));

      // Fees
      let totalFees = 0;
      try {
        let feesCursor = null, feesPage = 0;
        do {
          const pr = await sqFetch(`/payments?location_id=${SQUARE_LOCATION}&begin_time=${date}T06:00:00Z&end_time=${endTime}&limit=100${feesCursor ? '&cursor=' + feesCursor : ''}`);
          const pd = await pr.json();
          for (const p of (pd.payments || [])) {
            if (Array.isArray(p.processing_fee)) {
              for (const f of p.processing_fee) {
                if (f.type === 'INITIAL') totalFees += Math.abs((f.amount_money?.amount || 0)) / 100;
              }
            }
          }
          feesCursor = pd.cursor; feesPage++;
        } while (feesCursor && feesPage < 10);
      } catch(e) { console.log('Fees error:', e.message); }

      return res.json({
        date,
        netSales: parseFloat((liveGross - liveTax).toFixed(2)),
        grossSales: parseFloat(liveGross.toFixed(2)),
        tax: parseFloat(liveTax.toFixed(2)),
        discounts: parseFloat(liveDisc.toFixed(2)),
        returns: parseFloat(liveRet.toFixed(2)),
        fees: parseFloat(totalFees.toFixed(2)),
        cash: parseFloat(liveCash.toFixed(2)),
        card: parseFloat(liveCard.toFixed(2)),
        transactions: allOrders.length,
        categories: top5,
        syncedAt: new Date().toISOString(),
        isLive: true
      });
    } catch(e) {
      console.log(`[LIVE] Failed: ${e.message}`);
      // Fall through to stored data
    }

    // Return stored data (most recent sync)
    const storedR = await sbFetch(`${SUPABASE_URL}/rest/v1/square_daily_sales?store_id=eq.${STORE_ID}&sale_date=eq.${date}&limit=1`, { headers: sbHeaders });
    const stored = await storedR.json();
    const row = stored[0];

    if (!row) {
      // No stored data — do a direct sync and return
      const syncR = await sbFetch(`http://localhost:${process.env.PORT || 3000}/api/square/sync`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ start: date, end: date })
      }).catch(() => null);
      return res.json({ date, netSales: 0, transactions: 0, categories: [], synced_at: null, noData: true });
    }

    // Get categories from imported CSV data (most accurate)
    const catSalesR = await sbFetch(
      `${SUPABASE_URL}/rest/v1/square_category_sales?store_id=eq.${STORE_ID}&sale_date=eq.${date}&order=gross_sales.desc`,
      { headers: sbHeaders }
    );
    const catSales = await catSalesR.json();

    let scratchTotal = 0;
    const catList = [];

    if (catSales && catSales.length > 0) {
      // Use imported CSV categories — exact match to Square dashboard
      for (const cat of catSales) {
        const amt = parseFloat(cat.gross_sales || 0);
        if (cat.category === 'Scratch Lotto') { scratchTotal += amt; continue; }
        catList.push({ name: cat.category, netSales: amt });
      }
    } else {
      // Fall back to JSONB categories from order sync — filter out Other and items
      const cats = row.categories || {};
      const knownCats = new Set(['Beer','Wine','Single Beer','Custom Beer','Cigar','Tobacco','Drinks','Snacks','Candy','Misc.','Household','NA-Beer','NA-Wine','Scratch Lotto','Draft Beer','Hot Food','Meds','Old Products','Old Lotto','On Hold','Discount Wines','Not Available']);
      for (const [k, v] of Object.entries(cats)) {
        const name = v.name || k;
        const amt = parseFloat(v.amount || 0);
        if (name === 'Scratch Lotto') { scratchTotal += amt; continue; }
        if (name === 'Other' || !knownCats.has(name)) continue; // skip items/other
        catList.push({ name, netSales: amt });
      }
      catList.sort((a,b) => b.netSales - a.netSales);
    }

    // Net Sales = Gross Sales - Tax - Scratch Lotto
    const grossSales = parseFloat(row.gross_sales || 0);
    const tax = parseFloat(row.tax || 0);
    const discounts = parseFloat(row.discounts || 0);
    const returns = parseFloat(row.returns || 0);
    const netSales = grossSales - tax - scratchTotal;

    res.json({
      date,
      netSales: parseFloat(netSales.toFixed(2)),
      grossSales: parseFloat(grossSales.toFixed(2)),
      tax: parseFloat(tax.toFixed(2)),
      discounts: parseFloat(discounts.toFixed(2)),
      returns: parseFloat(returns.toFixed(2)),
      scratchLotto: parseFloat(scratchTotal.toFixed(2)),
      cash: parseFloat(row.cash_amount || 0),
      card: parseFloat(row.card_amount || 0),
      transactions: row.transaction_count || 0,
      categories: catList.slice(0, 5),
      syncedAt: row.synced_at
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
      d.discounts += (order.total_discount_money?.amount || 0) / 100;
      d.returns += (order.return_amounts?.total_money?.amount || 0) / 100;
      // Square processing fees are not in orders API — tracked separately
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
              created_at: { start_at: `${startDate}T06:00:00Z`, end_at: `${endDate}T05:59:59Z` }
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
      // Square reporting day starts at 2am EDT (6am UTC)
      // Subtract 6 hours to align with Square's reporting day
      const utcDate = new Date(order.created_at);
      const squareDate = new Date(utcDate.getTime() - 6 * 60 * 60 * 1000);
      const date = squareDate.toISOString().slice(0, 10);

      if (!byDate[date]) byDate[date] = { date, netSales: 0, grossSales: 0, tax: 0, cash: 0, card: 0, transactions: 0, discounts: 0, returns: 0, fees: 0 };
      if (!categoryByDate[date]) categoryByDate[date] = {};

      const d = byDate[date];
      d.netSales += (order.net_amounts?.total_money?.amount || 0) / 100;
      d.grossSales += (order.total_money?.amount || 0) / 100;
      d.tax += (order.total_tax_money?.amount || 0) / 100;
      d.discounts += (order.total_discount_money?.amount || 0) / 100;
      d.returns += (order.return_amounts?.total_money?.amount || 0) / 100;
      // Square processing fees are not in orders API — tracked separately
      d.transactions++;

      for (const tender of (order.tenders || [])) {
        const tamt = (tender.amount_money?.amount || 0) / 100;
        if (tender.type === 'CASH') d.cash += tamt;
        else d.card += tamt;
      }

      // Line items → store by catalog_object_id (variation ID) for category mapping
      for (const item of (order.line_items || [])) {
        const varId = item.catalog_object_id || null;
        const itemName = item.name || 'Unknown';
        const amt = (item.gross_sales_money?.amount || 0) / 100;
        if (amt <= 0) continue;
        // Key by variation ID so catalog sync can map to category
        const key = varId || ('name_' + itemName.substring(0, 40));
        if (!categoryByDate[date][key]) {
          categoryByDate[date][key] = { name: itemName, amount: 0, variation_id: varId };
        }
        categoryByDate[date][key].amount += amt;
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
      discounts: parseFloat(d.discounts.toFixed(2)),
      returns: parseFloat(d.returns.toFixed(2)),
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
    // Paginate through all rows
    let reprocessed = 0;
    let salesOffset = 0;
    while (true) {
      const salesR = await sbFetch(`${SUPABASE_URL}/rest/v1/square_daily_sales?store_id=eq.${STORE_ID}&select=id,sale_date,categories&limit=100&offset=${salesOffset}`, { headers: sbHeaders });
      const salesRows = await salesR.json();
      if (!salesRows.length) break;

      for (const row of salesRows) {
        const rawCats = row.categories || {};
        const newCats = {};
        for (const [key, itemData] of Object.entries(rawCats)) {
          const amt = parseFloat(itemData.amount || 0);
          if (amt <= 0) continue;
          const varId = itemData.variation_id || key;
          // Look up category by variation ID
          const catName = itemCatMap[varId] || itemCatMap[key] || 'Other';
          if (!newCats[catName]) newCats[catName] = { name: catName, amount: 0 };
          newCats[catName].amount += amt;
        }
        await sbFetch(`${SUPABASE_URL}/rest/v1/square_daily_sales?id=eq.${row.id}`, {
          method: 'PATCH',
          headers: sbHeaders,
          body: JSON.stringify({ categories: newCats })
        });
        reprocessed++;
      }
      if (salesRows.length < 100) break;
      salesOffset += 100;
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



// ── CSV IMPORT ENDPOINTS ──────────────────────────────────────────────────────

// Parse Square CSV wide format → array of {date, name, value} rows
function parseSquareCSV(csvText) {
  const lines = csvText.trim().split('\n');
  const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
  // First column is the label (Category or Item Name), rest are dates
  const dateColumns = headers.slice(1); // may include Item Variation, Category for items
  const rows = [];

  for (let i = 1; i < lines.length; i++) {
    const line = lines[i];
    if (!line.trim()) continue;
    // Parse CSV respecting quotes
    const cells = [];
    let cur = '', inQ = false;
    for (const ch of line) {
      if (ch === '"') inQ = !inQ;
      else if (ch === ',' && !inQ) { cells.push(cur.trim()); cur = ''; }
      else cur += ch;
    }
    cells.push(cur.trim());
    rows.push(cells.map(c => c.replace(/^"|"$/g, '')));
  }
  return { headers, rows };
}

function parseMoney(str) {
  if (!str) return 0;
  const n = parseFloat(str.replace(/[$,]/g, ''));
  return isNaN(n) ? 0 : n;
}

function parseSquareDate(str) {
  // MM/DD/YYYY → YYYY-MM-DD
  const m = str.trim().match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!m) return null;
  return `${m[3]}-${m[1].padStart(2,'0')}-${m[2].padStart(2,'0')}`;
}

// Import Category Sales CSV
app.post('/api/import/category-sales', async (req, res) => {
  try {
    const { csv } = req.body;
    if (!csv) return res.status(400).json({ error: 'No CSV data provided' });

    const { headers, rows } = parseSquareCSV(csv);
    // headers[0] = 'Category', rest are dates like '01/01/2026'
    const dateHeaders = headers.slice(1).filter(h => h.match(/\d{1,2}\/\d{1,2}\/\d{4}/));

    const toInsert = [];
    for (const row of rows) {
      const category = row[0];
      if (!category || category === 'Total') continue;
      for (let i = 0; i < dateHeaders.length; i++) {
        const dateStr = parseSquareDate(dateHeaders[i]);
        if (!dateStr) continue;
        const gross = parseMoney(row[i + 1]);
        if (gross === 0) continue; // skip zero rows
        toInsert.push({
          store_id: STORE_ID,
          sale_date: dateStr,
          category: category.trim(),
          gross_sales: gross
        });
      }
    }

    if (!toInsert.length) return res.json({ inserted: 0, message: 'No data to insert' });

    // Upsert in batches of 500
    let inserted = 0;
    for (let i = 0; i < toInsert.length; i += 500) {
      const batch = toInsert.slice(i, i + 500);
      const r = await sbFetch(`${SUPABASE_URL}/rest/v1/square_category_sales?on_conflict=store_id,sale_date,category`, {
        method: 'POST',
        headers: { ...sbHeaders, 'Prefer': 'return=minimal,resolution=merge-duplicates' },
        body: JSON.stringify(batch)
      });
      if (!r.ok) throw new Error('Insert failed: ' + await r.text());
      inserted += batch.length;
    }

    // Get date range imported
    const dates = toInsert.map(r => r.sale_date).sort();
    res.json({ success: true, inserted, dateRange: `${dates[0]} → ${dates[dates.length-1]}`, categories: [...new Set(toInsert.map(r=>r.category))].length });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Import Item Sales CSV
app.post('/api/import/item-sales', async (req, res) => {
  try {
    const { csv } = req.body;
    if (!csv) return res.status(400).json({ error: 'No CSV data provided' });

    const { headers, rows } = parseSquareCSV(csv);
    // headers: Item Name, Item Variation, Category, 01/01/2026, 01/02/2026, ...
    const dateStartIdx = headers.findIndex(h => h.match(/\d{1,2}\/\d{1,2}\/\d{4}/));
    if (dateStartIdx === -1) return res.status(400).json({ error: 'No date columns found' });
    const dateHeaders = headers.slice(dateStartIdx);

    const toInsert = [];
    for (const row of rows) {
      const itemName = row[0];
      const variation = row[1] || 'Regular';
      const category = row[2] || 'Other';
      if (!itemName || itemName === 'Total') continue;
      for (let i = 0; i < dateHeaders.length; i++) {
        const dateStr = parseSquareDate(dateHeaders[i]);
        if (!dateStr) continue;
        const gross = parseMoney(row[dateStartIdx + i]);
        if (gross === 0) continue;
        toInsert.push({
          store_id: STORE_ID,
          sale_date: dateStr,
          item_name: itemName.trim(),
          item_variation: variation.trim(),
          category: category.trim(),
          gross_sales: gross
        });
      }
    }

    if (!toInsert.length) return res.json({ inserted: 0, message: 'No data to insert' });

    let inserted = 0;
    for (let i = 0; i < toInsert.length; i += 500) {
      const batch = toInsert.slice(i, i + 500);
      const r = await sbFetch(`${SUPABASE_URL}/rest/v1/square_item_sales?on_conflict=store_id,sale_date,item_name,item_variation`, {
        method: 'POST',
        headers: { ...sbHeaders, 'Prefer': 'return=minimal,resolution=merge-duplicates' },
        body: JSON.stringify(batch)
      });
      if (!r.ok) throw new Error('Insert failed: ' + await r.text());
      inserted += batch.length;
    }

    const dates = toInsert.map(r => r.sale_date).sort();
    res.json({ success: true, inserted, dateRange: `${dates[0]} → ${dates[dates.length-1]}`, items: [...new Set(toInsert.map(r=>r.item_name))].length });
  } catch(e) {
    res.status(500).json({ error: e.message });
  }
});

// Get category sales for month (from imported CSV data)
app.get('/api/sales/categories/month', async (req, res) => {
  try {
    const { month, year } = req.query;
    const daysInMonth = new Date(parseInt(year), parseInt(month), 0).getDate();
    const from = `${year}-${String(month).padStart(2,'0')}-01`;
    const to = `${year}-${String(month).padStart(2,'0')}-${daysInMonth}`;
    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/square_category_sales?store_id=eq.${STORE_ID}&sale_date=gte.${from}&sale_date=lte.${to}&order=sale_date.asc`, { headers: sbHeaders });
    const rows = await r.json();
    // Aggregate by category
    const catMap = {};
    for (const row of rows) {
      catMap[row.category] = (catMap[row.category]||0) + parseFloat(row.gross_sales||0);
    }
    const sorted = Object.entries(catMap).sort((a,b)=>b[1]-a[1]).map(([category,gross_sales])=>({category, gross_sales: parseFloat(gross_sales.toFixed(2))}));
    res.json({ categories: sorted, dateRange: `${from} → ${to}` });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Get daily category totals for a month (for weekly bars)
app.get('/api/sales/daily/month', async (req, res) => {
  try {
    const { month, year } = req.query;
    const daysInMonth = new Date(parseInt(year), parseInt(month), 0).getDate();
    const from = `${year}-${String(month).padStart(2,'0')}-01`;
    const to = `${year}-${String(month).padStart(2,'0')}-${daysInMonth}`;
    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/square_category_sales?store_id=eq.${STORE_ID}&sale_date=gte.${from}&sale_date=lte.${to}&select=sale_date,gross_sales&order=sale_date.asc`, { headers: sbHeaders });
    const rows = await r.json();
    // Aggregate by date
    const dateMap = {};
    for (const row of rows) {
      dateMap[row.sale_date] = (dateMap[row.sale_date]||0) + parseFloat(row.gross_sales||0);
    }
    res.json(Object.entries(dateMap).map(([date,gross])=>({date, gross_sales: parseFloat(gross.toFixed(2))})));
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// Debug: show raw Square order structure for money fields
app.get('/api/square/debug-order', async (req, res) => {
  try {
    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
    const r = await sqFetch('/orders/search', {
      method: 'POST',
      body: JSON.stringify({
        location_ids: [SQUARE_LOCATION],
        query: {
          filter: {
            date_time_filter: { created_at: { start_at: `${today}T00:00:00-05:00`, end_at: `${today}T23:59:59-05:00` } },
            state_filter: { states: ['COMPLETED'] }
          }
        },
        limit: 2
      })
    });
    const data = await r.json();
    const order = data.orders?.[0];
    if (!order) return res.json({ error: 'No orders today', count: 0 });

    // Show all money-related fields
    // Also fetch payment for this order to see fees
    let paymentFees = null;
    if (order.tenders?.[0]?.payment_id) {
      const pr = await sqFetch(`/payments/${order.tenders[0].payment_id}`);
      const pd = await pr.json();
      paymentFees = pd.payment?.processing_fee;
    }

    res.json({
      order_id: order.id,
      total_money: order.total_money,
      net_amounts: order.net_amounts,
      total_tax_money: order.total_tax_money,
      total_discount_money: order.total_discount_money,
      return_amounts: order.return_amounts,
      line_items_count: order.line_items?.length,
      first_item_money: order.line_items?.[0] ? {
        name: order.line_items[0].name,
        gross_sales_money: order.line_items[0].gross_sales_money,
        total_discount_money: order.line_items[0].total_discount_money,
        catalog_object_id: order.line_items[0].catalog_object_id,
      } : null,
      payment_processing_fee: paymentFees,
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Debug: show raw payment with fees
app.get('/api/square/debug-payment', async (req, res) => {
  try {
    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
    const r = await sqFetch(`/payments?location_id=${SQUARE_LOCATION}&begin_time=${today}T00:00:00-05:00&end_time=${today}T23:59:59-05:00&limit=2`);
    const data = await r.json();
    const payment = data.payments?.[0];
    res.json({
      total: data.payments?.length,
      first_payment: payment ? {
        id: payment.id,
        amount_money: payment.amount_money,
        processing_fee: payment.processing_fee,
        total_money: payment.total_money,
        approved_money: payment.approved_money,
      } : null
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// Debug live category pull step by step
app.get('/api/square/debug-live', async (req, res) => {
  try {
    const date = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
    
    // Step 1: Load catalog map
    const itemCatMap = {};
    let dbgOffset = 0;
    let dbgHttpStatus = 200;
    while (true) {
      const itemMapR = await sbFetch(
        `${SUPABASE_URL}/rest/v1/square_items?store_id=eq.${STORE_ID}&select=id,category_name&is_deleted=eq.false&limit=1000&offset=${dbgOffset}`,
        { headers: sbHeaders }
      );
      dbgHttpStatus = itemMapR.status;
      const batch = await itemMapR.json();
      if (!Array.isArray(batch) || batch.length === 0) break;
      batch.forEach(r => { if (r.id && r.category_name) itemCatMap[r.id] = r.category_name; });
      if (batch.length < 1000) break;
      dbgOffset += 1000;
    }

    // Step 2: Pull a small sample of orders
    const ordersR = await sqFetch('/orders/search', {
      method: 'POST',
      body: JSON.stringify({
        location_ids: [SQUARE_LOCATION],
        query: {
          filter: {
            date_time_filter: { created_at: { start_at: `${date}T06:00:00Z`, end_at: `${date}T23:59:59Z` } },
            state_filter: { states: ['COMPLETED'] }
          }
        },
        limit: 5
      })
    });
    const ordersData = await ordersR.json();
    const orders = ordersData.orders || [];

    // Step 3: Check matching for first order's items
    const firstOrder = orders[0];
    const lineItemCheck = (firstOrder?.line_items || []).slice(0,5).map(item => ({
      name: item.name,
      catalog_object_id: item.catalog_object_id,
      gross_sales: (item.gross_sales_money?.amount || 0) / 100,
      foundInCatalog: item.catalog_object_id ? !!itemCatMap[item.catalog_object_id] : false,
      category: item.catalog_object_id ? (itemCatMap[item.catalog_object_id] || 'NOT FOUND') : 'NO ID'
    }));

    res.json({
      catalogMapSize: Object.keys(itemCatMap).length,
      catalogHttpStatus: dbgHttpStatus,
      ordersReturned: orders.length,
      ordersHttpStatus: ordersR.status,
      sampleItemMapEntry: Object.entries(itemCatMap).slice(0,2),
      firstOrderLineItems: lineItemCheck
    });
  } catch(e) {
    res.status(500).json({ error: e.message, stack: e.stack?.substring(0,500) });
  }
});


// Test Square's reporting endpoints for category sales
app.get('/api/square/test-reporting', async (req, res) => {
  try {
    const today = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
    const results = {};

    // Try 1: Labor API (not relevant but tests auth)
    const r1 = await sqFetch(`/labor/shifts?location_id=${SQUARE_LOCATION}&limit=1`);
    results.laborStatus = r1.status;

    // Try 2: Square's v1 settlements (legacy but has category data)
    const r2 = await sqFetch(`/v1/${SQUARE_LOCATION}/settlements?limit=1`);
    results.settlementsStatus = r2.status;
    if (r2.ok) results.settlementsData = (await r2.json()).slice(0,1);

    // Try 3: Reporting API - business report
    const r3 = await sqFetch(`/reporting/reports/cash-drawer-current-day-report?location_id=${SQUARE_LOCATION}`);
    results.cashDrawerStatus = r3.status;

    // Try 4: Inventory count by category (different approach)
    const r4 = await sqFetch(`/inventory/counts?location_ids=${SQUARE_LOCATION}&limit=1`);
    results.inventoryStatus = r4.status;

    res.json(results);
  } catch(e) { res.status(500).json({ error: e.message }); }
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


// Monthly net sales excluding Scratch Lotto
app.get('/api/sales/monthly/net', async (req, res) => {
  try {
    const { month, year } = req.query;
    const daysInMonth = new Date(parseInt(year), parseInt(month), 0).getDate();
    const from = `${year}-${String(month).padStart(2,'0')}-01`;
    const to = `${year}-${String(month).padStart(2,'0')}-${daysInMonth}`;

    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/square_daily_sales?store_id=eq.${STORE_ID}&sale_date=gte.${from}&sale_date=lte.${to}&select=sale_date,gross_sales,categories,transaction_count,synced_at&order=sale_date.asc`, { headers: sbHeaders });
    const rows = await r.json();

    // Also check imported category sales for more accurate data
    const catR = await sbFetch(`${SUPABASE_URL}/rest/v1/square_category_sales?store_id=eq.${STORE_ID}&sale_date=gte.${from}&sale_date=lte.${to}&select=sale_date,category,gross_sales`, { headers: sbHeaders });
    const catRows = await catR.json();

    let netSales = 0, scratchTotal = 0, transactions = 0;
    const useImported = catRows.length > 0;

    if (useImported) {
      // Use imported CSV data — more accurate
      for (const row of catRows) {
        const amt = parseFloat(row.gross_sales || 0);
        if (row.category === 'Scratch Lotto') scratchTotal += amt;
        else netSales += amt;
      }
      transactions = rows.reduce((a,r) => a + (r.transaction_count||0), 0);
    } else {
      // Fall back to order sync data
      for (const row of rows) {
        const gross = parseFloat(row.gross_sales || 0);
        const cats = row.categories || {};
        const scratch = Object.entries(cats).filter(([k,v])=>(v.name||k)==='Scratch Lotto').reduce((a,[k,v])=>a+parseFloat(v.amount||0),0);
        scratchTotal += scratch;
        netSales += gross - scratch;
        transactions += row.transaction_count || 0;
      }
    }

    // Also aggregate tax, discounts, returns from order sync data
    let totalTax = 0, totalDiscounts = 0, totalReturns = 0;
    for (const row of rows) {
      totalTax += parseFloat(row.tax || 0);
      totalDiscounts += parseFloat(row.discounts || 0);
      totalReturns += parseFloat(row.returns || 0);
    }
    // Recalculate net: gross - tax - scratch
    const grossSalesMonth = parseFloat((netSales + scratchTotal).toFixed(2));
    const netSalesMonth = parseFloat((grossSalesMonth - totalTax - scratchTotal).toFixed(2));

    // Exclude today from days count — today is still in progress
    const todayET = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
    const completedDays = rows.filter(r => r.sale_date < todayET).length;

    res.json({
      month: parseInt(month), year: parseInt(year),
      netSales: netSalesMonth,
      grossSales: grossSalesMonth,
      tax: parseFloat(totalTax.toFixed(2)),
      discounts: parseFloat(totalDiscounts.toFixed(2)),
      returns: parseFloat(totalReturns.toFixed(2)),
      scratchLotto: parseFloat(scratchTotal.toFixed(2)),
      days: completedDays,
      transactions,
      useImported
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
    // At 2am ET, sync yesterday (the just-completed business day)
    const etNow = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const yesterday = new Date(etNow);
    yesterday.setDate(etNow.getDate() - 1);
    const dateStr = yesterday.toLocaleDateString('en-CA'); // YYYY-MM-DD
    console.log(`Nightly sync: syncing ${dateStr}`);
    const { default: fetch } = await import('node-fetch');
    const r = await fetch(`http://localhost:${PORT}/api/square/sync`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ start: dateStr, end: dateStr })
    });
    const result = await r.json();
    console.log('Nightly sync result:', result);
  } catch (e) {
    console.error('Nightly sync failed:', e.message);
  }
}

// Schedule nightly sync at 2am ET (6am UTC during EDT, 7am UTC during EST)
function scheduleSyncs() {
  const now = new Date();
  const nextSync = new Date();
  // Use America/New_York — calculate next 2am ET
  const etNow = new Date(now.toLocaleString('en-US', { timeZone: 'America/New_York' }));
  const etTarget = new Date(etNow);
  etTarget.setHours(2, 0, 0, 0);
  if (etTarget <= etNow) etTarget.setDate(etTarget.getDate() + 1);
  const msUntilSync = etTarget - etNow;
  console.log(`Next nightly sync in ${Math.round(msUntilSync/1000/60)} minutes (2am ET)`);
  setTimeout(() => {
    runNightlySync();
    setInterval(runNightlySync, 24 * 60 * 60 * 1000);
  }, msUntilSync);
}

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Grape & Grain running on port ${PORT}`);
  console.log(`Square: location ${SQUARE_LOCATION}`);
  scheduleSyncs();
});
