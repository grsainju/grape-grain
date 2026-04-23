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
const ANTHROPIC_KEY = process.env.ANTHROPIC_API_KEY || '';
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
      let liveCash = 0, liveCard = 0, liveScratch = 0;
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
          // Track scratch lotto separately by name pattern
          if (name.match(/^\d+\s*\$|scratch|lotto|lottery/i)) { liveScratch += amt; continue; }
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
        netSales: parseFloat((liveGross - liveTax - liveScratch).toFixed(2)),
        grossSales: parseFloat(liveGross.toFixed(2)),
        tax: parseFloat(liveTax.toFixed(2)),
        discounts: parseFloat(liveDisc.toFixed(2)),
        returns: parseFloat(liveRet.toFixed(2)),
        fees: parseFloat(totalFees.toFixed(2)),
        scratchLotto: parseFloat(liveScratch.toFixed(2)),
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

    // Collect fees per day from Payments API
    const feesByDate = {};
    try {
      let feesCursor = null;
      const feesStart = startDate + 'T06:00:00Z';
      const feesEndDate = new Date(endDate + 'T06:00:00Z');
      feesEndDate.setDate(feesEndDate.getDate() + 1);
      const feesEnd = feesEndDate.toISOString().slice(0,19) + 'Z';
      do {
        const pr = await sqFetch(`/payments?location_id=${SQUARE_LOCATION}&begin_time=${feesStart}&end_time=${feesEnd}&limit=200${feesCursor ? '&cursor=' + feesCursor : ''}`);
        const pd = await pr.json();
        for (const p of (pd.payments || [])) {
          if (!Array.isArray(p.processing_fee)) continue;
          // Get payment date in ET
          const pDate = new Date(p.created_at || p.updated_at).toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
          for (const f of p.processing_fee) {
            feesByDate[pDate] = (feesByDate[pDate] || 0) + Math.abs((f.amount_money?.amount || 0)) / 100;
          }
        }
        feesCursor = pd.cursor;
      } while (feesCursor);
      console.log(`Fees collected for ${Object.keys(feesByDate).length} dates`);
    } catch(e) { console.log('Fees collection error:', e.message); }

    // Add fees to rows
    rows.forEach(r => { r.fees = parseFloat((feesByDate[r.sale_date] || 0).toFixed(2)); });

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

    // Always use square_daily_sales for money totals — has all fields
    const r = await sbFetch(
      `${SUPABASE_URL}/rest/v1/square_daily_sales?store_id=eq.${STORE_ID}&sale_date=gte.${from}&sale_date=lte.${to}&select=sale_date,gross_sales,tax,cash_amount,card_amount,discounts,returns,fees,transaction_count,categories&order=sale_date.asc`,
      { headers: sbHeaders }
    );
    const rows = await r.json();

    // Exclude today — still in progress
    const todayET = new Date().toLocaleDateString('en-CA', { timeZone: 'America/New_York' });
    const completedRows = rows.filter(r => r.sale_date < todayET);

    // Aggregate totals
    let grossSales = 0, totalTax = 0, totalDiscounts = 0, totalReturns = 0, transactions = 0, scratchTotal = 0;
    for (const row of completedRows) {
      grossSales += parseFloat(row.gross_sales || 0);
      totalTax += parseFloat(row.tax || 0);
      totalDiscounts += parseFloat(row.discounts || 0);
      totalReturns += parseFloat(row.returns || 0);
      transactions += row.transaction_count || 0;
      // Get scratch lotto from categories JSONB
      const cats = row.categories || {};
      for (const [k, v] of Object.entries(cats)) {
        if ((v.name || k) === 'Scratch Lotto') scratchTotal += parseFloat(v.amount || 0);
      }
    }

    // Also get fees from stored fees column if available, else 0
    // Net = Gross - Tax - Returns (matches Square's net sales definition)
    const netSales = grossSales - totalTax - totalReturns;

    // Fees — read from stored fees column in square_daily_sales
    const totalFees = completedRows.reduce((a, r) => a + parseFloat(r.fees || 0), 0);

    res.json({
      month: parseInt(month), year: parseInt(year),
      netSales: parseFloat(netSales.toFixed(2)),
      grossSales: parseFloat(grossSales.toFixed(2)),
      tax: parseFloat(totalTax.toFixed(2)),
      discounts: parseFloat(totalDiscounts.toFixed(2)),
      returns: parseFloat(totalReturns.toFixed(2)),
      fees: parseFloat(totalFees.toFixed(2)),
      scratchLotto: parseFloat(scratchTotal.toFixed(2)),
      days: completedRows.length,
      transactions
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ── CATCH ALL ─────────────────────────────────────────────────────────────────
// ═══════════════════════════════════════════════════════════════
// ITEM MANAGER ENDPOINTS
// ═══════════════════════════════════════════════════════════════

// Match items to square_items by UPC — single bulk SQL via Supabase RPC
app.post('/api/items/match-square', async (req, res) => {
  try {
    // Step 1: Pull square_items UPC map (one fetch, ~3500 rows)
    const sqR = await sbFetch(`${SUPABASE_URL}/rest/v1/square_items?store_id=eq.${STORE_ID}&sku=not.is.null&is_deleted=eq.false&select=id,sku,price_cents&limit=5000`, { headers: sbHeaders });
    const sqItems = await sqR.json();
    const upcMap = {};
    sqItems.forEach(si => { if (si.sku) upcMap[si.sku.trim()] = { id: si.id, price_cents: si.price_cents }; });

    // Step 2: Pull all items with UPC (one fetch, ~2600 rows)
    const iR = await sbFetch(`${SUPABASE_URL}/rest/v1/items?store_id=eq.${STORE_ID}&upc=not.is.null&select=id,upc&limit=5000`, { headers: sbHeaders });
    const items = await iR.json();

    // Step 3: Build matched and unmatched lists
    const toUpdate = [];
    let unmatched = 0;
    const now = new Date().toISOString();
    items.forEach(item => {
      const sq = upcMap[item.upc?.trim()];
      if (sq) toUpdate.push({ id: item.id, square_variation_id: sq.id, square_price_cents: sq.price_cents, square_synced_at: now });
      else unmatched++;
    });

    // Step 4: Upsert matched rows back via items table — batch of 500 at a time
    // Use POST with on_conflict=id and merge-duplicates to do bulk UPDATE
    const batchSize = 500;
    for (let i = 0; i < toUpdate.length; i += batchSize) {
      const batch = toUpdate.slice(i, i + batchSize);
      await sbFetch(`${SUPABASE_URL}/rest/v1/items?on_conflict=id`, {
        method: 'POST',
        headers: { ...sbHeaders, 'Prefer': 'return=minimal,resolution=merge-duplicates' },
        body: JSON.stringify(batch)
      });
    }

    res.json({ success: true, matched: toUpdate.length, unmatched, squareCatalogSize: sqItems.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Get items with Square data merged
app.get('/api/items/full', async (req, res) => {
  try {
    const { category, status, search, limit = 100, offset = 0 } = req.query;
    let q = `store_id=eq.${STORE_ID}&deleted_at=is.null&limit=${limit}&offset=${offset}&order=gg_name.asc`;
    if (category && category !== 'all') q += `&category=eq.${encodeURIComponent(category)}`;
    if (status && status !== 'all') q += `&status=eq.${encodeURIComponent(status)}`;
    if (search) q += `&or=(gg_name.ilike.*${encodeURIComponent(search)}*,abs_code.ilike.*${encodeURIComponent(search)}*,upc.ilike.*${encodeURIComponent(search)}*)`;

    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/items?${q}&select=*`, { headers: { ...sbHeaders, 'Prefer': 'count=exact' } });
    const items = await r.json();
    const total = parseInt(r.headers?.get('content-range')?.split('/')[1] || 0);

    // Pull live Square inventory — paginate through ALL counts
    let inventoryMap = {};
    let invCursor = null, invPage = 0;
    do {
      const invBody = { location_ids: [SQUARE_LOCATION] };
      if (invCursor) invBody.cursor = invCursor;
      const invR = await sqFetch('/inventory/counts/batch-retrieve', {
        method: 'POST',
        body: JSON.stringify(invBody)
      });
      const invData = await invR.json();
      (invData.counts || []).forEach(cnt => {
        const id = cnt.catalog_object_id;
        const qty = parseFloat(cnt.quantity || 0);
        if (cnt.state === 'IN_STOCK' || !inventoryMap[id]) inventoryMap[id] = qty;
      });
      invCursor = invData.cursor;
      invPage++;
    } while (invCursor && invPage < 50);

    const enriched = items.map(item => ({
      ...item,
      square_inventory: item.square_variation_id ? (inventoryMap[item.square_variation_id] ?? item.square_inventory ?? null) : null
    }));

    res.json({ items: enriched, total, limit: parseInt(limit), offset: parseInt(offset) });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Update sell price in Square + Supabase
app.post('/api/items/:id/update-price', async (req, res) => {
  try {
    const { id } = req.params;
    const { sell_price } = req.body;
    if (!sell_price || isNaN(sell_price)) return res.status(400).json({ error: 'Invalid price' });

    // Get item
    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/items?id=eq.${id}&store_id=eq.${STORE_ID}&limit=1`, { headers: sbHeaders });
    const items = await r.json();
    if (!items.length) return res.status(404).json({ error: 'Item not found' });
    const item = items[0];

    const priceCents = Math.round(parseFloat(sell_price) * 100);
    const oldPrice = item.sell_price;

    // Update Square if we have a variation ID
    if (item.square_variation_id) {
      const sqR = await sqFetch(`/catalog/object/${item.square_variation_id}`);
      const sqData = await sqR.json();
      const obj = sqData.object;
      if (obj) {
        obj.item_variation_data.price_money = { amount: priceCents, currency: 'USD' };
        const updateR = await sqFetch('/catalog/object', {
          method: 'PUT',
          body: JSON.stringify({ idempotency_key: `price-${id}-${Date.now()}`, object: obj })
        });
        if (!updateR.ok) {
          const err = await updateR.json();
          return res.status(400).json({ error: 'Square update failed: ' + (err.errors?.[0]?.detail || JSON.stringify(err)) });
        }
      }
    }

    // Calculate new margin
    const cost = parseFloat(item.cost || 0);
    const newPrice = parseFloat(sell_price);
    const margin = cost > 0 && newPrice > 0 ? ((newPrice - cost/item.bpc) / newPrice) : null;

    // Update Supabase
    await sbFetch(`${SUPABASE_URL}/rest/v1/items?id=eq.${id}`, {
      method: 'PATCH', headers: sbHeaders,
      body: JSON.stringify({ sell_price: newPrice, square_price_cents: priceCents, margin_pct: margin, updated_at: new Date().toISOString() })
    });

    // Log price change if changed
    if (oldPrice && Math.abs(oldPrice - newPrice) > 0.005) {
      await sbFetch(`${SUPABASE_URL}/rest/v1/price_history`, {
        method: 'POST', headers: sbHeaders,
        body: JSON.stringify({ store_id: STORE_ID, item_id: parseInt(id), abs_code: item.abs_code, gg_name: item.gg_name, field_changed: 'sell_price', old_value: String(oldPrice), new_value: String(newPrice), change_pct: oldPrice ? ((newPrice - oldPrice) / oldPrice * 100).toFixed(2) : null })
      });
    }

    res.json({ success: true, old_price: oldPrice, new_price: newPrice, square_updated: !!item.square_variation_id });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Update cost/margin in Supabase (ABS cost, not sell price)
app.post('/api/items/:id/update-cost', async (req, res) => {
  try {
    const { id } = req.params;
    const { cost, low_discount, high_discount, margin_target_pct } = req.body;

    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/items?id=eq.${id}&store_id=eq.${STORE_ID}&limit=1`, { headers: sbHeaders });
    const items = await r.json();
    if (!items.length) return res.status(404).json({ error: 'Item not found' });
    const item = items[0];

    const newCost = parseFloat(cost || item.cost || 0);
    const bpc = item.bpc || 1;
    const sellPrice = parseFloat(item.sell_price || 0);
    const costPerUnit = newCost > 0 && bpc > 0 ? newCost / bpc : null;
    const margin = sellPrice > 0 && costPerUnit != null ? ((sellPrice - costPerUnit) / sellPrice) : null;
    const oldCost = item.cost;

    const updateData = {
      cost: newCost || null,
      cost_per_unit: costPerUnit,
      margin_pct: margin,
      updated_at: new Date().toISOString()
    };
    if (low_discount !== undefined)   updateData.low_discount    = parseFloat(low_discount)   || null;
    if (high_discount !== undefined)  updateData.high_discount   = parseFloat(high_discount)  || null;
    if (margin_target_pct !== undefined) updateData.margin_target_pct = parseFloat(margin_target_pct) || null;

    await sbFetch(`${SUPABASE_URL}/rest/v1/items?id=eq.${id}`, {
      method: 'PATCH', headers: sbHeaders, body: JSON.stringify(updateData)
    });

    // Log cost change
    if (oldCost && Math.abs(oldCost - newCost) > 0.005) {
      await sbFetch(`${SUPABASE_URL}/rest/v1/price_history`, {
        method: 'POST', headers: sbHeaders,
        body: JSON.stringify({ store_id: STORE_ID, item_id: parseInt(id), abs_code: item.abs_code, gg_name: item.gg_name, field_changed: 'cost', old_value: String(oldCost), new_value: String(newCost), change_pct: oldCost ? ((newCost - oldCost) / oldCost * 100).toFixed(2) : null })
      });
    }

    res.json({ success: true, cost: newCost, cost_per_unit: costPerUnit, margin_pct: margin });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Push inventory update to Square
app.post('/api/items/:id/update-inventory', async (req, res) => {
  try {
    const { id } = req.params;
    const { quantity, reason = 'RECOUNT' } = req.body;
    if (quantity === undefined || isNaN(quantity)) return res.status(400).json({ error: 'Invalid quantity' });

    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/items?id=eq.${id}&store_id=eq.${STORE_ID}&limit=1`, { headers: sbHeaders });
    const items = await r.json();
    if (!items.length) return res.status(404).json({ error: 'Item not found' });
    const item = items[0];

    if (!item.square_variation_id) return res.status(400).json({ error: 'Item not linked to Square — run catalog sync first' });

    // Push to Square inventory
    const invR = await sqFetch('/inventory/changes/batch-create', {
      method: 'POST',
      body: JSON.stringify({
        idempotency_key: `inv-${id}-${Date.now()}`,
        changes: [{
          type: 'PHYSICAL_COUNT',
          physical_count: {
            catalog_object_id: item.square_variation_id,
            location_id: SQUARE_LOCATION,
            quantity: String(parseFloat(quantity)),
            state: 'IN_STOCK',
            occurred_at: new Date().toISOString()
          }
        }]
      })
    });

    if (!invR.ok) {
      const err = await invR.json();
      return res.status(400).json({ error: 'Square inventory update failed: ' + (err.errors?.[0]?.detail || JSON.stringify(err)) });
    }

    // Update Supabase
    await sbFetch(`${SUPABASE_URL}/rest/v1/items?id=eq.${id}`, {
      method: 'PATCH', headers: sbHeaders,
      body: JSON.stringify({ square_inventory: parseFloat(quantity), inventory: parseFloat(quantity), updated_at: new Date().toISOString() })
    });

    res.json({ success: true, quantity: parseFloat(quantity), square_updated: true });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Bulk inventory sync FROM Square (read current inventory for all matched items)
app.post('/api/items/sync-inventory', async (req, res) => {
  try {
    // Get all items with square_variation_id
    const r = await sbFetch(`${SUPABASE_URL}/rest/v1/items?store_id=eq.${STORE_ID}&square_variation_id=not.is.null&select=id,square_variation_id&limit=2000`, { headers: sbHeaders });
    const items = await r.json();

    // Fetch all inventory counts from Square with pagination
    const now = new Date().toISOString();
    let updated = 0;

    // Build full var ID list
    const allVarIds = items.map(it => it.square_variation_id);

    // Fetch ALL counts from Square using cursor pagination (no chunking needed)
    const invMap = {};
    let cursor = null;
    let invPage = 0;
    do {
      const body = { location_ids: [SQUARE_LOCATION] };
      if (cursor) body.cursor = cursor;
      const invR = await sqFetch('/inventory/counts/batch-retrieve', {
        method: 'POST',
        body: JSON.stringify(body)
      });
      const invData = await invR.json();
      const counts = invData.counts || [];
      console.log(`[SYNC] Page ${invPage}: got ${counts.length} counts`);
      for (const count of counts) {
        const id = count.catalog_object_id;
        const qty = parseFloat(count.quantity || 0);
        if (count.state === 'IN_STOCK' || !invMap[id]) {
          invMap[id] = qty;
        }
      }
      cursor = invData.cursor;
      invPage++;
    } while (cursor && invPage < 50);

    console.log(`[SYNC] Total counts fetched: ${Object.keys(invMap).length}`);

    // Update each matched item in Supabase
    const chunkSize = 100;
    for (let i = 0; i < items.length; i += chunkSize) {
      const chunk = items.slice(i, i + chunkSize);
      const toUpdate = chunk.filter(it => invMap[it.square_variation_id] !== undefined);
      for (const item of toUpdate) {
        const sqCount = invMap[item.square_variation_id];
        await sbFetch(`${SUPABASE_URL}/rest/v1/items?id=eq.${item.id}`, {
          method: 'PATCH', headers: sbHeaders,
          body: JSON.stringify({ square_inventory: sqCount, inventory: sqCount, square_synced_at: now })
        });
        updated++;
      }
    }
    res.json({ success: true, updated, total: items.length, squareCounts: Object.keys(invMap).length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════
// ABS NEWSLETTER — apply approved updates (parsing done client-side via Claude API)
// ═══════════════════════════════════════════════════════════════

// Apply approved newsletter updates to items
app.post('/api/newsletter/apply', async (req, res) => {
  try {
    const { updates } = req.body;
    if (!updates?.length) return res.status(400).json({ error: 'No updates provided' });

    let applied = 0;
    const now = new Date().toISOString();
    for (const u of updates) {
      const patch = { updated_at: now };
      if (u.type === 'discount') {
        patch.low_discount  = u.discount;
        patch.high_discount = u.discount;
        if (u.disc_from) patch.disc_from = u.disc_from;
        if (u.disc_to)   patch.disc_to   = u.disc_to;
      } else if (u.type === 'price_change') {
        const oldCost = u.old_cost;
        patch.cost = u.new_cost;
        const itemR = await sbFetch(`${SUPABASE_URL}/rest/v1/items?id=eq.${u.id}&select=bpc,sell_price`, { headers: sbHeaders });
        const item = (await itemR.json())[0];
        if (item?.bpc && item?.sell_price) {
          patch.cost_per_unit = u.new_cost / item.bpc;
          patch.margin_pct    = (item.sell_price - patch.cost_per_unit) / item.sell_price;
        }
        // Log price history
        if (oldCost && Math.abs(oldCost - u.new_cost) > 0.005) {
          await sbFetch(`${SUPABASE_URL}/rest/v1/price_history`, {
            method: 'POST', headers: sbHeaders,
            body: JSON.stringify({
              store_id: STORE_ID, item_id: parseInt(u.id),
              abs_code: u.abs_code, gg_name: u.gg_name,
              field_changed: 'cost', old_value: String(oldCost), new_value: String(u.new_cost),
              change_pct: oldCost ? ((u.new_cost - oldCost) / oldCost * 100).toFixed(2) : null
            })
          });
        }
      }
      await sbFetch(`${SUPABASE_URL}/rest/v1/items?id=eq.${u.id}`, {
        method: 'PATCH', headers: sbHeaders, body: JSON.stringify(patch)
      });
      applied++;
    }
    res.json({ success: true, applied });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Get our items for cross-referencing (used by newsletter UI)
app.get('/api/newsletter/items-map', async (req, res) => {
  try {
    const r = await sbFetch(
      `${SUPABASE_URL}/rest/v1/items?store_id=eq.${STORE_ID}&deleted_at=is.null&select=id,abs_code,gg_name,cost,low_discount,high_discount,category&limit=5000`,
      { headers: sbHeaders }
    );
    const items = await r.json();
    // Return as map keyed by abs_code
    const map = {};
    items.forEach(i => { if (i.abs_code) map[i.abs_code] = i; });
    res.json(map);
  } catch(e) { res.status(500).json({ error: e.message }); }
});


// ═══════════════════════════════════════════════════════════════
// CLAUDE API PROXY — keeps API key server-side
// ═══════════════════════════════════════════════════════════════
app.post('/api/claude/proxy', async (req, res) => {
  try {
    if (!ANTHROPIC_KEY) return res.status(500).json({ error: 'ANTHROPIC_API_KEY not configured on server' });
    const { model, max_tokens, messages } = req.body;
    const r = await sbFetch('https://api.anthropic.com/v1/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': ANTHROPIC_KEY,
        'anthropic-version': '2023-06-01'
      },
      body: JSON.stringify({ model, max_tokens, messages })
    });
    if (!r.ok) {
      const err = await r.json();
      return res.status(r.status).json(err);
    }
    const data = await r.json();
    res.json(data);
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Newsletter PDF parse — pure pdftotext + regex, no Claude API needed
app.post('/api/newsletter/parse', async (req, res) => {
  const { pdfBase64, section } = req.body;
  if (!pdfBase64 || !section) return res.status(400).json({ error: 'Missing pdfBase64 or section' });

  const { execSync } = require('child_process');
  const fsSync = require('fs');
  const os = require('os');
  const pathMod = require('path');
  const tmpPdf = pathMod.join(os.tmpdir(), `nl_${Date.now()}.pdf`);

  try {
    fsSync.writeFileSync(tmpPdf, Buffer.from(pdfBase64, 'base64'));

    // Page ranges per section
    const ranges = {
      beerDiscounts: [7,  14],
      wineDiscounts: [16, 27],
      qtyDiscounts:  [28, 80],
      priceChanges:  [132, 150]
    };
    const [from, to] = ranges[section] || [1, 10];

    const text = execSync(
      `pdftotext -layout -f ${from} -l ${to} "${tmpPdf}" -`,
      { encoding: 'utf8', timeout: 30000, maxBuffer: 10 * 1024 * 1024 }
    );

    let items = [];

    if (section === 'beerDiscounts') {
      // Pattern: code  description  size  bpc  date_from  date_to  list_price  discount  sale_price  supplier
      const pat = /^(\d+)\s{2,}(.+?)\s{2,}[\d.]+Z\s+(\d+)\s+(\d{2}\/\d{2})\s+(\d{2}\/\d{2})\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+(.+)$/gm;
      let m;
      while ((m = pat.exec(text)) !== null) {
        items.push({
          abs_code: m[1], description: m[2].trim(), bpc: parseInt(m[3]),
          disc_from: '2026-' + m[4].replace('/','-'),
          disc_to:   '2026-' + m[5].replace('/','-'),
          list_price: parseFloat(m[6]), discount: parseFloat(m[7]),
          sale_price: parseFloat(m[8]), supplier: m[9].trim()
        });
      }
    }

    else if (section === 'wineDiscounts') {
      // Pattern: code  description  size  bpc  discount  list_price  sale_price  vendor
      const pat = /^(\d{4,})\s{2,}(.+?)\s{2,}\d+ML\s+(\d+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s*(.*)$/gm;
      let m;
      while ((m = pat.exec(text)) !== null) {
        const disc = parseFloat(m[4]);
        const list = parseFloat(m[5]);
        const sale = parseFloat(m[6]);
        if (disc > 0 && list > 0 && sale > 0) {
          items.push({
            abs_code: m[1], description: m[2].trim(), bpc: parseInt(m[3]),
            disc_from: '2026-04-01', disc_to: '2026-04-30',
            discount: disc, list_price: list, sale_price: sale,
            supplier: m[7].trim()
          });
        }
      }
    }

    else if (section === 'qtyDiscounts') {
      const lines = text.split('\n');
      const dataPat = /^\s*(\d{4,})?\s{2,}(.{8,}?)\s{3,}(\d+ML|[\d.]+Z|[\d.]+L)\s{2,}(.+?)\s{2,}(\d+)\s+\$([\d.]+)((?:\s+\$?[\d.]+)+)\s*$/;
      const codePat = /^\s*(\d{5,})\s*$/;
      let pendingCode = null, pendingItem = null, currentGroup = '';

      for (const line of lines) {
        const gm = line.match(/Price Category:\s*(.+)/);
        if (gm) { currentGroup = gm[1].trim(); continue; }
        const cm = line.match(codePat);
        if (cm) {
          pendingCode = cm[1];
          if (pendingItem) { pendingItem.abs_code = pendingCode; items.push(pendingItem); pendingItem = null; pendingCode = null; }
          continue;
        }
        const dm = line.match(dataPat);
        if (dm) {
          const tierVals = (dm[7].match(/[\d.]+/g) || []).map(Number).filter(v => v > 0);
          const tierKeys = ['1+','3+','5+','10+','15+'];
          const tiers = {};
          tierVals.forEach((v,i) => { if (i < tierKeys.length) tiers[tierKeys[i]] = v; });
          const item = {
            abs_code: dm[1] || pendingCode || '',
            description: dm[2].trim(), size: dm[3].trim(),
            vendor: dm[4].trim(), bpc: parseInt(dm[5]),
            unit_cost: parseFloat(dm[6]), qty_tiers: tiers,
            group: currentGroup, category: 'Wine',
            disc_from: '2026-04-01', disc_to: '2026-04-30'
          };
          if (!item.abs_code) pendingItem = item;
          else { items.push(item); pendingCode = null; }
        }
      }
      if (pendingItem && pendingItem.abs_code) items.push(pendingItem);
    }

    else if (section === 'priceChanges') {
      const pat = /^(\d{4,})\s{2,}(.+?)\s{2,}(ST|SB|LQ|NK|NA)\s+([-\d.]+)\s+([\d.]+)\s+([\d.]+)\s*([\d.]*)\s*(.*)$/gm;
      let m;
      while ((m = pat.exec(text)) !== null) {
        const change = parseFloat(m[4]);
        items.push({
          abs_code: m[1], description: m[2].trim(), tag: m[3],
          price_change: change, new_case_price: parseFloat(m[5]),
          new_bottle_price: parseFloat(m[6]),
          direction: change > 0 ? 'UP' : 'DOWN',
          supplier: m[8]?.trim() || ''
        });
      }
    }

    res.json({ success: true, section, items, count: items.length });

  } catch(e) {
    console.error('Newsletter parse error:', e.message);
    res.status(500).json({ error: e.message });
  } finally {
    try { fsSync.unlinkSync(tmpPdf); } catch(e) {}
  }
});

;


// ═══════════════════════════════════════════════════════════════
// ORDER BUILDER — pull item-level sales from Square Orders API
// ═══════════════════════════════════════════════════════════════

app.get('/api/order-builder/sales', async (req, res) => {
  try {
    const tz = 'America/New_York';
    const today = new Date().toLocaleDateString('en-CA', { timeZone: tz });

    // Date ranges: last 7 days and last 28 days
    const d7  = new Date(); d7.setDate(d7.getDate() - 7);
    const d28 = new Date(); d28.setDate(d28.getDate() - 28);
    const start7  = d7.toLocaleDateString('en-CA',  { timeZone: tz });
    const start28 = d28.toLocaleDateString('en-CA', { timeZone: tz });

    // Pull all items with square_variation_id for matching
    const itemsR = await sbFetch(
      `${SUPABASE_URL}/rest/v1/items?store_id=eq.${STORE_ID}&status=eq.Active&deleted_at=is.null` +
      `&select=id,abs_code,gg_name,category,bpc,sell_size,splitted,made_from_abs,cost,low_discount,high_discount,square_variation_id,square_inventory,inventory&limit=5000`,
      { headers: sbHeaders }
    );
    const allItems = await itemsR.json();

    // Build variation_id → item map
    const varMap = {};
    allItems.forEach(i => { if (i.square_variation_id) varMap[i.square_variation_id] = i; });

    // Use Supabase inventory field (kept current by Item Manager sync)
    // Run "Sync Inventory" in Item Manager before generating orders for fresh counts

    // Fetch 12-month monthly sales summary for smart order calculation
    const cutoffMonth = (() => { const d = new Date(); d.setMonth(d.getMonth()-12); return d.toISOString().slice(0,7); })();
    const monthly12R = await sbFetch(
      `${SUPABASE_URL}/rest/v1/monthly_item_sales?store_id=eq.${STORE_ID}&month=gte.${cutoffMonth}&select=item_name,month,qty_sold&limit=20000`,
      { headers: sbHeaders }
    );
    const monthly12Rows = await monthly12R.json();

    // Build name → {total, months set} for avg calculation
    const monthlySummary = {};
    monthly12Rows.forEach(row => {
      if (!monthlySummary[row.item_name]) monthlySummary[row.item_name] = { total: 0, months: new Set() };
      monthlySummary[row.item_name].total += parseFloat(row.qty_sold || 0);
      monthlySummary[row.item_name].months.add(row.month?.slice(0,7));
    });
    console.log(`[ORDER] Monthly history loaded: ${Object.keys(monthlySummary).length} items`);

    // Pull orders for 28 days (covers both ranges)
    const startTime = `${start28}T05:00:00Z`;
    const endTime   = `${today}T05:00:00Z`;
    const endTime7  = `${start7}T05:00:00Z`;

    // Aggregate sales by variation_id
    const sales28 = {}; // variation_id → { qty, revenue }
    const sales7  = {};

    let cursor = null;
    let pages = 0;
    do {
      const body = {
        location_ids: [SQUARE_LOCATION],
        query: {
          filter: {
            date_time_filter: { created_at: { start_at: startTime, end_at: new Date().toISOString().slice(0,19)+'Z' } },
            state_filter: { states: ['COMPLETED'] }
          }
        },
        limit: 500
      };
      if (cursor) body.cursor = cursor;

      const r = await sqFetch('/orders/search', { method: 'POST', body: JSON.stringify(body) });
      const data = await r.json();
      if (data.errors) throw new Error(data.errors[0]?.detail);

      for (const order of (data.orders || [])) {
        const orderDate = order.created_at?.slice(0,10) || '';
        const isLast7 = orderDate >= start7;

        for (const item of (order.line_items || [])) {
          const varId = item.catalog_object_id;
          if (!varId) continue;
          const qty = parseFloat(item.quantity || 0);
          const rev = (item.gross_sales_money?.amount || 0) / 100;

          if (!sales28[varId]) sales28[varId] = { qty: 0, revenue: 0 };
          sales28[varId].qty     += qty;
          sales28[varId].revenue += rev;

          if (isLast7) {
            if (!sales7[varId]) sales7[varId] = { qty: 0, revenue: 0 };
            sales7[varId].qty     += qty;
            sales7[varId].revenue += rev;
          }
        }
      }

      cursor = data.cursor;
      pages++;
    } while (cursor && pages < 20);

    // Build result: start with ALL active Beer/Wine items, merge in sales
    const itemSales = {};
    const targetCats = new Set(['Beer','Wine','Single Beer','Custom Beer','Cigar','Tobacco']);

    allItems.forEach(item => {
      if (!targetCats.has(item.category)) return;
      const varId = item.square_variation_id;
      const s28 = varId ? (sales28[varId] || { qty: 0, revenue: 0 }) : { qty: 0, revenue: 0 };
      const s7  = varId ? (sales7[varId]  || { qty: 0, revenue: 0 }) : { qty: 0, revenue: 0 };

      itemSales[item.abs_code] = {
        item,
        sold_7d:  s7.qty,
        sold_28d: s28.qty,
        rev_7d:   s7.revenue,
        rev_28d:  s28.revenue
      };
    });

    // Step 2: roll custom pack sales into parent items
    // "Made From X" items: their unit sales should be added to parent's case count
    allItems.forEach(item => {
      if (!item.splitted || !item.splitted.startsWith('Made From') || !item.made_from_abs) return;
      const parentCode = item.made_from_abs;
      const varId = item.square_variation_id;
      if (!varId) return;

      const s28 = sales28[varId] || { qty: 0 };
      const s7  = sales7[varId]  || { qty: 0 };

      // Get sell_size (units per custom pack, e.g. 6 for a 6-pack)
      const sellSize = parseInt(item.sell_size || 1);
      // Get parent BPC
      const parentItem = allItems.find(i => i.abs_code === parentCode);
      const parentBpc  = parentItem?.bpc || 1;

      // Convert units sold to cases: (units_sold × sell_size) / parent_bpc
      const units28 = s28.qty * sellSize;
      const units7  = s7.qty  * sellSize;

      if (!itemSales[parentCode]) {
        itemSales[parentCode] = {
          item: parentItem || { abs_code: parentCode, gg_name: parentCode, category: 'Beer', bpc: parentBpc },
          sold_7d: 0, sold_28d: 0, rev_7d: 0, rev_28d: 0
        };
      }
      // Add rollup units (will convert to cases later)
      if (!itemSales[parentCode].rollup_units_7d)  itemSales[parentCode].rollup_units_7d  = 0;
      if (!itemSales[parentCode].rollup_units_28d) itemSales[parentCode].rollup_units_28d = 0;
      itemSales[parentCode].rollup_units_7d  += units7;
      itemSales[parentCode].rollup_units_28d += units28;
    });

    // Step 3: build final rows with order suggestion
    const rows = Object.values(itemSales).map(entry => {
      const { item, sold_7d, sold_28d } = entry;
      const bpc      = item.bpc || 1;
      const sellSize = parseInt(item.sell_size || 1);

      // inventory field = number of sell-size packs on hand
      // e.g. inv=4, sell_size=12 → 4 twelve-packs → 48 bottles → 2 cases of 24
      const invPacks   = parseFloat(item.inventory ?? item.square_inventory ?? 0);
      const invBottles = invPacks * sellSize;   // total individual bottles/cans
      const inv_cases  = invBottles / bpc;      // cases

      // Square sales are in sell-size units — convert to cases
      // e.g. sold 8 six-packs → 8×6=48 bottles → 2 cases of 24
      const cases7d  = (sold_7d  * sellSize) / bpc;
      const cases28d = (sold_28d * sellSize) / bpc;

      // Rollup from custom packs (already in bottles) → cases
      const rollup7d  = ((entry.rollup_units_7d  || 0) / bpc);
      const rollup28d = ((entry.rollup_units_28d || 0) / bpc);

      const total7d    = cases7d  + rollup7d;
      const total28d   = cases28d + rollup28d;
      const avg_weekly = total28d / 4;

      // Monthly history for this item (matched by name)
      const monthlyData = monthlySummary[item.gg_name] || { total: 0, months: new Set() };
      const total_12mo   = monthlyData.total;
      const months_count = monthlyData.months.size;
      const avg_monthly  = parseFloat((total_12mo / Math.max(months_count, 1)).toFixed(2));
      const avg_monthly_cases = avg_monthly / bpc;
      const has_history  = total_12mo > 0;

      // Smart order logic based on velocity
      // velocity = avg cases per month
      let suggested = 0;
      if (!has_history) {
        // No 12-month history — use recent 28-day sales only if selling
        if (avg_weekly > 0) suggested = Math.max(0, Math.ceil(avg_weekly * 2 - inv_cases));
      } else if (avg_monthly_cases >= 1) {
        // HIGH velocity (≥1 case/month): maintain 2-week buffer = 0.5 cases
        const target = avg_monthly_cases * 2; // 2 months stock
        suggested = Math.max(0, Math.ceil(target - inv_cases));
      } else if (avg_monthly_cases >= 0.25) {
        // MED velocity (0.25-1 case/month): keep at least 1 case
        suggested = inv_cases < 1 ? Math.ceil(1 - inv_cases) : 0;
      } else {
        // LOW velocity (<0.25 case/month, ~3 units/month for 12-pack): only order if out
        suggested = inv_cases <= 0 ? 1 : 0;
      }

      return {
        abs_code:       item.abs_code,
        gg_name:        item.gg_name,
        category:       item.category,
        bpc:            bpc,
        sell_size:      sellSize,
        cost:           item.cost,
        low_disc:       item.low_discount,
        high_disc:      item.high_discount,
        inv_packs:      invPacks,
        inv_bottles:    parseFloat(invBottles.toFixed(1)),
        inv_cases:      parseFloat(inv_cases.toFixed(2)),
        sold_7d:        parseFloat(total7d.toFixed(2)),
        sold_28d:       parseFloat(total28d.toFixed(2)),
        avg_weekly:     parseFloat(avg_weekly.toFixed(2)),
        avg_monthly:    avg_monthly,
        avg_monthly_cs: parseFloat(avg_monthly_cases.toFixed(2)),
        total_12mo:     total_12mo,
        months_count:   months_count,
        velocity:       !has_history ? 'NEW' : avg_monthly_cases >= 1 ? 'HIGH' : avg_monthly_cases >= 0.25 ? 'MED' : 'LOW',
        suggested:      suggested,
        has_rollup:     !!(entry.rollup_units_28d)
      };
    });

    // Sort: Beer first, then Wine, alphabetically within each
    rows.sort((a,b) => {
      if (a.category !== b.category) return a.category < b.category ? -1 : 1;
      return (a.gg_name||'').localeCompare(b.gg_name||'');
    });

    res.json({
      success: true,
      rows,
      meta: { start28, start7, today, pages, totalItems: allItems.length, matchedItems: Object.keys(varMap).length }
    });

  } catch(e) {
    console.error('Order builder error:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// Debug: test Square inventory for a specific item
app.get('/api/debug/inventory', async (req, res) => {
  try {
    // Get Andre Peach Mimosa from items
    const itemR = await sbFetch(
      `${SUPABASE_URL}/rest/v1/items?store_id=eq.${STORE_ID}&gg_name=ilike.*Andre+Peach+Mimosa*&select=id,gg_name,square_variation_id,inventory&limit=3`,
      { headers: sbHeaders }
    );
    const items = await itemR.json();
    
    if (!items.length || !items[0].square_variation_id) {
      return res.json({ error: 'Item not found or not matched to Square', items });
    }
    
    const varId = items[0].square_variation_id;
    
    // Call Square inventory API using POST batch endpoint
    const invR = await sqFetch('/inventory/counts/batch-retrieve', {
      method: 'POST',
      body: JSON.stringify({
        catalog_object_ids: [varId],
        location_ids: [SQUARE_LOCATION]
      })
    });
    const invData = await invR.json();
    
    res.json({
      item: items[0],
      squareResponse: invData,
      counts: invData.counts || []
    });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// ═══════════════════════════════════════════════════════════════
// MONTHLY SALES HISTORY
// ═══════════════════════════════════════════════════════════════

// Import monthly sales from xlsx data (sent as JSON from browser)
app.post('/api/monthly-sales/import', async (req, res) => {
  try {
    const { records } = req.body; // [{item_name, month, qty}]
    if (!records?.length) return res.status(400).json({ error: 'No records' });

    const rows = records.map(r => ({
      store_id: STORE_ID,
      item_name: r.item_name,
      month: r.month,
      qty_sold: parseFloat(r.qty) || 0
    }));

    // Upsert in batches of 500
    const batchSize = 500;
    for (let i = 0; i < rows.length; i += batchSize) {
      const batch = rows.slice(i, i + batchSize);
      await sbFetch(`${SUPABASE_URL}/rest/v1/monthly_item_sales?on_conflict=store_id,item_name,month`, {
        method: 'POST',
        headers: { ...sbHeaders, 'Prefer': 'return=minimal,resolution=merge-duplicates' },
        body: JSON.stringify(batch)
      });
    }
    res.json({ success: true, imported: rows.length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});

// Get monthly sales summary per item for order builder
app.get('/api/monthly-sales/summary', async (req, res) => {
  try {
    // Aggregate: total and avg per item over last 12 months
    const cutoff = new Date();
    cutoff.setMonth(cutoff.getMonth() - 12);
    const cutoffMonth = cutoff.toISOString().slice(0,7); // 'YYYY-MM'

    const r = await sbFetch(
      `${SUPABASE_URL}/rest/v1/monthly_item_sales?store_id=eq.${STORE_ID}&month=gte.${cutoffMonth}&select=item_name,month,qty_sold&limit=20000`,
      { headers: sbHeaders }
    );
    const rows = await r.json();

    // Aggregate by item_name
    const itemMap = {};
    rows.forEach(row => {
      if (!itemMap[row.item_name]) itemMap[row.item_name] = { total: 0, months: new Set() };
      itemMap[row.item_name].total += parseFloat(row.qty_sold || 0);
      itemMap[row.item_name].months.add(row.month);
    });

    const summary = {};
    Object.entries(itemMap).forEach(([name, data]) => {
      summary[name] = {
        total_12mo: parseFloat(data.total.toFixed(2)),
        months_with_sales: data.months.size,
        avg_monthly: parseFloat((data.total / 12).toFixed(2))
      };
    });

    res.json({ success: true, summary, itemCount: Object.keys(summary).length });
  } catch(e) { res.status(500).json({ error: e.message }); }
});


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
