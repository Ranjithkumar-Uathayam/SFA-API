const axios = require('axios');
require('dotenv').config();

// ─────────────────────────────────────────────────────────────────────────────
// CONFIG — tune these via .env or fallback defaults
// ─────────────────────────────────────────────────────────────────────────────
const CONCURRENCY   = parseInt(process.env.SF_CONCURRENCY,    10) || 5;   // parallel requests
const BATCH_SIZE    = parseInt(process.env.SF_BATCH_SIZE,     10) || 50;  // records per bulk batch
const MAX_RETRIES   = parseInt(process.env.SF_MAX_RETRIES,    10) || 3;   // retry attempts
const RETRY_DELAY   = parseInt(process.env.SF_RETRY_DELAY_MS, 10) || 1500; // ms between retries
const REQ_TIMEOUT   = parseInt(process.env.SF_TIMEOUT_MS,     10) || 30000;

// ─────────────────────────────────────────────────────────────────────────────
// LOGGER — timestamped, consistent prefix
// ─────────────────────────────────────────────────────────────────────────────
const log = {
  info  : (...a) => console.log (`[${ts()}] ℹ️ `, ...a),
  ok    : (...a) => console.log (`[${ts()}] ✅`, ...a),
  warn  : (...a) => console.warn(`[${ts()}] ⚠️ `, ...a),
  error : (...a) => console.error(`[${ts()}] ❌`, ...a),
  divider: (label = '') => console.log(`[${ts()}] ${'─'.repeat(20)} ${label} ${'─'.repeat(20)}`),
  progress: (done, total, label = '') => {
    const pct  = total ? Math.round((done / total) * 100) : 0;
    const bar  = '█'.repeat(Math.floor(pct / 5)) + '░'.repeat(20 - Math.floor(pct / 5));
    console.log(`[${ts()}] 📊 [${bar}] ${pct}% (${done}/${total}) ${label}`);
  }
};

function ts() {
  return new Date().toISOString().replace('T', ' ').slice(0, 23);
}

// ─────────────────────────────────────────────────────────────────────────────
// AUTH
// ─────────────────────────────────────────────────────────────────────────────
let cachedToken, tokenExpiry, instanceUrl;

async function getSalesforceToken() {
  if (cachedToken && tokenExpiry > new Date()) return cachedToken;

  try {
    const res = await axios.get(process.env.SF_AUTH_URL, {
      params: {
        grant_type   : 'client_credentials',
        client_id    : process.env.SF_CLIENT_ID,
        client_secret: process.env.SF_CLIENT_SECRET
      },
      timeout: REQ_TIMEOUT
    });

    cachedToken  = res.data.access_token;
    instanceUrl  = res.data.instance_url;
    tokenExpiry  = new Date(Date.now() + 55 * 60 * 1000);

    log.ok(`Salesforce token acquired. Instance: ${instanceUrl}`);
    return cachedToken;

  } catch (err) {
    log.error('Salesforce Authentication Failed');
    log.error(`  URL    : ${process.env.SF_AUTH_URL}`);
    log.error(`  Status : ${err.response?.status}`);
    log.error(`  Body   : ${JSON.stringify(err.response?.data ?? err.message)}`);
    throw new Error(`SF Auth Failed: ${err.response?.data?.error_description ?? err.message}`);
  }
}

// ─────────────────────────────────────────────────────────────────────────────
// URL BUILDER
// ─────────────────────────────────────────────────────────────────────────────
function buildUrl(baseUrl) {
  if (!baseUrl) throw new Error('SF_API_URL_ProductMaster is not set in .env');
  if (!instanceUrl) return baseUrl;
  try {
    const url      = new URL(baseUrl);
    const instance = new URL(instanceUrl);
    url.protocol   = instance.protocol;
    url.host       = instance.host;
    return url.toString();
  } catch { return baseUrl; }
}

function buildSalesforceUrl(base, inst) {
  if (!base) return null;
  if (!inst)  return base;
  try {
    const b = new URL(base);
    const i = new URL(inst);
    b.protocol = i.protocol;
    b.host     = i.host;
    return b.toString();
  } catch { return base; }
}

// ─────────────────────────────────────────────────────────────────────────────
// VERIFY SF RESPONSE
// ─────────────────────────────────────────────────────────────────────────────
function verifySFResponse(response, label) {
  const { status, data } = response;

  if (status < 200 || status >= 300) {
    throw new Error(`[${label}] HTTP ${status}: ${JSON.stringify(data)}`);
  }

  if (data) {
    const bodyStr = JSON.stringify(data).toLowerCase();
    if (
      data.success === false || data.Success === false ||
      data.status  === 'error' || data.Status === 'error' ||
      (Array.isArray(data) && data.some(d => d.success === false)) ||
      bodyStr.includes('"errorcode"') ||
      bodyStr.includes('"errormessage"') ||
      bodyStr.includes('"exceptionmessage"')
    ) {
      throw new Error(`[${label}] SF error in body: ${JSON.stringify(data)}`);
    }
  }
  return true;
}

// ─────────────────────────────────────────────────────────────────────────────
// RETRY WRAPPER  — exponential back-off
// ─────────────────────────────────────────────────────────────────────────────
async function withRetry(fn, label, maxRetries = MAX_RETRIES) {
  let lastErr;

  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const isLast = attempt === maxRetries;
      const delay  = RETRY_DELAY * attempt; // linear back-off

      if (isLast) {
        log.error(`[${label}] All ${maxRetries} retries exhausted.`);
      } else {
        log.warn(`[${label}] Attempt ${attempt}/${maxRetries} failed — retrying in ${delay}ms…`);
        log.warn(`  Reason: ${err.message}`);
        await sleep(delay);
      }
    }
  }
  throw lastErr;
}

// ─────────────────────────────────────────────────────────────────────────────
// CONCURRENCY POOL — run tasks with max N in parallel
// ─────────────────────────────────────────────────────────────────────────────
async function runWithConcurrency(tasks, concurrency) {
  const results = [];
  let index = 0;

  async function worker() {
    while (index < tasks.length) {
      const i    = index++;
      results[i] = await tasks[i]();
    }
  }

  const workers = Array.from({ length: Math.min(concurrency, tasks.length) }, worker);
  await Promise.all(workers);
  return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// SLEEP HELPER
// ─────────────────────────────────────────────────────────────────────────────
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

// ─────────────────────────────────────────────────────────────────────────────
// UPSERT PRODUCTS  — individual records, CONCURRENCY parallel at a time
// ─────────────────────────────────────────────────────────────────────────────
async function upsertProducts(products) {
  const token = await getSalesforceToken();
  const url   = buildUrl(process.env.SF_API_URL_ProductMaster);

  const headers = {
    'Content-Type': 'application/json',
    Authorization : `Bearer ${token}`
  };

  const total   = products.length;
  const results = { success: [], failed: [] };

  log.divider('PRODUCT UPSERT START');
  log.info(`Total products   : ${total}`);
  log.info(`Concurrency      : ${CONCURRENCY}`);
  log.info(`Max retries/item : ${MAX_RETRIES}`);
  log.info(`Endpoint         : ${url}`);
  log.divider();

  // Build one task per product
  const tasks = products.map((product, idx) => async () => {
    const code = product?.Product?.ProductCode ?? 'UNKNOWN';

    try {
      const response = await withRetry(
        () => axios.post(url, product, { headers, timeout: REQ_TIMEOUT }),
        code
      );

      verifySFResponse(response, code);

      results.success.push({ code, status: response.status, data: response.data });

      const done = results.success.length + results.failed.length;
      log.ok(`[${done}/${total}] [${code}] HTTP ${response.status} — OK`);

    } catch (err) {
      const status = err.response?.status ?? 'N/A';
      const body   = err.response?.data   ?? err.message;

      results.failed.push({ code, status, error: body });

      const done = results.success.length + results.failed.length;
      log.error(`[${done}/${total}] [${code}] HTTP ${status} — FAILED`);
      log.error(`  Detail: ${JSON.stringify(body)}`);
    }

    // Progress bar every 10 records
    const done = results.success.length + results.failed.length;
    if (done % 10 === 0 || done === total) {
      log.progress(done, total, 'products');
    }
  });

  await runWithConcurrency(tasks, CONCURRENCY);

  // ── Final summary ──────────────────────────────────────────────────────────
  log.divider('PRODUCT UPSERT SUMMARY');
  log.info(`Total Sent  : ${total}`);
  log.ok  (`Success     : ${results.success.length}`);
  log.error(`Failed      : ${results.failed.length}`);

  if (results.failed.length > 0) {
    log.warn('Failed Products:');
    results.failed.forEach(f =>
      log.warn(`  • [${f.code}] HTTP ${f.status} — ${JSON.stringify(f.error)}`)
    );
  }
  log.divider();

  if (results.success.length === 0 && results.failed.length > 0) {
    throw new Error(`All ${results.failed.length} product upsert(s) failed.`);
  }

  return results;
}

// ─────────────────────────────────────────────────────────────────────────────
// UPSERT PRICE LISTS  — batched bulk upload with per-batch status tracking
// ─────────────────────────────────────────────────────────────────────────────
async function upsertPriceLists(priceLists) {
  if (!Array.isArray(priceLists) || priceLists.length === 0) {
    log.warn('upsertPriceLists: nothing to send.');
    return { message: 'No price lists to upsert.' };
  }

  const token = await getSalesforceToken();

  let url =
    process.env.SF_API_URL_PriceList ||
    process.env.SF_API_URL_ProductMaster?.replace('ProductUpsertAPI', 'PriceListUpsertAPI');

  url = buildSalesforceUrl(url, instanceUrl);

  const headers = {
    'Content-Type': 'application/json',
    Authorization : `Bearer ${token}`
  };

  const total      = priceLists.length;
  const totalBatch = Math.ceil(total / BATCH_SIZE);
  const summary    = { success: [], failed: [] };

  log.divider('PRICELIST UPSERT START');
  log.info(`Total records : ${total}`);
  log.info(`Batch size    : ${BATCH_SIZE}`);
  log.info(`Total batches : ${totalBatch}`);
  log.info(`Concurrency   : ${CONCURRENCY}`);
  log.info(`Max retries   : ${MAX_RETRIES}`);
  log.info(`Endpoint      : ${url}`);
  log.divider();

  // Slice into batches
  const batches = [];
  for (let i = 0; i < total; i += BATCH_SIZE) {
    batches.push(priceLists.slice(i, i + BATCH_SIZE));
  }

  // Build one task per batch
  const tasks = batches.map((batch, batchIdx) => async () => {
    const batchNum    = batchIdx + 1;
    const recordRange = `records ${batchIdx * BATCH_SIZE + 1}–${Math.min((batchIdx + 1) * BATCH_SIZE, total)}`;

    log.info(`Batch ${batchNum}/${totalBatch} — sending ${batch.length} records (${recordRange})`);

    try {
      const response = await withRetry(
        () => axios.post(url, batch, { headers, timeout: REQ_TIMEOUT }),
        `BATCH-${batchNum}`
      );

      verifySFResponse(response, `BATCH-${batchNum}`);

      summary.success.push({
        batch    : batchNum,
        count    : batch.length,
        status   : response.status,
        response : response.data
      });

      log.ok(`Batch ${batchNum}/${totalBatch} — HTTP ${response.status} — ${batch.length} records OK`);
      log.info(`  SF Response: ${JSON.stringify(response.data)}`);

    } catch (err) {
      const status = err.response?.status ?? 'N/A';
      const body   = err.response?.data   ?? err.message;

      // Capture which ProductCodes were in this failed batch
      const codes = batch.map(r => r.ProductCode ?? 'UNKNOWN');

      summary.failed.push({
        batch   : batchNum,
        count   : batch.length,
        status,
        error   : body,
        codes
      });

      log.error(`Batch ${batchNum}/${totalBatch} — HTTP ${status} — FAILED (${batch.length} records)`);
      log.error(`  Product Codes: ${codes.slice(0, 10).join(', ')}${codes.length > 10 ? ` … +${codes.length - 10} more` : ''}`);
      log.error(`  Detail: ${JSON.stringify(body)}`);
    }

    // Overall progress
    const done = summary.success.length + summary.failed.length;
    log.progress(done, totalBatch, 'batches');
  });

  await runWithConcurrency(tasks, CONCURRENCY);

  // ── Final summary ──────────────────────────────────────────────────────────
  const successRecords = summary.success.reduce((n, b) => n + b.count, 0);
  const failedRecords  = summary.failed.reduce ((n, b) => n + b.count, 0);

  log.divider('PRICELIST UPSERT SUMMARY');
  log.info(`Total Batches    : ${totalBatch}`);
  log.info(`Total Records    : ${total}`);
  log.ok  (`Batches OK       : ${summary.success.length}  (${successRecords} records)`);
  log.error(`Batches FAILED   : ${summary.failed.length}   (${failedRecords} records)`);

  if (summary.failed.length > 0) {
    log.warn('Failed Batches:');
    summary.failed.forEach(f => {
      log.warn(`  • Batch ${f.batch} | HTTP ${f.status} | ${f.count} records`);
      log.warn(`    Codes : ${f.codes.slice(0, 5).join(', ')}${f.codes.length > 5 ? ` +${f.codes.length - 5} more` : ''}`);
      log.warn(`    Error : ${JSON.stringify(f.error)}`);
    });
  }
  log.divider();

  if (summary.success.length === 0) {
    throw new Error(`All ${totalBatch} PriceList batches failed. Check logs.`);
  }

  return {
    totalRecords  : total,
    totalBatches  : totalBatch,
    successBatches: summary.success.length,
    failedBatches : summary.failed.length,
    successRecords,
    failedRecords,
    failedDetails : summary.failed
  };
}

// ─────────────────────────────────────────────────────────────────────────────
// UPLOAD IMAGES  — batched with per-batch status + retry
// ─────────────────────────────────────────────────────────────────────────────
async function uploadImages(images) {
  const token = await getSalesforceToken();

  let url = process.env.SF_API_URL_UploadImages;

  if (!url && process.env.SF_API_URL_ProductMaster) {
    url = process.env.SF_API_URL_ProductMaster.replace('ProductUpsertAPI', 'UploadSKUImages');
    log.info(`Derived Image Upload URL: ${url}`);
  }

  url = buildSalesforceUrl(url, instanceUrl);

  const headers = {
    'Content-Type': 'application/json',
    Authorization : `Bearer ${token}`
  };

  const total      = images.length;
  const totalBatch = Math.ceil(total / BATCH_SIZE);
  const summary    = { success: [], failed: [] };

  log.divider('IMAGE UPLOAD START');
  log.info(`Total images  : ${total}`);
  log.info(`Batch size    : ${BATCH_SIZE}`);
  log.info(`Total batches : ${totalBatch}`);
  log.info(`Concurrency   : ${CONCURRENCY}`);
  log.info(`Max retries   : ${MAX_RETRIES}`);
  log.info(`Endpoint      : ${url}`);
  log.divider();

  const batches = [];
  for (let i = 0; i < total; i += BATCH_SIZE) {
    batches.push(images.slice(i, i + BATCH_SIZE));
  }

  const tasks = batches.map((batch, batchIdx) => async () => {
    const batchNum    = batchIdx + 1;
    const recordRange = `images ${batchIdx * BATCH_SIZE + 1}–${Math.min((batchIdx + 1) * BATCH_SIZE, total)}`;

    log.info(`Batch ${batchNum}/${totalBatch} — sending ${batch.length} image(s) (${recordRange})`);

    try {
      const response = await withRetry(
        () => axios.post(url, batch, { headers, timeout: REQ_TIMEOUT }),
        `IMG-BATCH-${batchNum}`
      );

      verifySFResponse(response, `IMG-BATCH-${batchNum}`);

      summary.success.push({ batch: batchNum, count: batch.length, status: response.status, response: response.data });

      log.ok(`Batch ${batchNum}/${totalBatch} — HTTP ${response.status} — ${batch.length} image(s) OK`);
      log.info(`  SF Response: ${JSON.stringify(response.data)}`);

    } catch (err) {
      const status = err.response?.status ?? 'N/A';
      const body   = err.response?.data   ?? err.message;
      const codes  = batch.map(r => r.skuCode ?? 'UNKNOWN');

      summary.failed.push({ batch: batchNum, count: batch.length, status, error: body, codes });

      log.error(`Batch ${batchNum}/${totalBatch} — HTTP ${status} — FAILED`);
      log.error(`  SKU Codes : ${codes.slice(0, 10).join(', ')}${codes.length > 10 ? ` +${codes.length - 10} more` : ''}`);
      log.error(`  Detail    : ${JSON.stringify(body)}`);
    }

    const done = summary.success.length + summary.failed.length;
    log.progress(done, totalBatch, 'image batches');
  });

  await runWithConcurrency(tasks, CONCURRENCY);

  // ── Final summary ──────────────────────────────────────────────────────────
  const successRecords = summary.success.reduce((n, b) => n + b.count, 0);
  const failedRecords  = summary.failed.reduce ((n, b) => n + b.count, 0);

  log.divider('IMAGE UPLOAD SUMMARY');
  log.info(`Total Batches  : ${totalBatch}`);
  log.info(`Total Images   : ${total}`);
  log.ok  (`Batches OK     : ${summary.success.length}  (${successRecords} images)`);
  log.error(`Batches FAILED : ${summary.failed.length}   (${failedRecords} images)`);

  if (summary.failed.length > 0) {
    log.warn('Failed Batches:');
    summary.failed.forEach(f => {
      log.warn(`  • Batch ${f.batch} | HTTP ${f.status} | ${f.count} images`);
      log.warn(`    SKUs  : ${f.codes.slice(0, 5).join(', ')}${f.codes.length > 5 ? ` +${f.codes.length - 5} more` : ''}`);
      log.warn(`    Error : ${JSON.stringify(f.error)}`);
    });
  }
  log.divider();

  if (summary.success.length === 0) {
    throw new Error(`All ${totalBatch} image batches failed. Check logs.`);
  }

  return {
    totalImages   : total,
    totalBatches  : totalBatch,
    successBatches: summary.success.length,
    failedBatches : summary.failed.length,
    successRecords,
    failedRecords,
    failedDetails : summary.failed
  };
}

module.exports = {
  upsertProducts,
  upsertPriceLists,
  uploadImages
};