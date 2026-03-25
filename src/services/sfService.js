const axios = require('axios');
require('dotenv').config();

// ─────────────────────────────────────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────────────────────────────────────
const CONCURRENCY    = parseInt(process.env.SF_CONCURRENCY,         10) || 5;
const BATCH_SIZE     = parseInt(process.env.SF_BATCH_SIZE,          10) || 50;
const MAX_RETRIES    = parseInt(process.env.SF_MAX_RETRIES,         10) || 3;
const RETRY_DELAY    = parseInt(process.env.SF_RETRY_DELAY_MS,      10) || 1500;
const REQ_TIMEOUT    = parseInt(process.env.SF_TIMEOUT_MS,          10) || 30000;

// Price list batches must stay small to avoid Apex SOQL governor limit (101/tx).
// Each record costs ~2 SOQLs inside PriceListUpsertAPI.createPriceList, so
// a batch of 25 → ~50 queries — safely under the cap.
// Override with SF_BATCH_SIZE_PRICELIST in .env if needed.
const PL_BATCH_SIZE  = parseInt(process.env.SF_BATCH_SIZE_PRICELIST, 10) || 25;

// Optional pause between price-list batches (ms). Helps if the org is close
// to async/CPU limits. Set SF_PL_BATCH_DELAY_MS=0 in .env to disable.
const PL_BATCH_DELAY = parseInt(process.env.SF_PL_BATCH_DELAY_MS,   10) || 300;

// ─────────────────────────────────────────────────────────────────────────────
// LOGGER
// ─────────────────────────────────────────────────────────────────────────────
function ts() { return new Date().toISOString().replace('T', ' ').slice(0, 23); }

const log = {
    info    : (...a) => console.log (`[${ts()}] ℹ️ `, ...a),
    ok      : (...a) => console.log (`[${ts()}] ✅`, ...a),
    warn    : (...a) => console.warn(`[${ts()}] ⚠️ `, ...a),
    error   : (...a) => console.error(`[${ts()}] ❌`, ...a),
    divider : (label = '') => console.log(`[${ts()}] ${'─'.repeat(20)} ${label} ${'─'.repeat(20)}`),
    progress: (done, total, label = '') => {
        const pct = total ? Math.round((done / total) * 100) : 0;
        const bar = '█'.repeat(Math.floor(pct / 5)) + '░'.repeat(20 - Math.floor(pct / 5));
        console.log(`[${ts()}] 📊 [${bar}] ${pct}% (${done}/${total}) ${label}`);
    }
};

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

        cachedToken = res.data.access_token;
        instanceUrl = res.data.instance_url;
        tokenExpiry = new Date(Date.now() + 55 * 60 * 1000);

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
// URL BUILDERS
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
    if (!inst) return base;
    try {
        const b = new URL(base);
        const i = new URL(inst);
        b.protocol = i.protocol;
        b.host     = i.host;
        return b.toString();
    } catch { return base; }
}

// ─────────────────────────────────────────────────────────────────────────────
// RESPONSE VERIFIER
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
// RETRY WRAPPER
// ─────────────────────────────────────────────────────────────────────────────
async function withRetry(fn, label, maxRetries = MAX_RETRIES) {
    let lastErr;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            return await fn();
        } catch (err) {
            lastErr = err;
            if (attempt === maxRetries) {
                log.error(`[${label}] All ${maxRetries} retries exhausted.`);
            } else {
                const delay = RETRY_DELAY * attempt;
                log.warn(`[${label}] Attempt ${attempt}/${maxRetries} failed — retrying in ${delay}ms…`);
                log.warn(`  Reason: ${err.message}`);
                await sleep(delay);
            }
        }
    }
    throw lastErr;
}

// ─────────────────────────────────────────────────────────────────────────────
// CONCURRENCY POOL
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

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

// ─────────────────────────────────────────────────────────────────────────────
// PRODUCTS  — one request per product, CONCURRENCY in parallel
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

        const done = results.success.length + results.failed.length;
        if (done % 10 === 0 || done === total) log.progress(done, total, 'products');
    });

    await runWithConcurrency(tasks, CONCURRENCY);

    log.divider('PRODUCT UPSERT SUMMARY');
    log.info(`Total Sent : ${total}`);
    log.ok  (`Success    : ${results.success.length}`);
    log.error(`Failed     : ${results.failed.length}`);
    if (results.failed.length > 0) {
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
// PRICE LISTS
//
// Apex governor limit: 101 SOQL queries per transaction.
// PriceListUpsertAPI.createPriceList fires ~2 SOQLs per record inside a loop,
// so we cap each batch at PL_BATCH_SIZE (default 25) to stay well under 101.
//
// Batches run SEQUENTIALLY (concurrency=1) with a short inter-batch pause
// to prevent adjacent transactions from stacking up in the same Apex context.
// ─────────────────────────────────────────────────────────────────────────────
async function upsertPriceLists(priceLists) {
    if (!Array.isArray(priceLists) || priceLists.length === 0) {
        log.warn('upsertPriceLists: nothing to send.');
        return { message: 'No price lists to upsert.' };
    }

    const token = await getSalesforceToken();

    let url = process.env.SF_API_URL_PriceList ||
        process.env.SF_API_URL_ProductMaster?.replace('ProductUpsertAPI', 'PriceListUpsertAPI');
    url = buildSalesforceUrl(url, instanceUrl);

    const headers = {
        'Content-Type': 'application/json',
        Authorization : `Bearer ${token}`
    };

    const total      = priceLists.length;
    const totalBatch = Math.ceil(total / PL_BATCH_SIZE);
    const summary    = { success: [], failed: [] };

    log.divider('PRICELIST UPSERT START');
    log.info(`Total records    : ${total}`);
    log.info(`Batch size       : ${PL_BATCH_SIZE}  (capped for Apex SOQL limit)`);
    log.info(`Total batches    : ${totalBatch}`);
    log.info(`Inter-batch delay: ${PL_BATCH_DELAY}ms`);
    log.info(`Endpoint         : ${url}`);
    log.divider();

    // Build batches
    const batches = [];
    for (let i = 0; i < total; i += PL_BATCH_SIZE) {
        batches.push(priceLists.slice(i, i + PL_BATCH_SIZE));
    }

    // Process SEQUENTIALLY — one batch at a time to avoid SOQL limit across
    // concurrent transactions sharing the same Apex execution context.
    for (let batchIdx = 0; batchIdx < batches.length; batchIdx++) {
        const batch       = batches[batchIdx];
        const batchNum    = batchIdx + 1;
        const recordRange = `records ${batchIdx * PL_BATCH_SIZE + 1}–${Math.min((batchIdx + 1) * PL_BATCH_SIZE, total)}`;
        log.info(`Batch ${batchNum}/${totalBatch} — sending ${batch.length} records (${recordRange})`);

        try {
            const response = await withRetry(
                () => axios.post(url, batch, { headers, timeout: REQ_TIMEOUT }),
                `BATCH-${batchNum}`
            );
            verifySFResponse(response, `BATCH-${batchNum}`);
            summary.success.push({ batch: batchNum, count: batch.length, status: response.status, response: response.data });
            log.ok(`Batch ${batchNum}/${totalBatch} — HTTP ${response.status} — ${batch.length} records OK`);
        } catch (err) {
            const status = err.response?.status ?? 'N/A';
            const body   = err.response?.data   ?? err.message;
            const codes  = batch.map(r => r.ProductCode ?? 'UNKNOWN');
            summary.failed.push({ batch: batchNum, count: batch.length, status, error: body, codes });
            log.error(`Batch ${batchNum}/${totalBatch} — HTTP ${status} — FAILED`);
            log.error(`  Detail: ${JSON.stringify(body)}`);
        }

        log.progress(batchIdx + 1, totalBatch, 'batches');

        // Pause between batches to let SF finish the previous transaction
        if (PL_BATCH_DELAY > 0 && batchIdx < batches.length - 1) {
            await sleep(PL_BATCH_DELAY);
        }
    }

    const successRecords = summary.success.reduce((n, b) => n + b.count, 0);
    const failedRecords  = summary.failed.reduce ((n, b) => n + b.count, 0);

    log.divider('PRICELIST UPSERT SUMMARY');
    log.ok  (`Batches OK     : ${summary.success.length}  (${successRecords} records)`);
    if (summary.failed.length > 0) {
        log.error(`Batches FAILED : ${summary.failed.length}  (${failedRecords} records)`);
    }
    log.divider();

    if (summary.success.length === 0) throw new Error(`All ${totalBatch} PriceList batches failed.`);

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
// IMAGES  — batched
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
    log.info(`Total batches : ${totalBatch}`);
    log.info(`Endpoint      : ${url}`);
    log.divider();

    const batches = [];
    for (let i = 0; i < total; i += BATCH_SIZE) batches.push(images.slice(i, i + BATCH_SIZE));

    const tasks = batches.map((batch, batchIdx) => async () => {
        const batchNum = batchIdx + 1;
        try {
            const response = await withRetry(
                () => axios.post(url, batch, { headers, timeout: REQ_TIMEOUT }),
                `IMG-BATCH-${batchNum}`
            );
            verifySFResponse(response, `IMG-BATCH-${batchNum}`);
            summary.success.push({ batch: batchNum, count: batch.length, status: response.status });
            log.ok(`Batch ${batchNum}/${totalBatch} — OK`);
        } catch (err) {
            const status = err.response?.status ?? 'N/A';
            const body   = err.response?.data   ?? err.message;
            summary.failed.push({ batch: batchNum, count: batch.length, status, error: body, codes: batch.map(r => r.skuCode) });
            log.error(`Batch ${batchNum}/${totalBatch} — FAILED: ${JSON.stringify(body)}`);
        }
        log.progress(summary.success.length + summary.failed.length, totalBatch, 'image batches');
    });

    await runWithConcurrency(tasks, CONCURRENCY);

    const successRecords = summary.success.reduce((n, b) => n + b.count, 0);
    const failedRecords  = summary.failed.reduce ((n, b) => n + b.count, 0);

    if (summary.success.length === 0) throw new Error(`All ${totalBatch} image batches failed.`);

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

// ─────────────────────────────────────────────────────────────────────────────
// SCHEMES  — one request per scheme/policy, CONCURRENCY in parallel
// ─────────────────────────────────────────────────────────────────────────────
async function upsertSchemes(schemes) {
    if (!Array.isArray(schemes) || schemes.length === 0) {
        log.warn('upsertSchemes: nothing to send.');
        return { message: 'No schemes to upsert.' };
    }

    const token = await getSalesforceToken();

    let url = process.env.SF_API_URL_Scheme;
    if (!url && process.env.SF_API_URL_ProductMaster) {
        url = process.env.SF_API_URL_ProductMaster.replace('ProductUpsertAPI', 'SchemeService');
        log.info(`Derived Scheme URL: ${url}`);
    }
    url = buildSalesforceUrl(url, instanceUrl);

    if (!url) throw new Error('SF_API_URL_Scheme is not set in .env');

    const headers = {
        'Content-Type': 'application/json',
        Authorization : `Bearer ${token}`
    };

    const total   = schemes.length;
    const summary = { success: [], failed: [] };

    log.divider('SCHEME UPSERT START');
    log.info(`Total schemes : ${total}`);
    log.info(`Concurrency   : ${CONCURRENCY}`);
    log.info(`Max retries   : ${MAX_RETRIES}`);
    log.info(`Endpoint      : ${url}`);
    log.divider();

    const tasks = schemes.map((scheme, idx) => async () => {
        const policyNum = scheme?.Policy?.PolicyNumber ?? `IDX-${idx}`;
        const policyId  = scheme?.Policy?.PolicyID     ?? 'UNKNOWN';

        try {
            console.log("****************", JSON.stringify(scheme))
            const response = await withRetry(
                () => axios.post(url, scheme, { headers, timeout: REQ_TIMEOUT }),
                policyNum
            );
            verifySFResponse(response, policyNum);
            summary.success.push({ policyNum, policyId, status: response.status, data: response.data });

            const done = summary.success.length + summary.failed.length;
            log.ok(`[${done}/${total}] [${policyNum}] HTTP ${response.status} — OK`);

        } catch (err) {
            const status = err.response?.status ?? 'N/A';
            const body   = err.response?.data   ?? err.message;
            summary.failed.push({ policyNum, policyId, status, error: body });

            const done = summary.success.length + summary.failed.length;
            log.error(`[${done}/${total}] [${policyNum}] HTTP ${status} — FAILED`);
            log.error(`  Detail: ${JSON.stringify(body)}`);
        }

        const done = summary.success.length + summary.failed.length;
        if (done % 10 === 0 || done === total) log.progress(done, total, 'schemes');
    });

    await runWithConcurrency(tasks, CONCURRENCY);

    log.divider('SCHEME UPSERT SUMMARY');
    log.info(`Total Sent : ${total}`);
    log.ok  (`Success    : ${summary.success.length}`);
    if (summary.failed.length > 0) {
        log.error(`Failed     : ${summary.failed.length}`);
        summary.failed.forEach(f =>
            log.warn(`  • [${f.policyNum}] HTTP ${f.status} — ${JSON.stringify(f.error)}`)
        );
    }
    log.divider();

    if (summary.success.length === 0 && summary.failed.length > 0) {
        throw new Error(`All ${summary.failed.length} scheme upsert(s) failed.`);
    }

    return {
        totalSchemes : total,
        successCount : summary.success.length,
        failedCount  : summary.failed.length,
        failedDetails: summary.failed
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// BUSINESS PARTNERS
// ─────────────────────────────────────────────────────────────────────────────
async function upsertBusinessPartners(payload) {
    const businessPartners = payload?.businessPartners ?? payload;

    if (!Array.isArray(businessPartners) || businessPartners.length === 0) {
        log.warn('upsertBusinessPartners: nothing to send.');
        return { message: 'No business partners to upsert.' };
    }

    const token = await getSalesforceToken();

    let url = process.env.SF_API_URL_BusinessPartner;
    if (!url && process.env.SF_API_URL_ProductMaster) {
        url = process.env.SF_API_URL_ProductMaster.replace('ProductUpsertAPI', 'BusinessPartnerUpsertAPI');
        log.info(`Derived BP URL: ${url}`);
    }
    url = buildSalesforceUrl(url, instanceUrl);

    if (!url) throw new Error('SF_API_URL_BusinessPartner is not set in .env');

    const headers = {
        'Content-Type': 'application/json',
        Authorization : `Bearer ${token}`
    };

    const total   = businessPartners.length;
    const summary = { success: [], failed: [] };

    log.divider('BP UPSERT START');
    log.info(`Total BPs        : ${total}`);
    log.info(`Concurrency      : ${CONCURRENCY}`);
    log.info(`Max retries/item : ${MAX_RETRIES}`);
    log.info(`Endpoint         : ${url}`);
    log.info(`Mode             : 1 BP per request (SF limit)`);
    log.divider();

    const tasks = businessPartners.map((bp, idx) => async () => {
        const code = bp.BPCode ?? `IDX-${idx}`;
        const body = { businessPartners: [bp] };

        try {
            console.log("*************body", JSON.stringify(body) )
            const response = await withRetry(
                () => axios.post(url, body, { headers, timeout: REQ_TIMEOUT }),
                code
            );
            
            verifySFResponse(response, code);
            summary.success.push({ code, status: response.status, data: response.data });

            const done = summary.success.length + summary.failed.length;
            log.ok(`[${done}/${total}] [${code}] HTTP ${response.status} — OK`);

        } catch (err) {
            const status  = err.response?.status ?? 'N/A';
            const errBody = err.response?.data   ?? err.message;
            summary.failed.push({ code, status, error: errBody });

            const done = summary.success.length + summary.failed.length;
            log.error(`[${done}/${total}] [${code}] HTTP ${status} — FAILED`);
            log.error(`  Detail: ${JSON.stringify(errBody)}`);
        }

        const done = summary.success.length + summary.failed.length;
        if (done % 10 === 0 || done === total) log.progress(done, total, 'BPs');
    });

    await runWithConcurrency(tasks, CONCURRENCY);

    log.divider('BP UPSERT SUMMARY');
    log.info(`Total Sent : ${total}`);
    log.ok  (`Success    : ${summary.success.length}`);
    if (summary.failed.length > 0) {
        log.error(`Failed     : ${summary.failed.length}`);
        summary.failed.forEach(f =>
            log.warn(`  • [${f.code}] HTTP ${f.status} — ${JSON.stringify(f.error)}`)
        );
    }
    log.divider();

    if (summary.success.length === 0 && summary.failed.length > 0) {
        throw new Error(`All ${summary.failed.length} BP upsert(s) failed.`);
    }

    return {
        totalBPs      : total,
        successRecords: summary.success.length,
        failedRecords : summary.failed.length,
        failedDetails : summary.failed
    };
}


async function upsertStockInventory(payload) {
    const stockInventory = payload;

    if (!Array.isArray(stockInventory) || stockInventory.length === 0) {
        log.warn('upsertstockInventory: nothing to send.');
        return { message: 'No Stock Inventory to upsert.' };
    }

    const token = await getSalesforceToken();

    let url = process.env.SF_API_URL_Stock;
    if (!url && process.env.SF_API_URL_Stock) {
        url = process.env.SF_API_URL_Stock.replace('ProductUpsertAPI', '/data/v63.0/composite/graph');
        log.info(`Derived Stock URL: ${url}`);
    }

    url = buildSalesforceUrl(url, instanceUrl);

    if (!url) throw new Error('SF_API_URL_BusinessPartner is not set in .env');

    const headers = {
        'Content-Type': 'application/json',
        Authorization : `Bearer ${token}`
    };

    const total   = stockInventory.length;
    const summary = { success: [], failed: [] };

    log.divider('BP UPSERT START');
    log.info(`Total BPs        : ${total}`);
    log.info(`Concurrency      : ${CONCURRENCY}`);
    log.info(`Max retries/item : ${MAX_RETRIES}`);
    log.info(`Endpoint         : ${url}`);
    log.info(`Mode             : 1 BP per request (SF limit)`);
    log.divider();

    const tasks = stockInventory.map((bp, idx) => async () => {
        const code = bp.BPCode ?? `IDX-${idx}`;
        const body = { stockInventory: [bp] };

        try {
            const response = await withRetry(
                () => axios.post(url, body, { headers, timeout: REQ_TIMEOUT }),
                code
            );
            
            verifySFResponse(response, code);
            summary.success.push({ code, status: response.status, data: response.data });

            const done = summary.success.length + summary.failed.length;
            log.ok(`[${done}/${total}] [${code}] HTTP ${response.status} — OK`);

        } catch (err) {
            const status  = err.response?.status ?? 'N/A';
            const errBody = err.response?.data   ?? err.message;
            summary.failed.push({ code, status, error: errBody });

            const done = summary.success.length + summary.failed.length;
            log.error(`[${done}/${total}] [${code}] HTTP ${status} — FAILED`);
            log.error(`  Detail: ${JSON.stringify(errBody)}`);
        }

        const done = summary.success.length + summary.failed.length;
        if (done % 10 === 0 || done === total) log.progress(done, total, 'BPs');
    });

    await runWithConcurrency(tasks, CONCURRENCY);

    log.divider('BP UPSERT SUMMARY');
    log.info(`Total Sent : ${total}`);
    log.ok  (`Success    : ${summary.success.length}`);
    if (summary.failed.length > 0) {
        log.error(`Failed     : ${summary.failed.length}`);
        summary.failed.forEach(f =>
            log.warn(`  • [${f.code}] HTTP ${f.status} — ${JSON.stringify(f.error)}`)
        );
    }
    log.divider();

    if (summary.success.length === 0 && summary.failed.length > 0) {
        throw new Error(`All ${summary.failed.length} BP upsert(s) failed.`);
    }

    return {
        totalBPs      : total,
        successRecords: summary.success.length,
        failedRecords : summary.failed.length,
        failedDetails : summary.failed
    };
}

module.exports = {
    upsertProducts,
    upsertPriceLists,
    uploadImages,
    upsertSchemes,
    upsertBusinessPartners
};