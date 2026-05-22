const axios = require('axios');

// ─────────────────────────────────────────────────────────────────────────────
// CONFIG  (all values from .env)
// ─────────────────────────────────────────────────────────────────────────────
const EHR_CLIENT_ID     = () => process.env.EHR_CLIENT_ID;
const EHR_CLIENT_SECRET = () => process.env.EHR_CLIENT_SECRET;
const EHR_TOKEN_URL     = () => process.env.EHR_TOKEN_URL;
const EHR_BASE_URL      = () => process.env.EHR_BASE_URL;
const EHR_TIMEOUT       = () => parseInt(process.env.EHR_TIMEOUT_MS, 10) || 30000;
const EHR_MAX_RETRIES   = () => parseInt(process.env.EHR_MAX_RETRIES, 10) || 3;
const EHR_RETRY_DELAY   = () => parseInt(process.env.EHR_RETRY_DELAY_MS, 10) || 2000;

// ─────────────────────────────────────────────────────────────────────────────
// LOGGER
// ─────────────────────────────────────────────────────────────────────────────
function ts() { return new Date().toISOString().replace('T', ' ').slice(0, 23); }

const log = {
    info  : (...a) => console.log (`[${ts()}] [EHR] ℹ️ `, ...a),
    ok    : (...a) => console.log (`[${ts()}] [EHR] ✅`, ...a),
    warn  : (...a) => console.warn(`[${ts()}] [EHR] ⚠️ `, ...a),
    error : (...a) => console.error(`[${ts()}] [EHR] ❌`, ...a),
};

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ─────────────────────────────────────────────────────────────────────────────
// AUTHENTICATION
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Fetch a fresh bearer token from the EHR Auth API.
 * Token is NOT cached — refreshed on every push cycle as required.
 */
async function getEhrToken() {
    const url = EHR_TOKEN_URL();
    if (!url) throw new Error('EHR_TOKEN_URL is not set in .env');

    log.info(`Requesting EHR token from ${url}…`);

    try {
        const res = await axios.post(
            url,
            { ClientId: EHR_CLIENT_ID(), ClientSecret: EHR_CLIENT_SECRET() },
            { headers: { 'Content-Type': 'application/json' }, timeout: EHR_TIMEOUT() }
        );

        // API returns the JWT token as a plain string in the response body
        const token = res.data
        if (!token) {
            throw new Error(`Unexpected auth response format: ${JSON.stringify(res.data)}`);
        }

        log.ok('EHR token acquired successfully.');
        return token;

    } catch (err) {
        const status = err.response?.status;
        const body   = JSON.stringify(err.response?.data ?? err.message);
        log.error('EHR Authentication FAILED');
        log.error(`  URL    : ${url}`);
        log.error(`  Status : ${status ?? 'N/A'}`);
        log.error(`  Body   : ${body}`);
        throw new Error(`EHR Auth Failed [HTTP ${status ?? 'N/A'}]: ${err.response?.data?.message ?? err.message}`);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PUSH
// ─────────────────────────────────────────────────────────────────────────────
function formatDateTime(value) {
    if (!value) return null;

    return new Date(value)
        .toISOString()
        .slice(0, 19);
}
/**
 * Push a single attendance record to the EHR Attendance API.
 * Retries on 5xx; stops immediately on 4xx.
 */
async function pushSingleRecord(token, record) {
    const url        = EHR_BASE_URL();
    const maxRetries = EHR_MAX_RETRIES();
    const retryDelay = EHR_RETRY_DELAY();
    
    const payload = {
        EmployeeId     : record.EmployeeId,
        PunchType      : record.PunchType,
        PunchTime      : formatDateTime(record.PunchTime),
        CaptureDateTime: formatDateTime(record.CaptureDateTime),
    };

    log.info(`  Sending: ${JSON.stringify(payload)}`);

    let lastErr = null;
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const res = await axios.post(url, payload, {
                headers: {
                    Authorization : `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
                timeout: EHR_TIMEOUT(),
            });

            log.ok(`  SUCCESS — HTTP ${res.status} | attempt ${attempt}/${maxRetries} | Response: ${JSON.stringify(res.data)}`);
            return { success: true, status: res.status, data: res.data, error: null };

        } catch (err) {
            lastErr = err;
            const status = err.response?.status;
            const body   = JSON.stringify(err.response?.data ?? err.message);

            log.error(`  FAILED — attempt ${attempt}/${maxRetries} | HTTP ${status ?? 'N/A'} | ${body}`);

            if (status && status >= 400 && status < 500) {
                log.warn(`  4xx error — not retrying.`);
                break;
            }

            if (attempt < maxRetries) {
                log.info(`  Retrying in ${retryDelay}ms…`);
                await sleep(retryDelay);
            }
        }
    }

    return {
        success: false,
        status : lastErr?.response?.status,
        data   : null,
        error  : lastErr?.response?.data ?? lastErr?.message,
    };
}

/**
 * Push attendance records to the EHR Attendance API — one request per record.
 *
 * @param {Array<{EmployeeId, PunchType, PunchTime, CaptureDateTime}>} records
 * @returns {{ results: Array<{ record, success, status, data, error }> }}
 */
async function pushAttendanceToEhr(records) {
    if (!records || records.length === 0) {
        log.warn('pushAttendanceToEhr called with 0 records — nothing to push.');
        return { results: [] };
    }

    const url = EHR_BASE_URL();
    if (!url) throw new Error('EHR_BASE_URL is not set in .env');

    const token = await getEhrToken();

    log.info(`Pushing ${records.length} record(s) individually to EHR Attendance API — ${url}`);

    const results = [];
    for (const record of records) {
        const result = await pushSingleRecord(token, record);
        results.push({ record, ...result });
    }

    const succeeded = results.filter(r => r.success).length;
    const failed    = results.length - succeeded;
    log.info(`EHR push complete — ${succeeded} succeeded, ${failed} failed out of ${results.length} total.`);

    return { results };
}

module.exports = { pushAttendanceToEhr };
