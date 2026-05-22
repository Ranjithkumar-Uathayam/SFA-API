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
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Format a Date object or ISO string to the EHR datetime2 string format:
 * "YYYY-MM-DD HH:MM:SS.0000000"
 */
function formatDatetime2(value) {
    if (!value) return null;
    const d   = new Date(value);
    if (isNaN(d.getTime())) return String(value);
    const pad = (n, w = 2) => String(n).padStart(w, '0');
    return (
        `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ` +
        `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}.0000000`
    );
}

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

        // Accommodate common token field names across EHR API versions
        const token = res
        if (!token) {
            throw new Error(`Token field not found in auth response: ${JSON.stringify(res.data)}`);
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

/**
 * Push attendance records to the EHR Attendance API.
 *
 * @param {Array<{EmployeeId, PunchType, PunchTime, CaptureDateTime}>} records
 * @returns {{ success: boolean, status: number|undefined, data: any, error: any }}
 */
async function pushAttendanceToEhr(records) {
    if (!records || records.length === 0) {
        log.warn('pushAttendanceToEhr called with 0 records — nothing to push.');
        return { success: true, status: null, data: null, error: null };
    }

    const url   = EHR_BASE_URL();
    if (!url) throw new Error('EHR_BASE_URL is not set in .env');

    const token = await getEhrToken();

    const payload = records.map(r => ({
        EmployeeId     : r.EmployeeId,
        PunchType      : r.PunchType,
        PunchTime      : formatDatetime2(r.PunchTime),
        CaptureDateTime: formatDatetime2(r.CaptureDateTime),
    }));

    log.info(`Pushing ${payload.length} record(s) to EHR Attendance API — ${url}`);

    const maxRetries  = EHR_MAX_RETRIES();
    const retryDelay  = EHR_RETRY_DELAY();
    let   lastErr     = null;

    for (let attempt = 1; attempt <= maxRetries; attempt++) {
        try {
            const res = await axios.post(url, payload, {
                headers: {
                    Authorization : `Bearer ${token}`,
                    'Content-Type': 'application/json',
                },
                timeout: EHR_TIMEOUT(),
            });

            log.ok(`EHR push SUCCESS — HTTP ${res.status} | attempt ${attempt}/${maxRetries}`);
            log.info(`  Response: ${JSON.stringify(res.data)}`);
            return { success: true, status: res.status, data: res.data, error: null };

        } catch (err) {
            lastErr = err;
            const status = err.response?.status;
            const body   = JSON.stringify(err.response?.data ?? err.message);

            log.error(`EHR push FAILED — attempt ${attempt}/${maxRetries}`);
            log.error(`  HTTP Status : ${status ?? 'N/A'}`);
            log.error(`  Response    : ${body}`);

            // Do not retry on client-side errors (4xx) — they won't self-heal
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

    const status = lastErr?.response?.status;
    const error  = lastErr?.response?.data ?? lastErr?.message;
    return { success: false, status, data: null, error };
}

module.exports = { pushAttendanceToEhr };
