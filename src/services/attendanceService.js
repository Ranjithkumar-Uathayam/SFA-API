const axios     = require('axios');
const dbService = require('./dbService');
const sfService = require('./sfService');

const SF_API_VERSION = process.env.SF_API_VERSION || 'v60.0';
const REQ_TIMEOUT    = parseInt(process.env.SF_TIMEOUT_MS, 10) || 30000;

function ts() { return new Date().toISOString().replace('T', ' ').slice(0, 23); }

const log = {
    info : (...a) => console.log (`[${ts()}] [ATTENDANCE] ℹ️ `, ...a),
    ok   : (...a) => console.log (`[${ts()}] [ATTENDANCE] ✅`, ...a),
    warn : (...a) => console.warn(`[${ts()}] [ATTENDANCE] ⚠️ `, ...a),
    error: (...a) => console.error(`[${ts()}] [ATTENDANCE] ❌`, ...a),
};

// instanceUrl from auth response has no trailing slash, so no double-slash risk
function buildQueryUrl(instanceUrl, soql) {
    return `${instanceUrl}/services/data/${SF_API_VERSION}/query?q=${encodeURIComponent(soql)}`;
}

function buildQueryUrl2(instanceUrl, soql) {
    return `${instanceUrl}//services/data/${SF_API_VERSION}/query?q=${soql}`;
}

async function fetchAllPages(initialUrl, instanceUrl, headers) {
    const records = [];
    const res = await axios.get(initialUrl, { headers, timeout: REQ_TIMEOUT });
    records.push(...(res.data.records || []));

    let nextPath = res.data.nextRecordsUrl || null;
    while (nextPath) {
        const pageRes = await axios.get(`${instanceUrl}${nextPath}`, { headers, timeout: REQ_TIMEOUT });
        records.push(...(pageRes.data.records || []));
        nextPath = pageRes.data.nextRecordsUrl || null;
    }
    return records;
}

// API 1 — employee attendance availability for today (Id + EmployeeId__c)
async function fetchAvailability(token, instanceUrl) {
    const q       = `SELECT Id,EmployeeId__c FROM dmpl__ResourceAvailability__c WHERE CreatedDate=TODAY`;
    const headers = { Authorization: `Bearer ${token}` };
    return fetchAllPages(buildQueryUrl(instanceUrl, q), instanceUrl, headers);
}

// API 2 — all punch records for today
// dmpl__ResourceAvailability__c is the lookup ID that links each punch back to API 1
async function fetchPunchRecords(token, instanceUrl) {
    const q       = `SELECT+Id%2CAttendenceTime__c%2Cdmpl__Type__c+FROM+dmpl__ResourceAvailabilityData__c+WHERE+CreatedDate%3DTODAY`;
    const headers = { Authorization: `Bearer ${token}` };
    return fetchAllPages(buildQueryUrl2(`${instanceUrl}`, q), instanceUrl, headers);
}

async function syncAttendance(punchType) {
    const startTime = Date.now();
    const elapsed   = () => `${((Date.now() - startTime) / 1000).toFixed(2)}s`;
    log.info(`──────────── ${punchType.toUpperCase()} SYNC START ────────────`);

    let token, instanceUrl;
    try {
        ({ token, instanceUrl } = await sfService.getAuthData());
    } catch (err) {
        log.error(`SF authentication failed: ${err.message}`);
        throw err;
    }

    // Step 1 — fetch availability records and build empMap (API 1)
    let availability;
    try {
        availability = await fetchAvailability(token, instanceUrl);
        log.info(`Fetched ${availability.length} availability record(s) from Salesforce`);
    } catch (err) {
        log.error(`Availability API failed [${err.response?.status}]: ${JSON.stringify(err.response?.data ?? err.message)}`);
        throw err;
    }

    if (!availability.length) {
        log.warn('No availability records found for today — nothing to sync.');
        log.info(`──────────── ${punchType.toUpperCase()} SYNC END (${elapsed()}) ────────────`);
        return { inserted: 0, skipped: 0, failed: 0, unmatched: 0 };
    }

    // Map: ResourceAvailability.Id → EmployeeId__c
    const empMap = new Map(availability.map(r => [r.Id, r.EmployeeId__c]));
    log.info(`Present employees today: ${empMap.size}`);

    // Step 2 — fetch all punch records, then filter by punchType (API 2)
    let punches;
    try {
        const allPunches = await fetchPunchRecords(token, instanceUrl);
        log.info(`Fetched ${allPunches.length} total punch record(s) from Salesforce`);
        punches = allPunches.filter(p => p.dmpl__Type__c === punchType);
        log.info(`Filtered to ${punches.length} ${punchType} record(s)`);
    } catch (err) {
        log.error(`Punch data API failed [${err.response?.status}]: ${JSON.stringify(err.response?.data ?? err.message)}`);
        throw err;
    }

    if (!punches.length) {
        log.warn(`No ${punchType} records found for today.`);
        log.info(`──────────── ${punchType.toUpperCase()} SYNC END (${elapsed()}) ────────────`);
        return { inserted: 0, skipped: 0, failed: 0, unmatched: 0 };
    }

    // Step 3 — match punch records to employees via the lookup ID field
    const punchCode = punchType === 'Check-In' ? 'I' : 'O';
    const rows      = [];
    let unmatched   = 0;

    for (const punch of punches) {
        
        console.log("empMap",empMap)
        const availId    = punch.Id;
        const employeeId = empMap.get(availId);
        if (!employeeId) {
            log.warn(`No employee match for punch Id=${punch.Id} (AvailId=${availId}) — skipping`);
            unmatched++;
            continue;
        }
        rows.push({
            RefId:      punch.Id,
            EmployeeId: employeeId,
            PunchType:  punchCode,
            PunchTime:  punch.AttendenceTime__c,
        });
    }

    if (unmatched > 0) log.warn(`${unmatched} punch record(s) skipped (no employee match)`);

    if (!rows.length) {
        log.warn('No rows to insert after matching.');
        log.info(`──────────── ${punchType.toUpperCase()} SYNC END (${elapsed()}) ────────────`);
        return { inserted: 0, skipped: 0, failed: 0, unmatched };
    }

    // Step 4 — upsert into SQL (duplicate RefId is silently skipped)
    const result     = await dbService.upsertPunchLog(rows);
    result.unmatched = unmatched;

    log.ok(`${punchType} sync complete in ${elapsed()}`);
    log.info(`Inserted  : ${result.inserted}`);
    log.info(`Skipped   : ${result.skipped} (duplicate RefId)`);
    if (result.failed    > 0) log.error(`Failed    : ${result.failed}`);
    if (result.unmatched > 0) log.warn (`Unmatched : ${result.unmatched}`);
    log.info(`──────────── ${punchType.toUpperCase()} SYNC END (${elapsed()}) ────────────`);

    return result;
}

module.exports = { syncAttendance };
