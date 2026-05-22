const cron       = require('node-cron');
const dbService  = require('../services/dbService');
const sfService  = require('../services/sfService');
const ehrService = require('../services/ehrService');
const mapper     = require('../utils/dataMapper');

function ts()       { return new Date().toISOString().replace('T', ' ').slice(0, 23); }
function elapsed(t) { return `${((Date.now() - t) / 1000).toFixed(2)}s`; }

const log = {
    info  : (...a) => console.log (`[${ts()}] [CRON] ℹ️ `, ...a),
    ok    : (...a) => console.log (`[${ts()}] [CRON] ✅`, ...a),
    warn  : (...a) => console.warn(`[${ts()}] [CRON] ⚠️ `, ...a),
    error : (...a) => console.error(`[${ts()}] [CRON] ❌`, ...a),
};

// Guard flags prevent overlapping runs if a job takes longer than its interval
let stockSyncRunning       = false;
let outstandingSyncRunning = false;
let checkInRunning         = false;
let checkOutRunning        = false;
let ehrCheckInRunning      = false;
let ehrCheckOutRunning     = false;

async function runStockInventorySync() {
    if (stockSyncRunning) {
        log.warn('Stock inventory sync already in progress — skipping this tick.');
        return;
    }
    stockSyncRunning = true;
    const startTime  = Date.now();
    log.info('──────────── STOCK INVENTORY SYNC START ────────────');

    try {
        log.info('Fetching stock data from DB…');
        const sqlData = await dbService.getStockData();
        log.info(`Fetched ${sqlData.length} raw DB row(s)`);

        if (!sqlData.length) {
            log.warn('No stock data found in DB — nothing to sync.');
            return;
        }

        const payload = mapper.mapToStockPayload(sqlData);
        log.info(`Mapped to ${payload.length} stock record(s)`);

        if (payload.length === 0) {
            log.warn('Mapper produced 0 records — nothing to sync.');
            return;
        }

        const sfResult = await sfService.upsertStockInventory(payload);

        log.ok(`Stock Inventory Sync COMPLETE — elapsed: ${elapsed(startTime)}`);
        log.info(`DB rows fetched : ${sqlData.length}`);
        log.info(`Records sent    : ${sfResult.totalRecords}`);
        log.ok  (`Success         : ${sfResult.successRecords}`);
        if (sfResult.failedRecords > 0) {
            log.error(`Failed          : ${sfResult.failedRecords}`);
        }
    } catch (err) {
        log.error(`Stock Inventory Sync FAILED after ${elapsed(startTime)}: ${err.message}`);
    } finally {
        stockSyncRunning = false;
        log.info('──────────── STOCK INVENTORY SYNC END ────────────');
    }
}

async function runOutstandingSync() {
    if (outstandingSyncRunning) {
        log.warn('Outstanding sync already in progress — skipping this tick.');
        return;
    }
    outstandingSyncRunning = true;
    const startTime        = Date.now();
    log.info('──────────── OUTSTANDING SYNC START ────────────');

    try {
        log.info('Fetching outstanding data from DB…');
        const sqlData = await dbService.getOutstandingData();
        log.info(`Fetched ${sqlData.length} raw DB row(s)`);

        if (!sqlData.length) {
            log.warn('No outstanding data found in DB — nothing to sync.');
            return;
        }

        const payload = mapper.mapToOutstandingPayload(sqlData);
        log.info(`Mapped to ${payload.length} outstanding record(s)`);

        if (payload.length === 0) {
            log.warn('Mapper produced 0 records — nothing to sync.');
            return;
        }

        const sfResult = await sfService.upsertOutstanding(payload);

        log.ok(`Outstanding Sync COMPLETE — elapsed: ${elapsed(startTime)}`);
        log.info(`DB rows fetched : ${sqlData.length}`);
        log.info(`Records sent    : ${sfResult.totalRecords}`);
        log.ok  (`Success         : ${sfResult.successRecords}`);
        if (sfResult.failedRecords > 0) {
            log.error(`Failed          : ${sfResult.failedRecords}`);
        }
    } catch (err) {
        log.error(`Outstanding Sync FAILED after ${elapsed(startTime)}: ${err.message}`);
    } finally {
        outstandingSyncRunning = false;
        log.info('──────────── OUTSTANDING SYNC END ────────────');
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ATTENDANCE — Check-In sync (runs at 11:00 AM daily)
// ─────────────────────────────────────────────────────────────────────────────

async function runAttendanceSync(punchTypeLabel, punchTypeCode) {
    const isCheckIn = punchTypeCode === 'I';
    const guard     = isCheckIn ? () => checkInRunning  : () => checkOutRunning;
    const setGuard  = (v) => { if (isCheckIn) checkInRunning = v; else checkOutRunning = v; };

    if (guard()) {
        log.warn(`Attendance ${punchTypeLabel} sync already in progress — skipping this tick.`);
        return;
    }
    setGuard(true);
    const startTime = Date.now();
    log.info(`──────────── ATTENDANCE ${punchTypeLabel.toUpperCase()} SYNC START ────────────`);

    try {
        await dbService.ensureEhrPunchLogTable();

        log.info(`Fetching ${punchTypeLabel} records from Salesforce…`);
        const sfRecords = await sfService.fetchAttendanceRecords(punchTypeLabel);
        log.info(`Fetched ${sfRecords.length} ${punchTypeLabel} record(s)`);

        if (!sfRecords.length) {
            log.warn(`No ${punchTypeLabel} records found in Salesforce — nothing to sync.`);
            return;
        }

        let inserted = 0, skipped = 0, failed = 0;

        for (const record of sfRecords) {
            const RefId      = record.Id;
            const EmployeeId = record.dmpl__ResourceId__r?.EmployeeId__c;
            const PunchTime  = record.AttendenceTime__c;

            if (!EmployeeId || !PunchTime) {
                log.warn(`  Skipping RefId ${RefId} — missing EmployeeId or PunchTime`);
                skipped++;
                continue;
            }

            try {
                const result = await dbService.insertPunchLog({
                    RefId, EmployeeId, PunchType: punchTypeCode, PunchTime
                });

                if (result.skipped) 
                {
                    log.info(`  Duplicate skipped — RefId: ${RefId}`);
                    skipped++;
                } 
                else
                {
                    log.info(`  Inserted (Pending) — RefId: ${RefId} | Employee: ${EmployeeId}`);
                    inserted++;
                }
            } catch (err) {
                log.error(`  Insert FAILED — RefId: ${RefId} | ${err.message}`);
                // Attempt to mark the row as Failed if it was partially written
                try { await dbService.updatePunchLogStatus(RefId, 'Failed'); } catch (_) {}
                failed++;
            }
        }

        log.ok(`Attendance ${punchTypeLabel} Sync COMPLETE — elapsed: ${elapsed(startTime)}`);
        log.info(`  Inserted (Pending): ${inserted}`);
        log.info(`  Skipped (dup/null): ${skipped}`);
        if (failed > 0) log.error(`  Failed            : ${failed}`);

    } catch (err) {
        log.error(`Attendance ${punchTypeLabel} Sync FAILED after ${elapsed(startTime)}: ${err.message}`);
    } finally {
        setGuard(false);
        log.info(`──────────── ATTENDANCE ${punchTypeLabel.toUpperCase()} SYNC END ────────────`);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EHR PUSH — Push Pending punch logs to the E-HR Attendance API
// ─────────────────────────────────────────────────────────────────────────────

async function runEhrPushSync(punchTypeCode) {
    const label     = punchTypeCode === 'I' ? 'Check-In' : 'Check-Out';
    const isCheckIn = punchTypeCode === 'I';
    const getGuard  = () => isCheckIn ? ehrCheckInRunning  : ehrCheckOutRunning;
    const setGuard  = (v) => { if (isCheckIn) ehrCheckInRunning = v; else ehrCheckOutRunning = v; };

    if (getGuard()) {
        log.warn(`EHR ${label} push already in progress — skipping this tick.`);
        return;
    }
    setGuard(true);
    const startTime = Date.now();
    log.info(`──────────── EHR ${label.toUpperCase()} PUSH START ────────────`);

    try {
        log.info(`Fetching Pending ${label} records from ehr_punch_log…`);
        const records = await dbService.getPendingPunchLogs(punchTypeCode);
        log.info(`Found ${records.length} Pending ${label} record(s)`);

        if (!records.length) {
            log.warn(`No Pending ${label} records — nothing to push.`);
            return;
        }

        const ids    = records.map(r => r.Id);
        const result = await ehrService.pushAttendanceToEhr(records);

        if (result.success) {
            await dbService.updatePunchLogStatusByIds(ids, 'Pushed');
            log.ok(`EHR ${label} Push COMPLETE — ${ids.length} record(s) marked Pushed | elapsed: ${elapsed(startTime)}`);
        } else {
            await dbService.updatePunchLogStatusByIds(ids, 'Failed');
            log.error(`EHR ${label} Push FAILED — ${ids.length} record(s) marked Failed | elapsed: ${elapsed(startTime)}`);
            log.error(`  HTTP : ${result.status ?? 'N/A'}`);
            log.error(`  Error: ${JSON.stringify(result.error)}`);
        }

    } catch (err) {
        log.error(`EHR ${label} Push unhandled error after ${elapsed(startTime)}: ${err.message}`);
    } finally {
        setGuard(false);
        log.info(`──────────── EHR ${label.toUpperCase()} PUSH END ────────────`);
    }
}

function startCronJobs() {
    // // Stock inventory sync — every 5 hours (at minute 0)
    // cron.schedule('0 */5 * * *', () => {
    //     log.info('Cron triggered: Stock Inventory Sync (every 5 hours)');
    //     runStockInventorySync();
    // });

    // // Outstanding sync — every 45 minutes
    // cron.schedule('*/45 * * * *', () => {
    //     log.info('Cron triggered: Outstanding Sync (every 45 minutes)');
    //     runOutstandingSync();
    // });

    // Attendance Check-In sync (SF → DB) — daily at 11:00 AM
    cron.schedule('0 11 * * *', () => {
        log.info('Cron triggered: Attendance Check-In Sync (11:00 AM)');
        runAttendanceSync('Check-In', 'I');
    });
    
    runEhrPushSync('O')
    // EHR Check-In push (DB → EHR API) — daily at 11:10 AM
    cron.schedule('10 11 * * *', () => {
        log.info('Cron triggered: EHR Check-In Push (11:10 AM)');
        runEhrPushSync('I');
    });

    // Attendance Check-Out sync (SF → DB) — daily at 11:30 PM
    cron.schedule('30 23 * * *', () => {
        log.info('Cron triggered: Attendance Check-Out Sync (11:30 PM)');
        runAttendanceSync('Check-Out', 'O');
    });

    // EHR Check-Out push (DB → EHR API) — daily at 11:40 PM
    cron.schedule('40 23 * * *', () => {
        log.info('Cron triggered: EHR Check-Out Push (11:40 PM)');
        runEhrPushSync('O');
    });
}

module.exports = { startCronJobs, runAttendanceSync, runEhrPushSync };
