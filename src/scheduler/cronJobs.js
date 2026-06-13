'use strict';

const cron       = require('node-cron');
const dbService  = require('../services/dbService');
const sfService  = require('../services/sfService');
const ehrService = require('../services/ehrService');
const mapper     = require('../utils/dataMapper');

// ─────────────────────────────────────────────────────────────────────────────
// Logging helpers
// ─────────────────────────────────────────────────────────────────────────────

const TIMEZONE = 'Asia/Kolkata';

function utcTs() {
    return new Date().toISOString().replace('T', ' ').slice(0, 23) + ' UTC';
}

function istTs() {
    return new Date().toLocaleString('en-IN', {
        timeZone    : TIMEZONE,
        year        : 'numeric',
        month       : '2-digit',
        day         : '2-digit',
        hour        : '2-digit',
        minute      : '2-digit',
        second      : '2-digit',
        hour12      : false,
    });
}

function elapsed(startMs) {
    return `${((Date.now() - startMs) / 1000).toFixed(2)}s`;
}

const log = {
    info  : (...a) => console.log (`[${utcTs()}] [IST ${istTs()}] [CRON] ℹ️  `, ...a),
    ok    : (...a) => console.log (`[${utcTs()}] [IST ${istTs()}] [CRON] ✅ `, ...a),
    warn  : (...a) => console.warn (`[${utcTs()}] [IST ${istTs()}] [CRON] ⚠️  `, ...a),
    error : (...a) => console.error(`[${utcTs()}] [IST ${istTs()}] [CRON] ❌ `, ...a),
    banner: (title) => {
        const line = '─'.repeat(60);
        console.log(`\n${line}\n  ${title}\n${line}`);
    },
};

// ─────────────────────────────────────────────────────────────────────────────
// Guard flags — prevent overlapping runs if a job exceeds its interval
// ─────────────────────────────────────────────────────────────────────────────

let stockSyncRunning       = false;
let outstandingSyncRunning = false;
let checkInRunning         = false;
let checkOutRunning        = false;
let ehrCheckInRunning      = false;
let ehrCheckOutRunning     = false;

// ─────────────────────────────────────────────────────────────────────────────
// Stock inventory sync
// ─────────────────────────────────────────────────────────────────────────────

async function runStockInventorySync() {
    if (stockSyncRunning) {
        log.warn('Stock inventory sync already in progress — skipping this tick.');
        return;
    }
    stockSyncRunning = true;
    const startTime = Date.now();
    log.banner('STOCK INVENTORY SYNC START');
    log.info('Job: runStockInventorySync | IST trigger time noted above');

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

        if (!payload.length) {
            log.warn('Mapper produced 0 records — nothing to sync.');
            return;
        }

        const sfResult = await sfService.upsertStockInventory(payload);

        log.ok(`Stock Inventory Sync COMPLETE — elapsed: ${elapsed(startTime)}`);
        log.info(`  DB rows fetched : ${sqlData.length}`);
        log.info(`  Records sent    : ${sfResult.totalRecords}`);
        log.ok  (`  Succeeded       : ${sfResult.successRecords}`);
        if (sfResult.failedRecords > 0)
            log.error(`  Failed          : ${sfResult.failedRecords}`);

    } catch (err) {
        log.error(`Stock Inventory Sync FAILED after ${elapsed(startTime)}: ${err.message}`);
        log.error(err.stack);
    } finally {
        stockSyncRunning = false;
        log.banner('STOCK INVENTORY SYNC END');
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Outstanding sync
// ─────────────────────────────────────────────────────────────────────────────

async function runOutstandingSync() {
    if (outstandingSyncRunning) {
        log.warn('Outstanding sync already in progress — skipping this tick.');
        return;
    }
    outstandingSyncRunning = true;
    const startTime = Date.now();
    log.banner('OUTSTANDING SYNC START');
    log.info('Job: runOutstandingSync | IST trigger time noted above');

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

        if (!payload.length) {
            log.warn('Mapper produced 0 records — nothing to sync.');
            return;
        }

        const sfResult = await sfService.upsertOutstanding(payload);

        log.ok(`Outstanding Sync COMPLETE — elapsed: ${elapsed(startTime)}`);
        log.info(`  DB rows fetched : ${sqlData.length}`);
        log.info(`  Records sent    : ${sfResult.totalRecords}`);
        log.ok  (`  Succeeded       : ${sfResult.successRecords}`);
        if (sfResult.failedRecords > 0)
            log.error(`  Failed          : ${sfResult.failedRecords}`);

    } catch (err) {
        log.error(`Outstanding Sync FAILED after ${elapsed(startTime)}: ${err.message}`);
        log.error(err.stack);
    } finally {
        outstandingSyncRunning = false;
        log.banner('OUTSTANDING SYNC END');
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Attendance sync (Check-In at 14:00 IST, Check-Out at 23:30 IST)
// ─────────────────────────────────────────────────────────────────────────────

async function runAttendanceSync(punchTypeLabel, punchTypeCode) {
    const isCheckIn = punchTypeCode === 'I';
    const getGuard  = ()  => isCheckIn ? checkInRunning  : checkOutRunning;
    const setGuard  = (v) => { if (isCheckIn) checkInRunning = v; else checkOutRunning = v; };

    if (getGuard()) {
        log.warn(`Attendance ${punchTypeLabel} sync already in progress — skipping this tick.`);
        return;
    }
    setGuard(true);
    const startTime = Date.now();
    log.banner(`ATTENDANCE ${punchTypeLabel.toUpperCase()} SYNC START`);
    log.info(`Job: runAttendanceSync | PunchType: ${punchTypeCode} (${punchTypeLabel}) | IST trigger time noted above`);

    try {
        log.info(`Fetching ${punchTypeLabel} records from Salesforce…`);
        const sfRecords = await sfService.fetchAttendanceRecords(punchTypeLabel);
        log.info(`Fetched ${sfRecords.length} ${punchTypeLabel} record(s) from Salesforce`);

        if (!sfRecords.length) {
            log.warn(`No ${punchTypeLabel} records in Salesforce — nothing to sync.`);
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
                    RefId, EmployeeId, PunchType: punchTypeCode, PunchTime,
                });

                if (result.skipped) {
                    log.info(`  Duplicate skipped — RefId: ${RefId}`);
                    skipped++;
                } else {
                    log.info(`  Inserted (Pending) — RefId: ${RefId} | Employee: ${EmployeeId}`);
                    inserted++;
                }
            } catch (err) {
                log.error(`  Insert FAILED — RefId: ${RefId} | ${err.message}`);
                try { await dbService.updatePunchLogStatus(RefId, 'Failed'); } catch (_) {}
                failed++;
            }
        }

        log.ok(`Attendance ${punchTypeLabel} Sync COMPLETE — elapsed: ${elapsed(startTime)}`);
        log.info(`  Inserted (Pending) : ${inserted}`);
        log.info(`  Skipped (dup/null) : ${skipped}`);
        if (failed > 0) log.error(`  Failed             : ${failed}`);

    } catch (err) {
        log.error(`Attendance ${punchTypeLabel} Sync FAILED after ${elapsed(startTime)}: ${err.message}`);
        log.error(err.stack);
    } finally {
        setGuard(false);
        log.banner(`ATTENDANCE ${punchTypeLabel.toUpperCase()} SYNC END`);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EHR push sync (Check-In and Check-Out — every 90 minutes)
// ─────────────────────────────────────────────────────────────────────────────

async function runEhrPushSync(punchTypeCode) {
    const label     = punchTypeCode === 'I' ? 'Check-In' : 'Check-Out';
    const isCheckIn = punchTypeCode === 'I';
    const getGuard  = ()  => isCheckIn ? ehrCheckInRunning  : ehrCheckOutRunning;
    const setGuard  = (v) => { if (isCheckIn) ehrCheckInRunning = v; else ehrCheckOutRunning = v; };

    if (getGuard()) {
        log.warn(`EHR ${label} push already in progress — skipping this tick.`);
        return;
    }
    setGuard(true);
    const startTime = Date.now();
    log.banner(`EHR ${label.toUpperCase()} PUSH START`);
    log.info(`Job: runEhrPushSync | PunchType: ${punchTypeCode} (${label}) | IST trigger time noted above`);

    let records = [];
    try {
        log.info(`Fetching Pending ${label} records from ehr_punch_log…`);
        records = await dbService.getPendingPunchLogs(punchTypeCode);
        log.info(`Found ${records.length} Pending ${label} record(s) to push`);

        if (!records.length) {
            log.warn(`No Pending ${label} records — nothing to push to EHR.`);
            return;
        }

        log.info(`Pushing ${records.length} ${label} record(s) to EHR API…`);
        const { results } = await ehrService.pushAttendanceToEhr(records);

        const succeededIds = [];
        const failedIds    = [];

        for (let i = 0; i < results.length; i++) {
            const r      = results[i];
            const rec    = records[i];
            if (r.success) {
                succeededIds.push(rec.Id);
                log.info(`  Pushed  — Id: ${rec.Id} | Employee: ${rec.EmployeeId ?? 'N/A'}`);
            } else {
                failedIds.push(rec.Id);
                log.error(`  FAILED  — Id: ${rec.Id} | HTTP ${r.status ?? 'N/A'} | ${JSON.stringify(r.error)}`);
            }
        }

        if (succeededIds.length) {
            await dbService.updatePunchLogStatusByIds(succeededIds, 'Pushed');
            log.ok(`  DB updated → Pushed for ${succeededIds.length} record(s)`);
        }
        if (failedIds.length) {
            await dbService.updatePunchLogStatusByIds(failedIds, 'Failed');
            log.error(`  DB updated → Failed for ${failedIds.length} record(s)`);
        }

        log.ok(
            `EHR ${label} Push COMPLETE — ` +
            `Pushed: ${succeededIds.length}, Failed: ${failedIds.length} | ` +
            `elapsed: ${elapsed(startTime)}`
        );

    } catch (err) {
        log.error(`EHR ${label} Push unhandled error after ${elapsed(startTime)}: ${err.message}`);
        log.error(err.stack);
        if (records.length) {
            const allIds = records.map(r => r.Id);
            await dbService.updatePunchLogStatusByIds(allIds, 'Failed')
                .catch(dbErr => log.error(`Failed to mark records as Failed in DB: ${dbErr.message}`));
            log.error(`  DB updated → Failed for ${allIds.length} record(s) due to unhandled error`);
        }
    } finally {
        setGuard(false);
        log.banner(`EHR ${label.toUpperCase()} PUSH END`);
    }
}


function scheduleDaily(hour, minute, label, callback) {
    const hh   = String(hour).padStart(2, '0');
    const mm   = String(minute).padStart(2, '0');
    const expr = `${minute} ${hour} * * *`;

    // Validate the expression before registering
    if (!cron.validate(expr)) {
        log.error(`[REGISTRATION FAILED] Invalid cron expression "${expr}" for job "${label}"`);
        return;
    }

    log.info(`[REGISTERING] "${label}" | cron: "${expr}" | timezone: ${TIMEZONE} | time: ${hh}:${mm} IST`);

    cron.schedule(expr, async () => {
        const fireTime = Date.now();
        log.info(`━━━ CRON TRIGGERED ━━━ "${label}" | IST: ${istTs()}`);

        try {
            await callback();
        } catch (err) {
            log.error(`Unhandled error in scheduled job "${label}": ${err.message}`);
            log.error(err.stack);
        } finally {
            log.info(`━━━ CRON FINISHED  ━━━ "${label}" | elapsed: ${elapsed(fireTime)}`);
        }
    }, { timezone: TIMEZONE });

    log.ok(`[REGISTERED]  "${label}" — will fire daily at ${hh}:${mm} IST (cron: "${expr}")`);
}

function scheduleInterval(intervalMinutes, label, callback) {
    const intervalMs = intervalMinutes * 60 * 1000;

    log.info(`[REGISTERING] "${label}" | interval: every ${intervalMinutes} min`);

    const run = async () => {
        const fireTime = Date.now();
        log.info(`━━━ INTERVAL TRIGGERED ━━━ "${label}" | IST: ${istTs()}`);
        try {
            await callback();
        } catch (err) {
            log.error(`Unhandled error in interval job "${label}": ${err.message}`);
            log.error(err.stack);
        } finally {
            log.info(`━━━ INTERVAL FINISHED  ━━━ "${label}" | elapsed: ${elapsed(fireTime)}`);
        }
    };

    // Fire immediately on startup, then repeat every intervalMs
    run();
    setInterval(run, intervalMs);

    log.ok(`[REGISTERED]  "${label}" — fires every ${intervalMinutes} minutes (first run: immediate)`);
}

// ─────────────────────────────────────────────────────────────────────────────
// startCronJobs — called once from index.js inside app.listen() callback
// ─────────────────────────────────────────────────────────────────────────────

function startCronJobs() {
    log.banner('CRON SCHEDULER INITIALIZING');

    // ── Environment diagnostic ───────────────────────────────────────────────
    const serverTz     = Intl.DateTimeFormat().resolvedOptions().timeZone;
    const nowUtc       = new Date().toISOString();
    const nowIst       = new Date().toLocaleString('en-IN', { timeZone: TIMEZONE, hour12: false });
    const nodeVersion  = process.version;
    const pid          = process.pid;

    log.info(`Node.js version  : ${nodeVersion}`);
    log.info(`Process PID      : ${pid}`);
    log.info(`Server timezone  : ${serverTz}`);
    log.info(`Current time UTC : ${nowUtc}`);
    log.info(`Current time IST : ${nowIst}`);
    log.info(`Scheduled tz     : ${TIMEZONE}`);

    if (serverTz !== TIMEZONE) {
        log.warn(
            `Server timezone (${serverTz}) differs from scheduler timezone (${TIMEZONE}). ` +
            `All scheduleDaily() times are evaluated in ${TIMEZONE} — this is correct ` +
            `because node-cron is given an explicit timezone option.`
        );
    }

    // ── Job registration ─────────────────────────────────────────────────────
    log.info('Registering daily cron jobs…');

    scheduleDaily(14,  0, 'Attendance Check-In Sync (2:00 PM IST)',
        async () => { await runAttendanceSync('Check-In',  'I'); }
    );

    scheduleInterval(90, 'EHR Check-In Push (every 90 min)',
        async () => { await runEhrPushSync('I'); }
    );

    scheduleDaily(23, 30, 'Attendance Check-Out Sync (11:30 PM IST)',
        async () => { await runAttendanceSync('Check-Out', 'O'); }
    );

    scheduleInterval(90, 'EHR Check-Out Push (every 90 min)',
        async () => { await runEhrPushSync('O'); }
    );

    // ── Registration summary ─────────────────────────────────────────────────
    log.banner('CRON SCHEDULER READY');
    log.ok('All 4 jobs registered successfully:');
    log.ok('  [1] Attendance Check-In  Sync — 14:00 IST  (cron: "0 14 * * *")');
    log.ok('  [2] EHR Check-In         Push — every 90 minutes (interval)');
    log.ok('  [3] Attendance Check-Out Sync — 23:30 IST  (cron: "30 23 * * *")');
    log.ok('  [4] EHR Check-Out        Push — every 90 minutes (interval)');
    log.info('node-cron evaluates every 60 s against Asia/Kolkata wall clock.');
    log.info('Jobs will fire regardless of server OS timezone setting.');
}

// ─────────────────────────────────────────────────────────────────────────────
// Global safety net — log unhandled rejections so no silent job failures
// ─────────────────────────────────────────────────────────────────────────────

process.on('unhandledRejection', (reason, promise) => {
    log.error('Unhandled Promise Rejection detected:');
    log.error(`  Reason  : ${reason instanceof Error ? reason.message : String(reason)}`);
    if (reason instanceof Error) log.error(reason.stack);
});

process.on('uncaughtException', (err) => {
    log.error(`Uncaught Exception: ${err.message}`);
    log.error(err.stack);
    // Do NOT call process.exit() here — let PM2 / the OS decide restart policy.
});

module.exports = { startCronJobs, runAttendanceSync, runEhrPushSync };
