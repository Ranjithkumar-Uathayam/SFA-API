const cron              = require('node-cron');
const dbService         = require('../services/dbService');
const sfService         = require('../services/sfService');
const mapper            = require('../utils/dataMapper');
const attendanceService = require('../services/attendanceService');

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
let checkInSyncRunning     = false;
let checkOutSyncRunning    = false;

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

async function runCheckInSync() {
    if (checkInSyncRunning) {
        log.warn('Check-In attendance sync already in progress — skipping this tick.');
        return;
    }
    checkInSyncRunning = true;
    const startTime    = Date.now();
    log.info('──────────── CHECK-IN ATTENDANCE SYNC START ────────────');
    try {
        const result = await attendanceService.syncAttendance('Check-In');
        log.ok(`Check-In Sync COMPLETE — elapsed: ${elapsed(startTime)}`);
        log.info(`Inserted  : ${result.inserted}`);
        log.info(`Skipped   : ${result.skipped} (duplicates)`);
        if (result.failed    > 0) log.error(`Failed    : ${result.failed}`);
        if (result.unmatched > 0) log.warn (`Unmatched : ${result.unmatched}`);
    } catch (err) {
        log.error(`Check-In Sync FAILED after ${elapsed(startTime)}: ${err.message}`);
    } finally {
        checkInSyncRunning = false;
        log.info('──────────── CHECK-IN ATTENDANCE SYNC END ────────────');
    }
}

async function runCheckOutSync() {
    if (checkOutSyncRunning) {
        log.warn('Check-Out attendance sync already in progress — skipping this tick.');
        return;
    }
    checkOutSyncRunning = true;
    const startTime     = Date.now();
    log.info('──────────── CHECK-OUT ATTENDANCE SYNC START ────────────');
    try {
        const result = await attendanceService.syncAttendance('Check-Out');
        log.ok(`Check-Out Sync COMPLETE — elapsed: ${elapsed(startTime)}`);
        log.info(`Inserted  : ${result.inserted}`);
        log.info(`Skipped   : ${result.skipped} (duplicates)`);
        if (result.failed    > 0) log.error(`Failed    : ${result.failed}`);
        if (result.unmatched > 0) log.warn (`Unmatched : ${result.unmatched}`);
    } catch (err) {
        log.error(`Check-Out Sync FAILED after ${elapsed(startTime)}: ${err.message}`);
    } finally {
        checkOutSyncRunning = false;
        log.info('──────────── CHECK-OUT ATTENDANCE SYNC END ────────────');
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

    // Check-In attendance sync — every day at 11:00 AM
    cron.schedule('0 11 * * *', () => {
        log.info('Cron triggered: Check-In Attendance Sync (11:00 AM daily)');
        runCheckInSync();
    });

    // Check-Out attendance sync — every day at 11:30 PM
    cron.schedule('30 23 * * *', () => {
        log.info('Cron triggered: Check-Out Attendance Sync (11:30 PM daily)');
        runCheckOutSync();
    });

    log.ok('Cron jobs scheduled:');
    log.ok('  Check-In Attendance Sync  → 11:00 AM daily  (0 11 * * *)');
    log.ok('  Check-Out Attendance Sync → 11:30 PM daily  (30 23 * * *)');
}

module.exports = { startCronJobs };
