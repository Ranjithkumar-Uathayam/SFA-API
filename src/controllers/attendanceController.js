const attendanceService = require('../services/attendanceService');

function ts() { return new Date().toISOString().replace('T', ' ').slice(0, 23); }

async function syncCheckIn(req, res) {
    console.log(`[${ts()}] POST /api/attendance/sync-checkin`);
    try {
        const result = await attendanceService.syncAttendance('Check-In');
        res.json({ success: true, ...result });
    } catch (err) {
        console.error(`[${ts()}] ❌ Check-In sync error: ${err.message}`);
        res.status(500).json({ success: false, error: err.message });
    }
}

async function syncCheckOut(req, res) {
    console.log(`[${ts()}] POST /api/attendance/sync-checkout`);
    try {
        const result = await attendanceService.syncAttendance('Check-Out');
        res.json({ success: true, ...result });
    } catch (err) {
        console.error(`[${ts()}] ❌ Check-Out sync error: ${err.message}`);
        res.status(500).json({ success: false, error: err.message });
    }
}

module.exports = { syncCheckIn, syncCheckOut };
