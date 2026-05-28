const dbService  = require('../services/dbService');
const ehrService = require('../services/ehrService');
const { runAttendanceSync, runEhrPushSync } = require('../scheduler/cronJobs');

// GET /api/ehr/logs
exports.getEhrLogs = async (req, res) => {
    try {
        const { page = 1, limit = 50, search, punchType, pushStatus, dateFrom, dateTo } = req.query;
        const result = await dbService.getEhrLogsPaged({
            page:   parseInt(page,  10),
            limit:  parseInt(limit, 10),
            search, punchType, pushStatus, dateFrom, dateTo,
        });
        return res.status(200).json({ success: true, ...result });
    } catch (err) {
        return res.status(500).json({ success: false, error: err.message });
    }
};

function fireAndRespond(res, label, fn) {
    res.status(200).json({ success: true, message: `${label} started in background` });
    fn().catch(err => console.error(`[EHR Trigger] ${label} error:`, err.message));
}

// POST /api/ehr/trigger/sf-checkin
exports.triggerSFCheckIn = (req, res) =>
    fireAndRespond(res, 'SF Check-In Sync', () => runAttendanceSync('Check-In', 'I'));

// POST /api/ehr/trigger/sf-checkout
exports.triggerSFCheckOut = (req, res) =>
    fireAndRespond(res, 'SF Check-Out Sync', () => runAttendanceSync('Check-Out', 'O'));

// POST /api/ehr/trigger/ehr-checkin
exports.triggerEHRCheckIn = (req, res) =>
    fireAndRespond(res, 'EHR Check-In Push', () => runEhrPushSync('I'));

// POST /api/ehr/trigger/ehr-checkout
exports.triggerEHRCheckOut = (req, res) =>
    fireAndRespond(res, 'EHR Check-Out Push', () => runEhrPushSync('O'));

// POST /api/ehr/push/:id  — push a single punch log record to EHR API
exports.pushSingleRecord = async (req, res) => {
    const id = parseInt(req.params.id, 10);
    if (!id) return res.status(400).json({ success: false, error: 'Invalid record id' });

    try {
        const record = await dbService.getPunchLogById(id);
        if (!record) return res.status(404).json({ success: false, error: `Record ${id} not found` });

        const { results } = await ehrService.pushAttendanceToEhr([record]);
        const r = results[0];

        if (r.success) {
            await dbService.updatePunchLogStatusByIds([record.Id], 'Pushed');
            return res.json({ success: true, message: `Record ${id} pushed successfully` });
        } else {
            await dbService.updatePunchLogStatusByIds([record.Id], 'Failed');
            return res.status(502).json({ success: false, error: JSON.stringify(r.error) });
        }
    } catch (err) {
        return res.status(500).json({ success: false, error: err.message });
    }
};
