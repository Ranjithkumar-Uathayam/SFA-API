const syncController  = require('./syncController');
const statusService   = require('../services/pushStatusService');

const VALID_SYNC_TYPES = [
    'products', 'pricelists', 'images', 'schemes',
    'businesspartners', 'stockInventory', 'outstanding'
];

// ─── Adapts mock req/res so we can call existing sync handlers programmatically ─
function buildMockContext() {
    let resolver;
    const promise = new Promise((res) => { resolver = res; });
    const mockRes = {
        _code: 200,
        status(code) { this._code = code; return this; },
        json(data)   { resolver({ statusCode: this._code, data }); },
    };
    return { mockReq: { body: {} }, mockRes, promise };
}

async function runSync(syncType) {
    const handlerMap = {
        products:         syncController.syncProducts,
        pricelists:       syncController.syncPriceLists,
        images:           syncController.syncImages,
        schemes:          syncController.syncSchemes,
        businesspartners: syncController.syncBusinessPartners,
        stockInventory:   syncController.syncStockInventory,
        outstanding:      syncController.syncOutstanding,
    };

    const handler = handlerMap[syncType];
    if (!handler) throw new Error(`Unknown sync type: ${syncType}`);

    const { mockReq, mockRes, promise } = buildMockContext();
    handler(mockReq, mockRes);
    return promise;
}

// Maps diverse response shapes across all 7 sync types into unified counts
function extractCounts(syncType, data) {
    if (!data) return {};
    switch (syncType) {
        case 'products':
            return {
                totalRecords: data.totalDbRows,
                successCount: data.totalSuccess,
                failedCount:  data.totalFailed,
            };
        case 'schemes':
            return {
                totalRecords: data.dbRowsFetched,
                successCount: data.successCount,
                failedCount:  data.failedCount,
            };
        case 'businesspartners':
            return {
                totalRecords: data.dbRowsFetched,
                successCount: data.successRecords,
                failedCount:  data.failedRecords,
            };
        default: // pricelists, images, stockInventory, outstanding
            return {
                totalRecords: data.dbRowsFetched ?? data.totalRecords,
                successCount: data.successRecords,
                failedCount:  data.failedRecords ?? data.failedBatches,
            };
    }
}

async function persistResult(syncType, httpResult) {
    const isSuccess = httpResult.statusCode >= 200
        && httpResult.statusCode < 300
        && !httpResult.data?.error;

    const counts = extractCounts(syncType, httpResult.data);

    await statusService.setResult(syncType, {
        success:      isSuccess,
        errorMessage: httpResult.data?.error ?? null,
        ...counts,
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/dashboard/status
// ─────────────────────────────────────────────────────────────────────────────
exports.getDashboardStatus = async (req, res) => {
    try {
        const records = await statusService.getAllStatus();
        return res.status(200).json({ success: true, data: records });
    } catch (err) {
        return res.status(500).json({ success: false, error: err.message });
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/push/single   { syncType: "products" }
// Returns immediately; sync runs in background; DB status is polled by UI.
// ─────────────────────────────────────────────────────────────────────────────
exports.pushSingle = async (req, res) => {
    const { syncType } = req.body;

    if (!VALID_SYNC_TYPES.includes(syncType)) {
        return res.status(400).json({ success: false, error: `Invalid syncType: ${syncType}` });
    }

    try {
        await statusService.setRunning(syncType);
    } catch (err) {
        return res.status(500).json({ success: false, error: err.message });
    }

    // Respond immediately — sync runs in background
    res.status(200).json({ success: true, message: `${syncType} sync started`, syncType });

    runSync(syncType)
        .then(result => persistResult(syncType, result))
        .catch(err  => statusService.setResult(syncType, { success: false, errorMessage: err.message }));
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/push/bulk   { syncTypes: ["products", "pricelists", ...] }
// ─────────────────────────────────────────────────────────────────────────────
exports.pushBulk = async (req, res) => {
    const { syncTypes } = req.body;

    if (!Array.isArray(syncTypes) || syncTypes.length === 0) {
        return res.status(400).json({ success: false, error: 'syncTypes must be a non-empty array' });
    }

    const invalid = syncTypes.filter(t => !VALID_SYNC_TYPES.includes(t));
    if (invalid.length > 0) {
        return res.status(400).json({ success: false, error: `Invalid syncTypes: ${invalid.join(', ')}` });
    }

    // Mark all as Running before returning
    for (const syncType of syncTypes) {
        await statusService.setRunning(syncType);
    }

    res.status(200).json({
        success: true,
        message: `Bulk sync started for ${syncTypes.length} module(s)`,
        syncTypes,
    });

    // Run sequentially in background so SAP/SF aren't overwhelmed in parallel
    (async () => {
        for (const syncType of syncTypes) {
            try {
                const result = await runSync(syncType);
                await persistResult(syncType, result);
            } catch (err) {
                await statusService.setResult(syncType, { success: false, errorMessage: err.message });
            }
        }
    })();
};
