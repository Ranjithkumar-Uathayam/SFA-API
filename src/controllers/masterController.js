const dbService     = require('../services/dbService');
const sfService     = require('../services/sfService');
const mapper        = require('../utils/dataMapper');
const recordPushSvc = require('../services/recordPushService');

// Per-master config: how to fetch, map, push, and decode SF results
const MASTERS = {
    pricelists: {
        masterType:    'pricelists',
        recordKeyField: 'ProductCode',
        fetchPaged:    (opts)  => dbService.getPriceListsPaged(opts),
        fetchByCodes:  (codes) => dbService.getPriceListDataByCodes(codes),
        mapPayload:    (rows)  => mapper.mapToPriceListPayload(rows),
        sfPush:        (pl)    => sfService.upsertPriceLists(pl),
        // failedDetails: [{batch, codes:[ProductCode], error}]
        extractResults(sfResult, requestedKeys) {
            const failedMap = {};
            for (const f of (sfResult.failedDetails || [])) {
                const err = typeof f.error === 'string' ? f.error : JSON.stringify(f.error);
                for (const code of (f.codes || [])) failedMap[code] = err;
            }
            return {
                success: requestedKeys.filter(k => !failedMap[k]),
                failed:  Object.entries(failedMap).map(([k, e]) => ({ key: k, error: e })),
            };
        },
    },

    businesspartners: {
        masterType:    'businesspartners',
        recordKeyField: 'BPCode',
        fetchPaged:    (opts)  => dbService.getBPListPaged(opts),
        fetchByCodes:  (codes) => dbService.getBPMasterDataByCodes(codes),
        // mapToBPPayload returns { businessPartners:[...] } — normalize to array so the controller can check .length
        mapPayload:    (rows)  => (mapper.mapToBPPayload(rows).businessPartners || []),
        sfPush:        (bps)   => sfService.upsertBusinessPartners({ businessPartners: bps }),
        // failedDetails: [{code:BPCode, error}]
        extractResults(sfResult, requestedKeys) {
            const failedMap = {};
            for (const f of (sfResult.failedDetails || [])) {
                if (f.code) failedMap[f.code] = typeof f.error === 'string' ? f.error : JSON.stringify(f.error);
            }
            return {
                success: requestedKeys.filter(k => !failedMap[k]),
                failed:  Object.entries(failedMap).map(([k, e]) => ({ key: k, error: e })),
            };
        },
    },

    schemes: {
        masterType:    'schemes',
        recordKeyField: 'DocEntry',
        fetchPaged:    (opts)  => dbService.getSchemesPaged(opts),
        fetchByCodes:  (codes) => dbService.getSchemeDataByCodes(codes),
        mapPayload:    (rows)  => mapper.mapToSchemePayload(rows),
        sfPush:        (pl)    => sfService.upsertSchemes(pl),
        // failedDetails: [{policyNum, policyId:DocEntry, error}]
        extractResults(sfResult, requestedKeys) {
            const failedMap = {};
            for (const f of (sfResult.failedDetails || [])) {
                const k = String(f.policyId);
                if (k) failedMap[k] = typeof f.error === 'string' ? f.error : JSON.stringify(f.error);
            }
            return {
                success: requestedKeys.filter(k => !failedMap[String(k)]),
                failed:  Object.entries(failedMap).map(([k, e]) => ({ key: k, error: e })),
            };
        },
    },

    stockInventory: {
        masterType:    'stockInventory',
        recordKeyField: 'ProductCode',
        fetchPaged:    (opts)  => dbService.getStockPaged(opts),
        fetchByCodes:  (codes) => dbService.getStockDataByCodes(codes),
        mapPayload:    (rows)  => mapper.mapToStockPayload(rows),
        sfPush:        (pl)    => sfService.upsertStockInventory(pl),
        // failedDetails: [{batch, codes:[ItemCode], error}]
        extractResults(sfResult, requestedKeys) {
            const failedMap = {};
            for (const f of (sfResult.failedDetails || [])) {
                const err = typeof f.error === 'string' ? f.error : JSON.stringify(f.error);
                for (const code of (f.codes || [])) failedMap[code] = err;
            }
            return {
                success: requestedKeys.filter(k => !failedMap[k]),
                failed:  Object.entries(failedMap).map(([k, e]) => ({ key: k, error: e })),
            };
        },
    },

    outstanding: {
        masterType:    'outstanding',
        recordKeyField: 'CardCode',
        fetchPaged:    (opts)  => dbService.getOutstandingPaged(opts),
        fetchByCodes:  (codes) => dbService.getOutstandingDataByCodes(codes),
        mapPayload:    (rows)  => mapper.mapToOutstandingPayload(rows),
        sfPush:        (pl)    => sfService.upsertOutstanding(pl),
        // failedDetails: [{batch, codes:['CardCode:InvoiceNo'], error}]
        extractResults(sfResult, requestedKeys) {
            const failedCardCodes = new Set();
            for (const f of (sfResult.failedDetails || [])) {
                for (const code of (f.codes || [])) failedCardCodes.add(code.split(':')[0]);
            }
            return {
                success: requestedKeys.filter(k => !failedCardCodes.has(k)),
                failed:  [...failedCardCodes].map(k => ({ key: k, error: 'Batch failed' })),
            };
        },
    },
};

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/master/:masterType/list
// Query params: page, limit, search, pushStatus
// ─────────────────────────────────────────────────────────────────────────────
exports.getMasterList = async (req, res) => {
    const cfg = MASTERS[req.params.masterType];
    if (!cfg) return res.status(404).json({ success: false, error: 'Unknown master type' });

    try {
        await recordPushSvc.ensureTable();

        const page       = Math.max(1, parseInt(req.query.page,  10) || 1);
        const limit      = Math.min(200, Math.max(10, parseInt(req.query.limit, 10) || 50));
        const search     = req.query.search     || null;
        const pushStatus = req.query.pushStatus || null;

        const result = await cfg.fetchPaged({ page, limit, search, pushStatus });
        return res.status(200).json({ success: true, ...result });
    } catch (err) {
        console.error(`[masterController] getMasterList (${req.params.masterType}) error:`, err.message);
        return res.status(500).json({ success: false, error: err.message });
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/master/:masterType/push
// Body: { recordKeys: ["K001", "K002", ...] }
// Synchronous — waits for SF response, returns full result.
// ─────────────────────────────────────────────────────────────────────────────
exports.pushMasterRecords = async (req, res) => {
    const cfg = MASTERS[req.params.masterType];
    if (!cfg) return res.status(404).json({ success: false, error: 'Unknown master type' });

    const { recordKeys } = req.body;
    if (!Array.isArray(recordKeys) || recordKeys.length === 0) {
        return res.status(400).json({ success: false, error: 'recordKeys must be a non-empty array' });
    }

    await recordPushSvc.setManyPushing(cfg.masterType, recordKeys);

    try {
        const rows = await cfg.fetchByCodes(recordKeys);
        if (!rows.length) {
            await recordPushSvc.setBulkResults(cfg.masterType,
                recordKeys.map(k => ({ recordKey: k, success: false, errorMessage: 'Record not found in DB' }))
            );
            return res.status(404).json({ success: false, error: 'No data found for the given keys', requestedKeys: recordKeys });
        }

        const payload = cfg.mapPayload(rows);
        if (!payload.length) {
            await recordPushSvc.setBulkResults(cfg.masterType,
                recordKeys.map(k => ({ recordKey: k, success: false, errorMessage: 'Mapper produced no output' }))
            );
            return res.status(422).json({ success: false, error: 'Mapper produced no payload' });
        }

        const sfResult = await cfg.sfPush(payload);
        const { success: successKeys, failed } = cfg.extractResults(sfResult, recordKeys);

        await recordPushSvc.setBulkResults(cfg.masterType, [
            ...successKeys.map(k  => ({ recordKey: k,     success: true,  errorMessage: null })),
            ...failed.map(f       => ({ recordKey: f.key, success: false, errorMessage: f.error })),
        ]);

        return res.status(200).json({
            success:        failed.length === 0,
            totalRequested: recordKeys.length,
            successCount:   successKeys.length,
            failedCount:    failed.length,
            failedRecords:  failed,
        });

    } catch (err) {
        await recordPushSvc.setBulkResults(cfg.masterType,
            recordKeys.map(k => ({ recordKey: k, success: false, errorMessage: err.message }))
        );
        console.error(`[masterController] pushMasterRecords (${req.params.masterType}) error:`, err.message);
        return res.status(500).json({ success: false, error: err.message });
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/master/:masterType/push-all
// Body: { search?, pushStatus? }
// Async background — responds immediately, pushes all pages in background.
// ─────────────────────────────────────────────────────────────────────────────
exports.pushAllMasterRecords = async (req, res) => {
    const cfg = MASTERS[req.params.masterType];
    if (!cfg) return res.status(404).json({ success: false, error: 'Unknown master type' });

    const { search, pushStatus } = req.body;

    res.status(200).json({
        success: true,
        message: 'Push-all started in background. Refresh the grid to see progress.',
    });

    (async () => {
        const PAGE = 100;
        let page   = 1;
        let total  = Infinity;

        while ((page - 1) * PAGE < total) {
            try {
                const result = await cfg.fetchPaged({ page, limit: PAGE, search, pushStatus });
                total = result.total;
                if (!result.data.length) break;

                const keys = result.data.map(r => r[cfg.recordKeyField]);
                await recordPushSvc.setManyPushing(cfg.masterType, keys);

                const rows    = await cfg.fetchByCodes(keys);
                const payload = cfg.mapPayload(rows);

                if (payload.length > 0) {
                    const sfResult = await cfg.sfPush(payload);
                    const { success: successKeys, failed } = cfg.extractResults(sfResult, keys);
                    await recordPushSvc.setBulkResults(cfg.masterType, [
                        ...successKeys.map(k => ({ recordKey: k,     success: true,  errorMessage: null })),
                        ...failed.map(f      => ({ recordKey: f.key, success: false, errorMessage: f.error })),
                    ]);
                }
            } catch (err) {
                console.error(`[pushAll:${cfg.masterType}] page ${page} error:`, err.message);
            }
            page++;
        }
        console.log(`[pushAll:${cfg.masterType}] background job complete`);
    })();
};
