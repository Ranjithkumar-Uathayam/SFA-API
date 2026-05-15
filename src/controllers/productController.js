const dbService        = require('../services/dbService');
const sfService        = require('../services/sfService');
const mapper           = require('../utils/dataMapper');
const recordPushSvc    = require('../services/recordPushService');

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/products
// Query params: page, limit, search, pushStatus, division
// ─────────────────────────────────────────────────────────────────────────────
exports.getProducts = async (req, res) => {
    try {
        // Ensure [BBLive].[dbo].[SFA_RecordPushStatus] exists before the LEFT JOIN query runs
        await recordPushSvc.ensureTable();

        const page       = Math.max(1, parseInt(req.query.page,  10) || 1);
        const limit      = Math.min(200, Math.max(10, parseInt(req.query.limit, 10) || 50));
        const search     = req.query.search     || null;
        const pushStatus = req.query.pushStatus || null;
        const division   = req.query.division   || null;

        const result = await dbService.getProductsPaged({ page, limit, search, pushStatus, division });
        return res.status(200).json({ success: true, ...result });
    } catch (err) {
        console.error('[productController] getProducts error:', err.message);
        return res.status(500).json({ success: false, error: err.message });
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/products/push
// Body: { productCodes: ["P001", "P002", ...] }
// Synchronous — waits for SF response and returns full result.
// ─────────────────────────────────────────────────────────────────────────────
exports.pushProducts = async (req, res) => {
    const { productCodes } = req.body;

    if (!Array.isArray(productCodes) || productCodes.length === 0) {
        return res.status(400).json({ success: false, error: 'productCodes must be a non-empty array' });
    }

    // Mark all as Pushing before we start
    await recordPushSvc.setManyPushing('products', productCodes);

    try {
        // Fetch product rows for the given codes
        const rows = await dbService.getProductDataByCodes(productCodes);

        if (!rows.length) {
            await recordPushSvc.setBulkResults('products',
                productCodes.map(c => ({ recordKey: c, success: false, errorMessage: 'Product not found in DB' }))
            );
            return res.status(404).json({
                success: false,
                error:   'No product data found for the given codes',
                requestedCodes: productCodes,
            });
        }

        // Map → Salesforce payload (groups rows by ProductCode)
        const payload = mapper.mapToSalesforcePayload(rows);

        if (!payload.length) {
            await recordPushSvc.setBulkResults('products',
                productCodes.map(c => ({ recordKey: c, success: false, errorMessage: 'Mapper produced no output' }))
            );
            return res.status(422).json({ success: false, error: 'Mapper produced no payload' });
        }

        // Push to Salesforce
        const sfResult = await sfService.upsertProducts(payload);

        // Persist per-product status
        const statusResults = [
            ...sfResult.success.map(s => ({
                recordKey:    s.code,
                success:      true,
                errorMessage: null,
            })),
            ...sfResult.failed.map(f => ({
                recordKey:    f.code,
                success:      false,
                errorMessage: typeof f.error === 'string' ? f.error : JSON.stringify(f.error),
            })),
        ];
        await recordPushSvc.setBulkResults('products', statusResults);

        return res.status(200).json({
            success:        sfResult.failed.length === 0,
            totalRequested: productCodes.length,
            totalFetched:   rows.length,
            totalMapped:    payload.length,
            successCount:   sfResult.success.length,
            failedCount:    sfResult.failed.length,
            failedProducts: sfResult.failed.map(f => ({
                code:  f.code,
                error: typeof f.error === 'string' ? f.error : JSON.stringify(f.error),
            })),
        });

    } catch (err) {
        // Mark all requested codes as Failed
        await recordPushSvc.setBulkResults('products',
            productCodes.map(c => ({ recordKey: c, success: false, errorMessage: err.message }))
        );
        console.error('[productController] pushProducts error:', err.message);
        return res.status(500).json({ success: false, error: err.message });
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/products/push-all
// Body: { search?, pushStatus?, division? }
// Async background — immediately returns; pushes all pages in background.
// ─────────────────────────────────────────────────────────────────────────────
exports.pushAllProducts = async (req, res) => {
    const { search, pushStatus, division } = req.body;

    // Return immediately; background job handles the rest
    res.status(200).json({
        success: true,
        message: 'Push-all started in background. Refresh the grid to see progress.',
    });

    // Background worker — processes page by page
    (async () => {
        const PAGE = 100;
        let page   = 1;
        let total  = Infinity;

        while ((page - 1) * PAGE < total) {
            try {
                const result = await dbService.getProductsPaged({ page, limit: PAGE, search, pushStatus, division });
                total = result.total;

                if (!result.data.length) break;

                const codes = result.data.map(r => r.ProductCode);
                await recordPushSvc.setManyPushing('products', codes);

                const rows    = await dbService.getProductDataByCodes(codes);
                const payload = mapper.mapToSalesforcePayload(rows);

                if (payload.length > 0) {
                    const sfResult = await sfService.upsertProducts(payload);
                    await recordPushSvc.setBulkResults('products', [
                        ...sfResult.success.map(s => ({ recordKey: s.code, success: true,  errorMessage: null })),
                        ...sfResult.failed.map (f => ({ recordKey: f.code, success: false, errorMessage: JSON.stringify(f.error) })),
                    ]);
                }
            } catch (err) {
                console.error(`[pushAllProducts] page ${page} error:`, err.message);
            }
            page++;
        }
        console.log('[pushAllProducts] background job complete');
    })();
};
