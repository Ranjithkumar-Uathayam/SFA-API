if (!AbortSignal.any) {
    AbortSignal.any = function (signals) {
        const controller = new AbortController();

        function onAbort() {
            controller.abort();
            cleanup();
        }

        function cleanup() {
            for (const s of signals) {
                s.removeEventListener("abort", onAbort);
            }
        }

        for (const s of signals) {
            s.addEventListener("abort", onAbort);
        }

        if (signals.some(s => s.aborted)) {
            controller.abort();
        }

        return controller.signal;
    };
}
const path = require('path');
const express = require('express');
const cors = require('cors');
require('dotenv').config();
const syncController = require('./src/controllers/syncController');
const pushController    = require('./src/controllers/pushController');
const productController = require('./src/controllers/productController');
const masterController  = require('./src/controllers/masterController');
const { startCronJobs } = require('./src/scheduler/cronJobs');

const app = express();
const PORT = process.env.PORT || 3100;

app.use(cors());
app.use(express.json());

// Serve frontend dashboard
app.use(express.static(path.join(__dirname, 'public')));

// ── Existing sync routes ───────────────────────────────────────────────────
app.post('/api/sync/products',          syncController.syncProducts);
app.post('/api/sync/pricelists',        syncController.syncPriceLists);
app.post('/api/sync/images',            syncController.syncImages);
app.post('/api/sync/schemes',           syncController.syncSchemes);
app.post('/api/sync/businesspartners',  syncController.syncBusinessPartners);
app.post('/api/sync/stockInventory',    syncController.syncStockInventory);
app.post('/api/sync/outstanding',       syncController.syncOutstanding);

// ── Dashboard & push routes ────────────────────────────────────────────────
app.get ('/api/dashboard/status', pushController.getDashboardStatus);
app.post('/api/push/single',      pushController.pushSingle);
app.post('/api/push/bulk',        pushController.pushBulk);

// ── Product Master routes ──────────────────────────────────────────────────
app.get ('/api/products',          productController.getProducts);
app.post('/api/products/push',     productController.pushProducts);
app.post('/api/products/push-all', productController.pushAllProducts);

// ── Generic master routes ──────────────────────────────────────────────────
app.get ('/api/master/:masterType/list',     masterController.getMasterList);
app.post('/api/master/:masterType/push',     masterController.pushMasterRecords);
app.post('/api/master/:masterType/push-all', masterController.pushAllMasterRecords);

// Health check (JSON for API consumers)
app.get('/health', (req, res) => {
    res.json({ status: 'ok', service: 'SFA API', port: PORT });
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
    console.log(`Endpoints available:`);
    console.log(` - POST /api/sync/products`);
    console.log(` - POST /api/sync/pricelists`);
    console.log(` - POST /api/sync/images`);
    console.log(` - POST /api/sync/schemes`);
    console.log(` - POST /api/sync/businesspartners`);
    console.log(` - POST /api/sync/stockInventory`);
    console.log(` - POST /api/sync/outstanding`);

    startCronJobs();
});
