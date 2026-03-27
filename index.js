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
const express = require('express');
const cors = require('cors');
require('dotenv').config();
const syncController = require('./src/controllers/syncController');

const app = express();
const PORT = process.env.PORT || 3100;

app.use(cors());
app.use(express.json());

// Routes
app.post('/api/sync/products',          syncController.syncProducts);
app.post('/api/sync/pricelists',        syncController.syncPriceLists);
app.post('/api/sync/images',            syncController.syncImages);
app.post('/api/sync/schemes',           syncController.syncSchemes);
app.post('/api/sync/businesspartners',  syncController.syncBusinessPartners);  
app.post('/api/sync/stockInventory',  syncController.syncStockInventory);

// Health check
app.get('/', (req, res) => {
    res.send(
        'SFA API is running.\n\n' +
        'Endpoints:\n' +
        '  POST /api/sync/products\n' +
        '  POST /api/sync/pricelists\n' +
        '  POST /api/sync/images\n' +
        '  POST /api/sync/schemes\n' +
        '  POST /api/sync/businesspartners\n' +
        ' POST /api/sync/stockInventory'
    );
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
});