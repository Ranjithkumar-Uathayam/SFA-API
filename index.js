const express = require('express');
const cors = require('cors');
require('dotenv').config();
const syncController = require('./src/controllers/syncController');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Routes
// "above postman collection based" - implied /api/sync/... endpoints
app.post('/api/sync/products', syncController.syncProducts);
app.post('/api/sync/pricelists', syncController.syncPriceLists);
app.post('/api/sync/images', syncController.syncImages);

// Health check
app.get('/', (req, res) => {
    res.send('SFA API is running. Use POST /api/sync/... endpoints to trigger syncs.');
});

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
    console.log(`Endpoints available:`);
    console.log(` - POST /api/sync/products`);
    console.log(` - POST /api/sync/pricelists`);
    console.log(` - POST /api/sync/images`);
});

// Optional: Keep the cron scheduling logic if desired, but now triggering the controller logic directly could be done
/*
const cron = require('node-cron');
cron.schedule('0 0 * * *', () => {
    console.log('Running scheduled tasks...');
    // You would call the controller logic here, but mocked req/res needed or refactor controller to separate logic.
});
*/
