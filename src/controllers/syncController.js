const dbService = require('../services/dbService');
const sfService = require('../services/sfService');
const mapper = require('../utils/dataMapper');

let lastProductSync = new Date('2024-01-01');

exports.syncProducts = async (req, res) => {   
    console.log('Product sync started');
    let offset = 0;
    const limit = 500;

    try {
        while (true) {
            const rows = await dbService.getProductData(lastProductSync, offset, limit);
            if (rows.length === 0) break;

            const payload = mapper.mapToSalesforcePayload(rows);
            if (payload.length) {
                await sfService.upsertProducts(payload);
            }

            offset += limit;
        }

        lastProductSync = new Date();
        
        res.status(200).json({ message: 'Product Sync Completed' });
    } catch (err) {
        console.error('Sync Failed:', err);
    }
};

exports.syncPriceLists = async (req, res) => {
    try {
        console.log('🚀 Starting PriceList Sync');

        const sqlData = await dbService.getPriceListData();

        if (!sqlData.length) {
            return res.status(200).json({
                message: 'No price data found.'
            });
        }

        console.log(`📦 Fetched ${sqlData.length} records`);

        const payload = mapper.mapToPriceListPayload(sqlData);

        if (!payload.length) {
            return res.status(200).json({
                message: 'No valid price data to sync.'
            });
        }

        console.log('📤 Sending data to Salesforce');

        const sfResponse = await sfService.upsertPriceLists(payload);
        
        res.status(200).json({
            message: 'PriceList Sync Success',
            recordsSent: payload.length,
            salesforceResponse: sfResponse
        });

    } catch (error) {
        console.log('❌ PriceList Sync Error:', error);

        res.status(500).json({
            error: error.message,
            details: error.response?.data || null
        });
    }
};

exports.syncImages = async (req, res) => {
    try {
        console.log('Starting Image Sync...');
        const sqlData = await dbService.getImageData();
        console.log(`Fetched ${sqlData.length} image entries from DB.`);

        const payload = mapper.mapToImagePayload(sqlData);
        if (payload.length === 0) {
            return res.json({ message: 'No image data to sync.' });
        }

        console.log('Sending Image data...');
        const response = await sfService.uploadImages(payload);

        res.json({
            message: 'Image Sync Success',
            salesforceResponse: response
        });
    } catch (error) {
        console.error('Image Sync Error:', error);
        res.status(500).json({ error: error.message, details: error.response?.data });
    }
};
