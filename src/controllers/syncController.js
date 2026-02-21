const dbService = require('../services/dbService');
const sfService = require('../services/sfService');
const mapper = require('../utils/dataMapper');

let lastProductSync = new Date('2024-01-01');

exports.syncProducts = async (req, res) => {
  console.log('\n🚀 Product Sync Started');

  let offset = 0;
  const limit = 500;
  const summary = { totalFetched: 0, totalSuccess: 0, totalFailed: 0, failedProducts: [] };

  try {
    while (true) {
      const rows = await dbService.getProductData(lastProductSync, offset, limit);
      if (rows.length === 0) break;

      summary.totalFetched += rows.length;
      console.log(`\n📦 Fetched ${rows.length} DB rows (offset: ${offset})`);

      const payload = mapper.mapToSalesforcePayload(rows);
      console.log(`   Mapped to ${payload.length} product payload(s)`);

      if (payload.length) {
        const results = await sfService.upsertProducts(payload);

        summary.totalSuccess += results.success.length;
        summary.totalFailed  += results.failed.length;

        if (results.failed.length > 0) {
          summary.failedProducts.push(...results.failed.map(f => f.code));
        }
      }

      offset += limit;
    }

    lastProductSync = new Date();

    console.log('\n✅ Product Sync Completed');
    console.log(`   Total DB Rows Fetched : ${summary.totalFetched}`);
    console.log(`   SF Upsert Success     : ${summary.totalSuccess}`);
    console.log(`   SF Upsert Failed      : ${summary.totalFailed}`);

    return res.status(200).json({
      message: summary.totalFailed === 0
        ? 'Product Sync Completed Successfully'
        : 'Product Sync Completed with some failures',
      totalFetched : summary.totalFetched,
      totalSuccess : summary.totalSuccess,
      totalFailed  : summary.totalFailed,
      failedProducts: summary.failedProducts
    });

  } catch (err) {
    console.log('\n❌ Product Sync Failed:', err.message);
    return res.status(500).json({
      message: 'Product Sync Failed',
      error: err.message
    });
  }
};

exports.syncPriceLists = async (req, res) => {
  try {
    console.log('\n🚀 PriceList Sync Started');

    const sqlData = await dbService.getPriceListData();

    if (!sqlData.length) {
      return res.status(200).json({ message: 'No price data found.' });
    }

    console.log(`📦 Fetched ${sqlData.length} records`);

    const payload = mapper.mapToPriceListPayload(sqlData);

    if (!payload.length) {
      return res.status(200).json({ message: 'No valid price data to sync.' });
    }

    const sfResponse = await sfService.upsertPriceLists(payload);

    return res.status(200).json({
      message: 'PriceList Sync Success',
      recordsSent: payload.length,
      salesforceResponse: sfResponse
    });

  } catch (error) {
    console.log('❌ PriceList Sync Error:', error.message);
    return res.status(500).json({
      error: error.message,
      details: error.response?.data ?? null
    });
  }
};

exports.syncImages = async (req, res) => {
  try {
    console.log('\n🚀 Image Sync Started');

    const sqlData = await dbService.getImageData();
    console.log(`📦 Fetched ${sqlData.length} image entries from DB`);

    const payload = mapper.mapToImagePayload(sqlData);

    if (payload.length === 0) {
      return res.json({ message: 'No image data to sync.' });
    }

    console.log('📤 Sending image data to Salesforce');

    const response = await sfService.uploadImages(payload);

    return res.json({
      message: 'Image Sync Success',
      salesforceResponse: response
    });

  } catch (error) {
    console.log('❌ Image Sync Error:', error.message);
    return res.status(500).json({
      error: error.message,
      details: error.response?.data ?? null
    });
  }
};