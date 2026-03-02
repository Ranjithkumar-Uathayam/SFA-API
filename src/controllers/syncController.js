const dbService = require('../services/dbService');
const sfService = require('../services/sfService');
const mapper    = require('../utils/dataMapper');

// ─────────────────────────────────────────────────────────────────────────────
// Persistent last-sync timestamp (survives across API calls within one process)
// For production, persist this to a DB or file instead.
// ─────────────────────────────────────────────────────────────────────────────
let lastProductSync = new Date('2024-01-01');

// ─────────────────────────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────────────────────────
function ts()          { return new Date().toISOString().replace('T', ' ').slice(0, 23); }
function elapsed(t)    { return `${((Date.now() - t) / 1000).toFixed(2)}s`; }
function divider(l='') { console.log(`[${ts()}] ${'─'.repeat(20)} ${l} ${'─'.repeat(20)}`); }
const log = {
  info  : (...a) => console.log (`[${ts()}] ℹ️ `, ...a),
  ok    : (...a) => console.log (`[${ts()}] ✅`, ...a),
  warn  : (...a) => console.warn(`[${ts()}] ⚠️ `, ...a),
  error : (...a) => console.error(`[${ts()}] ❌`, ...a),
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/sync/products
// ─────────────────────────────────────────────────────────────────────────────
exports.syncProducts = async (req, res) => {
  const startTime = Date.now();
  divider('PRODUCT SYNC START');
  log.info(`Last sync timestamp : ${lastProductSync.toISOString()}`);

  let offset = 0;
  const PAGE_SIZE = parseInt(process.env.DB_PAGE_SIZE, 10) || 500;

  const summary = {
    totalDbRows   : 0,
    totalMapped   : 0,
    totalSuccess  : 0,
    totalFailed   : 0,
    failedProducts: [],
    pages         : 0
  };

  try {
    while (true) {
      summary.pages++;
      log.info(`── Page ${summary.pages} | DB offset: ${offset} | limit: ${PAGE_SIZE}`);

      const rows = await dbService.getProductData(lastProductSync, offset, PAGE_SIZE);

      if (rows.length === 0) {
        log.info(`No more rows at offset ${offset}. Pagination complete.`);
        break;
      }

      summary.totalDbRows += rows.length;
      log.info(`Fetched ${rows.length} DB row(s) (running total: ${summary.totalDbRows})`);

      const payload = mapper.mapToSalesforcePayload(rows);
      summary.totalMapped += payload.length;
      log.info(`Mapped to ${payload.length} product payload(s) (running total: ${summary.totalMapped})`);

      if (payload.length > 0) {
        const results = await sfService.upsertProducts(payload);

        summary.totalSuccess += results.success.length;
        summary.totalFailed  += results.failed.length;

        if (results.failed.length > 0) {
          summary.failedProducts.push(...results.failed.map(f => f.code));
        }
      }

      // If we got fewer rows than PAGE_SIZE, we're on the last page
      if (rows.length < PAGE_SIZE) break;

      offset += PAGE_SIZE;
    }

    lastProductSync = new Date();

    divider('PRODUCT SYNC COMPLETE');
    log.ok (`Elapsed          : ${elapsed(startTime)}`);
    log.info(`Pages fetched    : ${summary.pages}`);
    log.info(`Total DB rows    : ${summary.totalDbRows}`);
    log.info(`Total mapped     : ${summary.totalMapped}`);
    log.ok  (`SF success       : ${summary.totalSuccess}`);
    if (summary.totalFailed > 0) {
      log.error(`SF failed        : ${summary.totalFailed}`);
      log.warn (`Failed codes     : ${summary.failedProducts.join(', ')}`);
    }
    divider();

    return res.status(200).json({
      message       : summary.totalFailed === 0
                        ? 'Product Sync Completed Successfully'
                        : 'Product Sync Completed with some failures',
      elapsedSeconds: parseFloat(((Date.now() - startTime) / 1000).toFixed(2)),
      pages         : summary.pages,
      totalDbRows   : summary.totalDbRows,
      totalMapped   : summary.totalMapped,
      totalSuccess  : summary.totalSuccess,
      totalFailed   : summary.totalFailed,
      failedProducts: summary.failedProducts
    });

  } catch (err) {
    divider('PRODUCT SYNC ERROR');
    log.error(`Sync failed after ${elapsed(startTime)}: ${err.message}`);
    divider();

    return res.status(500).json({
      message       : 'Product Sync Failed',
      elapsedSeconds: parseFloat(((Date.now() - startTime) / 1000).toFixed(2)),
      error         : err.message
    });
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/sync/pricelists
// ─────────────────────────────────────────────────────────────────────────────
exports.syncPriceLists = async (req, res) => {
  const startTime = Date.now();
  divider('PRICELIST SYNC START');

  try {
    log.info('Fetching price list data from DB…');
    const sqlData = await dbService.getPriceListData();

    if (!sqlData.length) {
      log.warn('No price data found in DB.');
      return res.status(200).json({ message: 'No price data found.' });
    }

    log.info(`Fetched ${sqlData.length} raw DB row(s)`);

    log.info('Mapping to Salesforce payload…');
    const payload = mapper.mapToPriceListPayload(sqlData);

    if (!payload.length) {
      log.warn('Mapper produced 0 records — nothing to sync.');
      return res.status(200).json({ message: 'No valid price data to sync.' });
    }

    log.info(`Mapped to ${payload.length} product price record(s)`);

    // Log a sample of what's being sent (first 3 records)
    log.info('Sample payload (first 3):');
    payload.slice(0, 3).forEach((p, i) =>
      log.info(`  [${i + 1}] ${p.ProductCode} → ${p.PriceList?.length ?? 0} pricelist(s), ` +
               `${p.PriceList?.reduce((n, pl) => n + (pl.Prices?.length ?? 0), 0) ?? 0} price(s)`)
    );
    
    // const sfResult = await sfService.upsertPriceLists(payload);

    divider('PRICELIST SYNC COMPLETE');
    log.ok (`Elapsed          : ${elapsed(startTime)}`);
    log.info(`Records sent     : ${payload.length}`);
    log.ok  (`Batches OK       : ${sfResult.successBatches} (${sfResult.successRecords} records)`);
    if (sfResult.failedBatches > 0) {
      log.error(`Batches FAILED   : ${sfResult.failedBatches} (${sfResult.failedRecords} records)`);
    }
    divider();

    return res.status(200).json({
      message       : sfResult.failedBatches === 0
                        ? 'PriceList Sync Completed Successfully'
                        : 'PriceList Sync Completed with some batch failures',
      elapsedSeconds: parseFloat(((Date.now() - startTime) / 1000).toFixed(2)),
      dbRowsFetched : sqlData.length,
      recordsSent   : payload.length,
      ...sfResult
    });

  } catch (err) {
    divider('PRICELIST SYNC ERROR');
    log.error(`Sync failed after ${elapsed(startTime)}: ${err.message}`);
    divider();

    return res.status(500).json({
      message       : 'PriceList Sync Failed',
      elapsedSeconds: parseFloat(((Date.now() - startTime) / 1000).toFixed(2)),
      error         : err.message,
      details       : err.response?.data ?? null
    });
  }
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/sync/images
// ─────────────────────────────────────────────────────────────────────────────
exports.syncImages = async (req, res) => {
  const startTime = Date.now();
  divider('IMAGE SYNC START');

  try {
    log.info('Fetching image data from DB…');
    const sqlData = await dbService.getImageData();
    log.info(`Fetched ${sqlData.length} image entry(ies) from DB`);

    if (!sqlData.length) {
      log.warn('No image data found in DB.');
      return res.json({ message: 'No image data to sync.' });
    }

    const payload = mapper.mapToImagePayload(sqlData);
    log.info(`Mapped to ${payload.length} image payload(s)`);

    if (payload.length === 0) {
      log.warn('Mapper produced 0 payloads — nothing to upload.');
      return res.json({ message: 'No image data to sync.' });
    }

    const sfResult = await sfService.uploadImages(payload);

    divider('IMAGE SYNC COMPLETE');
    log.ok (`Elapsed          : ${elapsed(startTime)}`);
    log.info(`Images sent      : ${payload.length}`);
    log.ok  (`Batches OK       : ${sfResult.successBatches} (${sfResult.successRecords} images)`);
    if (sfResult.failedBatches > 0) {
      log.error(`Batches FAILED   : ${sfResult.failedBatches} (${sfResult.failedRecords} images)`);
    }
    divider();

    return res.status(200).json({
      message       : sfResult.failedBatches === 0
                        ? 'Image Sync Completed Successfully'
                        : 'Image Sync Completed with some batch failures',
      elapsedSeconds: parseFloat(((Date.now() - startTime) / 1000).toFixed(2)),
      dbRowsFetched : sqlData.length,
      imagesSent    : payload.length,
      ...sfResult
    });

  } catch (err) {
    divider('IMAGE SYNC ERROR');
    log.error(`Sync failed after ${elapsed(startTime)}: ${err.message}`);
    divider();

    return res.status(500).json({
      message       : 'Image Sync Failed',
      elapsedSeconds: parseFloat(((Date.now() - startTime) / 1000).toFixed(2)),
      error         : err.message,
      details       : err.response?.data ?? null
    });
  }
};