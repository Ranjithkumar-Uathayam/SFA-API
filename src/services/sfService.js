const axios = require('axios');
require('dotenv').config();

let cachedToken, tokenExpiry, instanceUrl;

// ─────────────────────────────────────────────
// AUTH
// ─────────────────────────────────────────────
async function getSalesforceToken() {
  if (cachedToken && tokenExpiry > new Date()) return cachedToken;

  try {
    const res = await axios.get(process.env.SF_AUTH_URL, {
      params: {
        grant_type: 'client_credentials',
        client_id: process.env.SF_CLIENT_ID,
        client_secret: process.env.SF_CLIENT_SECRET
      }
    });

    cachedToken = res.data.access_token;
    instanceUrl = res.data.instance_url;
    tokenExpiry = new Date(Date.now() + 55 * 60 * 1000);

    console.log('✅ Salesforce token acquired. Instance:', instanceUrl);
    return cachedToken;

  } catch (err) {
    console.log('❌ Salesforce Authentication Failed');
    console.log('   URL    :', process.env.SF_AUTH_URL);
    console.log('   Status :', err.response?.status);
    console.log('   Body   :', JSON.stringify(err.response?.data ?? err.message));
    throw new Error(`SF Auth Failed: ${err.response?.data?.error_description ?? err.message}`);
  }
}

// ─────────────────────────────────────────────
// URL BUILDER
// ─────────────────────────────────────────────
function buildUrl(baseUrl) {
  if (!baseUrl) throw new Error('SF_API_URL_ProductMaster is not set in .env');
  if (!instanceUrl) return baseUrl;

  try {
    const url = new URL(baseUrl);
    const instance = new URL(instanceUrl);
    url.protocol = instance.protocol;
    url.host = instance.host;
    return url.toString();
  } catch {
    return baseUrl;
  }
}

function buildSalesforceUrl(baseUrl, instanceUrl) {
  if (!baseUrl) return null;
  if (!instanceUrl) return baseUrl;

  try {
    const base = new URL(baseUrl);
    const instance = new URL(instanceUrl);
    base.protocol = instance.protocol;
    base.host = instance.host;
    return base.toString();
  } catch {
    return baseUrl;
  }
}

// ─────────────────────────────────────────────
// VERIFY SF RESPONSE — checks status + body
// ─────────────────────────────────────────────
function verifySFResponse(response, productCode) {
  const { status, data } = response;

  // HTTP-level check
  if (status < 200 || status >= 300) {
    throw new Error(
      `[${productCode}] Unexpected HTTP status ${status}: ${JSON.stringify(data)}`
    );
  }

  // Body-level check — Salesforce often returns 200 with error inside body
  if (data) {
    // Common SF error patterns
    const bodyStr = JSON.stringify(data).toLowerCase();

    if (
      data.success === false ||
      data.Success === false ||
      data.status === 'error' ||
      data.Status === 'error' ||
      (Array.isArray(data) && data.some(d => d.success === false)) ||
      bodyStr.includes('"errorcode"') ||
      bodyStr.includes('"errormessage"') ||
      bodyStr.includes('"exceptionmessage"')
    ) {
      throw new Error(
        `[${productCode}] Salesforce returned an error in response body: ${JSON.stringify(data)}`
      );
    }
  }

  return true;
}

// ─────────────────────────────────────────────
// UPSERT PRODUCTS  (SF_API_URL_ProductMaster)
// ─────────────────────────────────────────────
async function upsertProducts(products) {
  const token = await getSalesforceToken();
  const url = buildUrl(process.env.SF_API_URL_ProductMaster);

  console.log(`\n📤 Upserting ${products.length} product(s) to: ${url}`);

  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${token}`
  };

  const CONCURRENCY = 5;
  const results = { success: [], failed: [] };

  for (let i = 0; i < products.length; i += CONCURRENCY) {
    const chunk = products.slice(i, i + CONCURRENCY);
    const chunkNum = Math.floor(i / CONCURRENCY) + 1;
    const totalChunks = Math.ceil(products.length / CONCURRENCY);

    console.log(`\n  ── Batch ${chunkNum}/${totalChunks} (${chunk.length} records) ──`);

    await Promise.all(
      chunk.map(async (product) => {
        const code = product?.Product?.ProductCode ?? 'UNKNOWN';

        try {
          const response = await axios.post(url, product, { headers });

          verifySFResponse(response, code);

          console.log(`  ✅ [${code}] HTTP ${response.status} — Updated successfully`);
          console.log(`     SF Response: ${JSON.stringify(response.data)}`);

          results.success.push({ code, status: response.status, data: response.data });

        } catch (err) {
          const status = err.response?.status ?? 'N/A';
          const body   = err.response?.data   ?? err.message;

          console.log(`  ❌ [${code}] HTTP ${status} — Failed`);
          console.log(`     Error: ${JSON.stringify(body)}`);

          results.failed.push({ code, status, error: body });
        }
      })
    );
  }

  // ── Summary ──
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log(`📊 Product Upsert Summary`);
  console.log(`   ✅ Success : ${results.success.length}`);
  console.log(`   ❌ Failed  : ${results.failed.length}`);

  if (results.failed.length > 0) {
    console.log('\n  Failed Products:');
    results.failed.forEach(f =>
      console.log(`   • [${f.code}] HTTP ${f.status} — ${JSON.stringify(f.error)}`)
    );
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

    // Throw if ALL failed; partial failures are logged but don't hard-stop the sync
    if (results.success.length === 0) {
      throw new Error(
        `All ${results.failed.length} product upsert(s) failed. Check logs for details.`
      );
    }
  } else {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');
  }

  return results;
}

// ─────────────────────────────────────────────
// UPSERT PRICE LISTS
// ─────────────────────────────────────────────
async function upsertPriceLists(priceLists) {
  if (!Array.isArray(priceLists) || priceLists.length === 0) {
    return { message: 'No price lists to upsert.' };
  }

  const token = await getSalesforceToken();

  let url =
    process.env.SF_API_URL_PriceList ||
    process.env.SF_API_URL_ProductMaster?.replace('ProductUpsertAPI', 'PriceListUpsertAPI');

  url = buildSalesforceUrl(url, instanceUrl);

  console.log(`\n📤 Upserting ${priceLists.length} price list(s) to: ${url}`);

  const axiosConfig = {
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/json'
    },
    timeout: 30000
  };

  try {
    const response = await axios.post(url, priceLists, axiosConfig);

    verifySFResponse(response, 'PRICELIST');

    console.log(`✅ PriceList upsert HTTP ${response.status} — Success`);
    console.log(`   SF Response: ${JSON.stringify(response.data)}`);

    return response.data;

  } catch (error) {
    console.log('❌ Salesforce PriceList Upsert Failed');

    if (error.response) {
      console.log('   HTTP Status :', error.response.status);
      console.log('   SF Response :', JSON.stringify(error.response.data));
    } else {
      console.log('   Error       :', error.message);
    }

    throw error;
  }
}

// ─────────────────────────────────────────────
// UPLOAD IMAGES
// ─────────────────────────────────────────────
async function uploadImages(images) {
  const token = await getSalesforceToken();

  let url = process.env.SF_API_URL_UploadImages;

  if (!url && process.env.SF_API_URL_ProductMaster) {
    url = process.env.SF_API_URL_ProductMaster.replace('ProductUpsertAPI', 'UploadSKUImages');
    console.log('Derived Image Upload URL:', url);
  }

  if (instanceUrl) {
    try {
      const configUrl = new URL(url);
      const instanceUri = new URL(instanceUrl);
      configUrl.protocol = instanceUri.protocol;
      configUrl.host = instanceUri.host;
      url = configUrl.toString();
    } catch (e) {
      console.warn('Failed to construct dynamic URL for Images', e);
    }
  }

  console.log(`\n📤 Uploading ${images.length} image(s) to: ${url}`);

  try {
    const response = await axios.post(url, images, {
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`
      }
    });

    verifySFResponse(response, 'IMAGES');

    console.log(`✅ Image upload HTTP ${response.status} — Success`);
    console.log(`   SF Response: ${JSON.stringify(response.data)}`);

    return response.data;

  } catch (error) {
    console.log('❌ Error uploading images.');
    if (error.response) console.log('   Data:', JSON.stringify(error.response.data));
    else console.log('   Error:', error.message);
    throw error;
  }
}

module.exports = {
  upsertProducts,
  upsertPriceLists,
  uploadImages
};