const axios = require('axios');
require('dotenv').config();

let cachedToken, tokenExpiry, instanceUrl;

async function getSalesforceToken() {
  if (cachedToken && tokenExpiry > new Date()) return cachedToken;

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
  return cachedToken;
}

function buildUrl(baseUrl) {
  const url = new URL(baseUrl);
  const instance = new URL(instanceUrl);
  url.protocol = instance.protocol;
  url.host = instance.host;
  return url.toString();
}

async function upsertProducts(products) {
  const token = await getSalesforceToken();
  const url = buildUrl(process.env.SF_API_URL_ProductMaster);

  const headers = {
    'Content-Type': 'application/json',
    Authorization: `Bearer ${token}`
  };

  const CONCURRENCY = 5; // safe for Salesforce

  for (let i = 0; i < products.length; i += CONCURRENCY) {
    const chunk = products.slice(i, i + CONCURRENCY);

    await Promise.all(
      chunk.map(product =>
        axios.post(url, product, { headers })
      )
    );
  }
}

// async function upsertPriceLists(priceLists) {
//     const token = await getSalesforceToken();
//     let url = process.env.SF_API_URL_PriceList;

//     if (!url && process.env.SF_API_URL_ProductMaster) {
//         url = process.env.SF_API_URL_ProductMaster.replace('ProductUpsertAPI', 'PriceListUpsertAPI');
//         console.log('Derived PriceList URL:', url);
//     }

//     if (instanceUrl) {
//         try {
//             const configUrl = new URL(url);
//             const instanceUri = new URL(instanceUrl);
//             configUrl.protocol = instanceUri.protocol;
//             configUrl.host = instanceUri.host;
//             url = configUrl.toString();
//         } catch (e) {
//             console.warn('Failed to construct dynamic URL for PriceList', e);
//         }
//     }

//     try {
//         const config = {
//             headers: {
//                 'Content-Type': 'application/json',
//                 'Authorization': `Bearer ${token}`
//             }
//         };

//         // Assumption: Send array directly
//         const response = await axios.post(url, priceLists, config);
//         return response.data;
//     } catch (error) {
//         console.error('Error upserting price lists.');
//         if (error.response) console.error('Data:', JSON.stringify(error.response.data));
//         else console.error('Error:', error.message);
//         throw error;
//     }
// }
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

async function upsertPriceLists(priceLists) {
    if (!Array.isArray(priceLists) || priceLists.length === 0) {
        return { message: 'No price lists to upsert.' };
    }

    const token = await getSalesforceToken();

    let url =
        process.env.SF_API_URL_PriceList ||
        process.env.SF_API_URL_ProductMaster?.replace(
            'ProductUpsertAPI',
            'PriceListUpsertAPI'
        );

    url = buildSalesforceUrl(url, instanceUrl);

    const axiosConfig = {
        headers: {
            Authorization: `Bearer ${token}`,
            'Content-Type': 'application/json'
        },
        timeout: 30000
    };

    try {
        const { data } = await axios.post(url, priceLists, axiosConfig);
        return data;
    } catch (error) {
        console.log('❌ Salesforce PriceList Upsert Failed');

        if (error.response) {
            console.log('SF Response:', JSON.stringify(error.response.data));
        } else {
            console.log('Error:', error.message);
        }

        throw error;
    }
}

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

    try {
        const config = {
            headers: {
                'Content-Type': 'application/json',
                'Authorization': `Bearer ${token}`
            }
        };

        // Assumption: Send array directly
        const response = await axios.post(url, images, config);
        return response.data;
    } catch (error) {
        console.error('Error uploading images.');
        if (error.response) console.error('Data:', JSON.stringify(error.response.data));
        else console.error('Error:', error.message);
        throw error;
    }
}

module.exports = { 
    upsertProducts,
    upsertPriceLists,
    uploadImages 
};
