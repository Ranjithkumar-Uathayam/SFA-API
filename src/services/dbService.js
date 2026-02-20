const sql = require('mssql');
const config = require('../config/dbConfig');

let pool;

async function getPool() {
    if (!pool) 
    {
        pool = await sql.connect(config);
        console.log('SQL Pool Created');
    }
    return pool;
}

async function getProductData(lastSyncDate, offset = 0, limit = 500) {
  const pool = await getPool();

  const query = `
    SELECT
      t0.ItemCode AS ProductCode,
      t0.ItemName AS ProductName,
      CASE WHEN t0.validFor='Y' THEN 1 ELSE 0 END AS ProductIsActive,
      t1.ItmsGrpNam AS ProductGroupCode,
      t0.U_SubGrp7 AS ShortDesc,
      t0.ItemName AS DetailedDesc,
      t0.U_SubGrp3 AS CategoryName,
      t0.U_SubGrp4 AS StyleCode,
      RTRIM(t0.U_SubGrp5) AS SizeCode,
      CASE
            WHEN t0.U_SubGrp1 LIKE '%ARISER%'   THEN 'ARISER'
            WHEN t0.U_SubGrp1 LIKE '%UATHAYAM%' THEN 'UATHAYAM'
      END AS DivisionCode,
      t0.SalPackMsr AS UOM,
      t0.U_SubGrp3 AS AttributeSetName,
      RTRIM(t0.U_SubGrp5) AS SizeGroup,
      t0.U_HSNCODE AS HSNCode,
      t0.U_SubGrp1 AS Brand,
      t0.SalPackUn AS SalPackUn,
      RTRIM(t0.U_SubGrp6) AS ColorCode,
      t0.U_SubGrp11 AS ColorName,
      t0.U_SubGrp11 AS Color,
      t0.U_SubGrp13 AS Shade,
      t0.MinLevel AS Min_Qty,
      t0.MaxLevel AS Max_Qty,
      CASE WHEN t0.U_SubGrp13='Core item' THEN 1 ELSE 0 END AS IsCoreColor,
      t0.U_taxrate AS TaxBelow2500,
      t0.U_taxrate1000 AS TaxAbove2500,
      t0.U_SubGrp1 AS SubBrandCode
    FROM [BBLive].[dbo].oitm t0
    JOIN [BBLive].[dbo].oitb t1 ON t0.ItmsGrpCod = t1.ItmsGrpCod
    WHERE t0.U_SubGrp7='VUDU' and t0.validFor='Y'
      AND t0.UpdateDate >= @lastSyncDate
      AND U_SubGrp1 NOT IN ('ACCESSORIES','ADVERTISEMENT','ALL','SAMPLE','PRINTING & STATIONERY','IMPERIAL COMPUTERS','PACKING MATERIAL','REPAIRS & MAINTENANCE','SALES PROMOTION EXPENSES','EVERYDAY DHOTIE','ALLDAYS DHOTIE','ADD DHOTIE','ADD SHIRT','EVERYDAY SHIRTING','EVERYDAY RDY')
    ORDER BY t0.ItemCode
    OFFSET @offset ROWS
    FETCH NEXT @limit ROWS ONLY
  `;

  const result = await pool.request()
    .input('lastSyncDate', sql.DateTime, lastSyncDate)
    .input('offset', sql.Int, offset)
    .input('limit', sql.Int, limit)
    .query(query);

  return result.recordset;
}

// async function getPriceListData() {
//     try {
//         let pool = await sql.connect(config);
//         let result = await pool.request().query(`
//             select t0.ItemCode as ProductCode,
//             ----PriceList---
//             B.DocEntry as 'PriceListID',
//             B.U_Brand as 'SubBrandCode',
//             t0.ItemCode as 'BPProductName',
//             B.U_State as 'PriceLisCode',
//             '' as 'EffectiveFrom',
//             '' as 'EffectiveTo',
//             case when B.U_Lock ='Y' then 1 else 0 end  as 'IsActive',
//             ----------Prices-------------
//             B.DocEntry as 'PriceListID',
//             'BPCategory' as 'BPCategory',
//             B.U_SelPrice   as 'Price',
//             B.U_MRP    as 'MRP',
//             case when B.U_Lock ='Y' then 1 else 0 end as 'IsActive'
//             from [BBLive].[dbo].oitm as t0  WITH(NOLOCK)
//             LEFT JOIN
//             (
//             SELECT T0.DocEntry,T2.U_Brand,T1.U_ItemCode,t0.U_State,t0.U_SelPrice,t0.U_MRP,T2.U_Lock FROM [BBLive].[dbo].[@INS_PLM2] AS T0 WITH(NOLOCK)
//             INNER JOIN [BBLive].[dbo].[@INS_OPLM] AS T1 WITH(NOLOCK) ON T0.DocEntry=T1.DocEntry 
//             INNER JOIN [BBLive].[dbo].[@INS_PLM1] AS T2 WITH(NOLOCK) ON T0.DocEntry=T2.DocEntry 
//             --WHERE T0.DocEntry=56075
//             )B ON B.U_ItemCode=t0.ItemCode

//             where t0.U_SubGrp7 in ('JETA') AND B.U_SelPrice>0
//         `);
//         return result.recordset;
//     } catch (err) {
//         console.error('SQL Error (PriceList):', err);
//         throw err;
//     }
// }
async function getPriceListData() {
    try {
        const pool = await getPool();

        const query = `
            SELECT
                t0.ItemCode                          AS ProductCode,

                -- PriceList
                B.DocEntry                           AS PriceListID,
                B.U_Brand                            AS SubBrandCode,
                t0.ItemCode                          AS BPProductName,
                B.U_State                            AS PriceListCode,
                NULL                                 AS EffectiveFrom,
                NULL                                 AS EffectiveTo,
                CASE WHEN B.U_Lock = 'Y' THEN 0 ELSE 1 END AS PriceListIsActive,

                -- Prices
                'BPCategory'                         AS BPCategory,
                B.U_SelPrice                         AS Price,
                B.U_MRP                              AS MRP,
                CASE WHEN B.U_Lock = 'Y' THEN 0 ELSE 1 END AS PriceIsActive

            FROM [BBLive].[dbo].OITM t0
            LEFT JOIN (
                SELECT
                    T0.DocEntry,
                    T2.U_Brand,
                    T1.U_ItemCode,
                    T0.U_State,
                    T0.U_SelPrice,
                    T0.U_MRP,
                    T2.U_Lock
                FROM [BBLive].[dbo].[@INS_PLM2] T0
                INNER JOIN [BBLive].[dbo].[@INS_OPLM] T1 ON T0.DocEntry = T1.DocEntry
                INNER JOIN [BBLive].[dbo].[@INS_PLM1] T2 ON T0.DocEntry = T2.DocEntry
            ) B ON B.U_ItemCode = t0.ItemCode
            WHERE
                t0.U_SubGrp7 = 'JETA'
                AND B.U_SelPrice > 0
        `;

        const { recordset } = await pool.request().query(query);
        return recordset;

    } catch (err) {
        console.log('❌ SQL Error (PriceList):', err);
        throw err;
    }
}

async function getImageData() {
    try {
        let pool = await sql.connect(config);
        let result = await pool.request().query(`
            select t0.itemcode as skuCode,T0.U_SubGrp6 as ColorCode,
            ----images---
            '' as 'fileName',
            '' as 'Description',
            '' as 'base64Data'
            from [BBLive].[dbo].oitm as t0  where t0.U_SubGrp7 in ('JETA')
        `);
        return result.recordset;
    } catch (err) {
        console.error('SQL Error (Images):', err);
        throw err;
    }
}

module.exports = { 
    getProductData,
    getPriceListData,
    getImageData 
};
