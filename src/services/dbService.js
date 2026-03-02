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
      t0.U_SubGrp7 AS ProductGroupCode,
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
      ISNULL(t0.U_SubGrp11,T0.U_SUBGRP6) AS ColorName,
      ISNULL(t0.U_SubGrp17,T0.U_SubGrp6) AS Color,
      ISNULL(t0.U_SubGrp13,T0.U_SubGrp6) AS Shade,
      t0.MinLevel AS Min_Qty,
      t0.MaxLevel AS Max_Qty,
      CASE WHEN t0.U_SubGrp13='Core item' THEN 1 ELSE 0 END AS IsCoreColor,
      t0.U_taxrate AS TaxBelow2500,
      t0.U_taxrate1000 AS TaxAbove2500,
      t0.U_SubGrp1 AS SubBrandCode
    FROM [BBLive].[dbo].oitm t0
    JOIN [BBLive].[dbo].oitb t1 ON t0.ItmsGrpCod = t1.ItmsGrpCod
    WHERE t0.U_SubGrp7='ALPHA' and t0.validFor='Y'
      AND t0.UpdateDate >= @lastSyncDate
      AND t0.U_SubGrp1 NOT IN ('ACCESSORIES','ADVERTISEMENT','ALL','SAMPLE','PRINTING & STATIONERY','IMPERIAL COMPUTERS','PACKING MATERIAL','REPAIRS & MAINTENANCE','SALES PROMOTION EXPENSES','EVERYDAY DHOTIE','ALLDAYS DHOTIE','ADD DHOTIE','ADD SHIRT','EVERYDAY SHIRTING','EVERYDAY RDY')
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

async function getPriceListData() {
    try {
        const pool = await getPool();

        // ─────────────────────────────────────────────────────────────────────────
        // Each row = one Price entry inside a PriceList for a ProductCode.
        // The mapper will group:  ProductCode → PriceListID → Prices[]
        //
        // T0  = [@INS_PLM2]  — price-list detail lines  (one row per state/BPCategory)
        // T1  = [@INS_OPLM]  — price-list header (links DocEntry → ItemCode)
        // T2  = [@INS_PLM1]  — price-list master (Brand, Lock, EffectiveFrom/To …)
        // ─────────────────────────────────────────────────────────────────────────
        const query = `
            SELECT
                t0.ItemCode                                     AS ProductCode,

                -- PriceList header fields
                B.DocEntry                                      AS PriceListID,
                B.U_Brand                                       AS SubBrandCode,
                t0.ItemCode                                     AS BPProductName,
                B.U_State                                       AS PriceListCode,
                NULL                                            AS EffectiveFrom,
                NULL                                       AS EffectiveTo,
                CASE WHEN B.U_Lock = 'Y' THEN 0 ELSE 1 END     AS PriceListIsActive,

                -- Price line fields
                'Dealer'                                  AS BPCategory,
                B.U_SelPrice                                    AS Price,
                B.U_MRP                                         AS MRP,
                CASE WHEN B.U_Lock = 'Y' THEN 0 ELSE 1 END     AS PriceIsActive

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
            WHERE B.U_SelPrice > 0
                AND t0.U_SubGrp7='ALPHA'
              AND B.U_Brand NOT IN (
                'ACCESSORIES','ADVERTISEMENT','ALL','SAMPLE',
                'PRINTING & STATIONERY','IMPERIAL COMPUTERS','PACKING MATERIAL',
                'REPAIRS & MAINTENANCE','SALES PROMOTION EXPENSES',
                'EVERYDAY DHOTIE','ALLDAYS DHOTIE','ADD DHOTIE','ADD SHIRT',
                'EVERYDAY SHIRTING','EVERYDAY RDY'
              )
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