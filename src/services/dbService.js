const sql = require('mssql');
const config = require('../config/dbConfig');

let pool;

async function getPool() {
    if (!pool) {
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
            ISNULL(t0.U_SubGrp11, T0.U_SUBGRP6) AS ColorName,
            ISNULL(t0.U_SubGrp17, T0.U_SubGrp6) AS Color,
            ISNULL(t0.U_SubGrp13, T0.U_SubGrp6) AS Shade,
            t0.MinLevel AS Min_Qty,
            t0.MaxLevel AS Max_Qty,
            CASE WHEN t0.U_SubGrp13='Core item' THEN 1 ELSE 0 END AS IsCoreColor,
            t0.U_taxrate AS TaxBelow2500,
            t0.U_taxrate1000 AS TaxAbove2500,
            t0.U_SubGrp1 AS SubBrandCode
        FROM [BBLive].[dbo].oitm t0
        JOIN [BBLive].[dbo].oitb t1 ON t0.ItmsGrpCod = t1.ItmsGrpCod
        WHERE t0.U_SubGrp7='ALPHA' AND t0.validFor='Y'
            AND t0.UpdateDate >= @lastSyncDate
            AND t0.U_SubGrp1 NOT IN (
                'ACCESSORIES','ADVERTISEMENT','ALL','SAMPLE','PRINTING & STATIONERY',
                'IMPERIAL COMPUTERS','PACKING MATERIAL','REPAIRS & MAINTENANCE',
                'SALES PROMOTION EXPENSES','EVERYDAY DHOTIE','ALLDAYS DHOTIE',
                'ADD DHOTIE','ADD SHIRT','EVERYDAY SHIRTING','EVERYDAY RDY'
            )
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

        const query = `
            SELECT
                t0.ItemCode                                     AS ProductCode,
                B.DocEntry                                      AS PriceListID,
                B.U_Brand                                       AS SubBrandCode,
                t0.ItemCode                                     AS BPProductName,
                B.U_State                                       AS PriceListCode,
                NULL                                            AS EffectiveFrom,
                NULL                                            AS EffectiveTo,
                CASE WHEN B.U_Lock = 'Y' THEN 0 ELSE 1 END     AS PriceListIsActive,
                'Dealer'                                        AS BPCategory,
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
                AND t0.U_SubGrp7 = 'ALPHA'
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
        console.error('❌ SQL Error (PriceList):', err);
        throw err;
    }
}

async function getImageData() {
    try {
        const pool = await getPool();
        const result = await pool.request().query(`
            SELECT
                t0.itemcode       AS skuCode,
                T0.U_SubGrp6      AS ColorCode,
                ''                AS fileName,
                ''                AS Description,
                ''                AS base64Data
            FROM [BBLive].[dbo].oitm AS t0
            WHERE t0.U_SubGrp7 IN ('JETA')
        `);
        return result.recordset;
    } catch (err) {
        console.error('SQL Error (Images):', err);
        throw err;
    }
}


async function getSchemeData() {
    try {
        const pool = await getPool();

        const query = `
            SELECT
                -- ── Policy header (one value per T0 row; repeated across T1 lines) ─
                CAST(
                    CONCAT(T0.[Object], T0.DocNum)
                AS NVARCHAR(50))                                        AS PolicyNumber,
                0                                                      AS Revision,
                T0.DocEntry                                            AS PolicyID,
                RTRIM(ISNULL(T0.Remark, ''))                           AS PolicyName,
                RTRIM(ISNULL(T1.U_Schm,    ''))                        AS SavingType,
                RTRIM(ISNULL(T1.U_Discunt, ''))                        AS DiscountBasis,
                'P'                                                    AS Applicability,
                1                                                      AS IsCustomerDefined,
                1                                                      AS IsActive,
                'ARISER'                                               AS DivisionCode,
                CONVERT(VARCHAR(10), T0.U_FrmDt, 120)                  AS FromDate,
                CONVERT(VARCHAR(10), T0.U_ToDt,  120)                  AS ToDate,
                0                                                      AS AllowDiscountForAllProducts,
                NULL                                                   AS DiscountPer,

                -- ── Customer / state ──────────────────────────────────────────────
                -- BUG FIX #2: T1.U_CardCode is the direct customer on the scheme
                -- line. The SSMS test joined ORDR.DocEntry = T0.DocEntry which is
                -- wrong (different document types / DocEntry sequences).
                RTRIM(ISNULL(T1.U_CardCode,    ''))                    AS BPCode,
                RTRIM(ISNULL(CRD.U_SalPriceCode,''))                   AS StateCode,

                -- ── Product line (one row per T1 line) ───────────────────────────
                -- BUG FIX #3: ITM joined on T1.U_ItemCode, not a bare SELECT TOP 1
                RTRIM(ISNULL(ITM.ItemCode,  ''))                       AS ProductCode,
                RTRIM(ISNULL(ITM.U_Size,    ''))                       AS SizeCode,
                RTRIM(ISNULL(ITM.U_SubGrp6, ''))                       AS ColorCode,

                -- MinOrderQty = bills (order) quantity on the scheme line
                CAST(ISNULL(T1.U_BillsQty, 0)  AS DECIMAL(10,2))      AS MinOrderQty,

                -- FreeQty = offered / free goods quantity
                CAST(ISNULL(T1.U_OffersQty, 0) AS DECIMAL(10,2))      AS FreeQty,

                'S'                                                    AS ProductApplicability,

                -- BUG FIX #5: AllowMultiplyFreeQty is a boolean flag (0/1),
                -- not the raw quantity value
                CASE WHEN ISNULL(T1.U_OffersQty, 0) > 0 THEN 1
                     ELSE 0
                END                                                    AS AllowMultiplyFreeQty,

                -- BUG FIX #6: MaxAllowedFreeQty should cap the free goods,
                -- so it mirrors FreeQty (U_OffersQty), not the billing qty
                CAST(ISNULL(T1.U_OffersQty, 0) AS DECIMAL(10,2))      AS MaxAllowedFreeQty,

                1                                                      AS ProductIsActive,
                1                                                      AS MappingStatus

            FROM [BBLive].[dbo].[@SCHEM]  T0
            INNER JOIN [BBLive].[dbo].[@SCHEML] T1
                    ON T1.DocEntry = T0.DocEntry

            -- BUG FIX #2: resolve customer directly from scheme line → OCRD
            LEFT JOIN [BBLive].[dbo].OCRD CRD
                   ON CRD.CardCode = T1.U_CardCode

            -- BUG FIX #3: resolve item attributes from OITM via scheme line item code
            LEFT JOIN [BBLive].[dbo].OITM ITM
                   ON ITM.ItemCode = T1.U_ItemCode

            -- Only active schemes; exclude test / non-trade lines
            WHERE ISNULL(T1.U_ItemCode, '') <> ''

            ORDER BY T0.DocEntry, T1.LineId
        `;

        const { recordset } = await pool.request().query(query);
        console.log("**************************recordset", recordset)
        return recordset;

    } catch (err) {
        console.error('❌ SQL Error (Schemes):', err);
        throw err;
    }
}

module.exports = {
    getProductData,
    getPriceListData,
    getImageData,
    getSchemeData      
};