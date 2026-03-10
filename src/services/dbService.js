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
                RTRIM(ISNULL(T1.U_CardCode,    ''))                    AS BPCode,
                RTRIM(ISNULL(CRD.U_SalPriceCode,''))                   AS StateCode,
                RTRIM(ISNULL(ITM.ItemCode,  ''))                       AS ProductCode,
                RTRIM(ISNULL(ITM.U_Size,    ''))                       AS SizeCode,
                RTRIM(ISNULL(ITM.U_SubGrp6, ''))                       AS ColorCode,
                CAST(ISNULL(T1.U_BillsQty, 0)  AS DECIMAL(10,2))      AS MinOrderQty,
                CAST(ISNULL(T1.U_OffersQty, 0) AS DECIMAL(10,2))      AS FreeQty,
                'S'                                                    AS ProductApplicability,
                CASE WHEN ISNULL(T1.U_OffersQty, 0) > 0 THEN 1
                     ELSE 0
                END                                                    AS AllowMultiplyFreeQty,
                CAST(ISNULL(T1.U_OffersQty, 0) AS DECIMAL(10,2))      AS MaxAllowedFreeQty,
                1                                                      AS ProductIsActive,
                1                                                      AS MappingStatus
            FROM [BBLive].[dbo].[@SCHEM]  T0
            INNER JOIN [BBLive].[dbo].[@SCHEML] T1
                    ON T1.DocEntry = T0.DocEntry
            LEFT JOIN [BBLive].[dbo].OCRD CRD
                   ON CRD.CardCode = T1.U_CardCode
            LEFT JOIN [BBLive].[dbo].OITM ITM
                   ON ITM.ItemCode = T1.U_ItemCode
            WHERE ISNULL(T1.U_ItemCode, '') <> ''
            ORDER BY T0.DocEntry, T1.LineId
        `;

        const { recordset } = await pool.request().query(query);
        console.log("**************************recordset", recordset);
        return recordset;

    } catch (err) {
        console.error('❌ SQL Error (Schemes):', err);
        throw err;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BP MASTER
// Sub-brand / division matrix — drives one query pass per combination.
// Add or remove entries here as business requires.
// ─────────────────────────────────────────────────────────────────────────────
const BP_SUBBRAND_CONFIG = [
    { subBrandName: 'ARISER SHIRT',          divisionCode: 'ARISER' },
    { subBrandName: 'ARISER MENS TROUSERS',  divisionCode: 'ARISER' },
    { subBrandName: 'UATHAYAM DHOTIE',       divisionCode: 'BANDB'  },
    { subBrandName: 'UATHAYAM SHIRTING',     divisionCode: 'BANDB'  },
    { subBrandName: 'UATHAYAM RDY',          divisionCode: 'BANDB'  },
    { subBrandName: 'UATHAYAM RDY DHOTIE',   divisionCode: 'BANDB'  },
    { subBrandName: 'UATHAYAM HOS',          divisionCode: 'BANDB'  },
    { subBrandName: 'UATHAYAM KIDS SET',     divisionCode: 'BANDB'  },
    { subBrandName: 'UATHAYAM MENS SET',     divisionCode: 'BANDB'  },
    { subBrandName: 'EVERYDAY DHOTIE',       divisionCode: 'BANDB'  },
    { subBrandName: 'EVERYDAY SHIRTING',     divisionCode: 'BANDB'  },
    { subBrandName: 'EVERYDAY RDY',          divisionCode: 'BANDB'  },
    { subBrandName: 'ADD DHOTIE',            divisionCode: 'BANDB'  },
    { subBrandName: 'ADD SHIRTING',          divisionCode: 'BANDB'  },
    { subBrandName: 'ADD SHIRT',             divisionCode: 'BANDB'  }
];

async function getBPMasterData() {
    try {
        const pool = await getPool();
        const allRows = [];

        const query = `
            SELECT
            T0.CardCode        AS BPCode,
            T0.CardName        AS BPName,
            T0.Currency        AS DefaultCurrency,
            CASE WHEN T0.validFor = 'Y' THEN 1 ELSE 0 END AS IsActive,
            0                  AS AllowCreditLimit,
            T0.CardFName       AS DisplayName,
            CASE WHEN T0.GroupCode in('100','106') THEN 'Dealer' ELSE '' END AS BPCategory,
            T0.GroupCode       AS BPGroupCode,
            T0.U_showcode      AS SR_BPCode,
            T0.U_Grade         AS GradeOfBP,
            ''                 AS CustomerRemark,
            0                  AS Latitude,
            0                  AS Longitude,
            T0.U_AreaCode      AS AreaCode,

            -- ── Division & SubBrand (cross-joined from lookup) ────────────────────
            SB.DivisionCode    AS DivisionCode,
            SB.SubBrandName    AS SubBrandName,

            -- ── Discount per sub-brand ────────────────────────────────────────────
            CASE SB.SubBrandName
                WHEN 'UATHAYAM DHOTIE'      THEN T0.U_Dis1
                WHEN 'UATHAYAM SHIRTING'    THEN T0.U_Dis1
                WHEN 'UATHAYAM RDY'         THEN T0.U_Dis2
                WHEN 'ADD SHIRT'            THEN T0.U_Dis2
                WHEN 'UATHAYAM RDY DHOTIE'  THEN T0.U_Dis8
                WHEN 'UATHAYAM HOS'         THEN T0.U_Dis3
                WHEN 'EVERYDAY DHOTIE'      THEN T0.U_Dis4
                WHEN 'EVERYDAY SHIRTING'    THEN T0.U_Dis4
                WHEN 'EVERYDAY RDY'         THEN T0.U_Dis5
                WHEN 'ADD DHOTIE'           THEN T0.U_Dis6
                WHEN 'ADD SHIRTING'         THEN T0.U_Dis6
                WHEN 'UATHAYAM KIDS SET'    THEN T0.U_Dis9
                WHEN 'ARISER SHIRT'         THEN T0.U_Dis7
                WHEN 'UATHAYAM MENS SET'    THEN T0.U_Dis11
                WHEN 'ARISER MENS TROUSERS' THEN T0.U_Dis10
                ELSE 0
            END AS DiscountPer,

            -- ── Addresses (JSON) ──────────────────────────────────────────────────
            (
                SELECT
                    T1.Address   AS BillShipID,
                    T1.AdresType AS Type,
                    T0.CardName  AS DisplayName,
                    CASE WHEN T1.AdresType = 'B' THEN 'OFFICE' ELSE 'SHIP' END AS LocationName,
                    (
                        SELECT
                            ISNULL(T1.Building,   '') AS Line1,
                            ISNULL(T1.Block,      '') AS Line2,
                            ISNULL(T1.Street, T1.City) AS Line3,
                            CASE WHEN T0.ShipToDef = T1.Address THEN 1 ELSE 0 END AS IsDefault,
                            ISNULL(T1.City,       '') AS City,
                            ISNULL(T1.County,     '') AS County,
                            ISNULL(T1.State,      '') AS [State],
                            ISNULL(T1.Country,    '') AS Country,
                            ISNULL(T1.ZipCode,    '') AS ZipCode,
                            ''                        AS PhoneNumber,
                            ISNULL(T1.U_LMobile,  '') AS MobileNumber,
                            ''                        AS Email,
                            ISNULL(T1.U_SHWGSTIN, '') AS GSTNo,
                            CASE WHEN T0.validFor = 'Y' THEN 1 ELSE 0 END AS IsActive,
                            ''                        AS GstStatus
                        FOR JSON PATH, WITHOUT_ARRAY_WRAPPER
                    ) AS Address
                FROM [BBLive].[dbo].CRD1 T1
                WHERE T1.CardCode = T0.CardCode
                FOR JSON PATH
            ) AS BillShipTo,

            -- ── Contact persons (JSON) ────────────────────────────────────────────
            (
                SELECT *
                FROM (
                    SELECT
                        CntctCode                                 AS ContactPersonID,
                        Name                                      AS ContactPersonName,
                        Position                                  AS Designation,
                        Cellolar                                  AS MobileNum,
                        Cellolar                                  AS WhatsAppNum,
                        E_MailL                                   AS EmailID,
                        CASE WHEN Active = 'Y' THEN 1 ELSE 0 END AS IsActive,
                        CAST(0 AS INT)                            AS IsSendOverDueReminder,
                        SB.DivisionCode                           AS DivisionCode,
                        CAST(0 AS INT) AS PaymentSMS,
                        CAST(0 AS INT) AS PaymentEmail,
                        CAST(1 AS INT) AS PaymentWhatsapp,
                        CAST(1 AS INT) AS OrderEmail,
                        CAST(1 AS INT) AS OrderSMS,
                        CAST(1 AS INT) AS OrderWhatsapp,
                        CAST(1 AS INT) AS InvoiceWhatsapp,
                        CAST(0 AS INT) AS InvoiceEmail,
                        CAST(0 AS INT) AS InvoiceSMS,
                        CAST(0 AS INT) AS PaymentRequestSMS,
                        CAST(0 AS INT) AS PaymentRequestEmail,
                        CAST(0 AS INT) AS PaymentrequestWhatsapp,
                        CAST(0 AS INT) AS OutstandingSMS,
                        CAST(0 AS INT) AS OutstandingEmail,
                        CAST(0 AS INT) AS OutstandingWhatsapp,
                        CAST(0 AS INT) AS PaycollectionWhatsapp,
                        CAST(0 AS INT) AS DistributorWhatsapp
                    FROM [BBLive].[dbo].OCPR
                    WHERE CardCode = T0.CardCode

                    UNION ALL

                    -- Fallback empty row if no contacts exist
                    SELECT
                        CAST(0  AS INT), CAST('' AS NVARCHAR(100)), CAST('' AS NVARCHAR(100)),
                        CAST('' AS NVARCHAR(50)), CAST('' AS NVARCHAR(50)), CAST('' AS NVARCHAR(100)),
                        CAST(1  AS INT), CAST(0  AS INT),
                        CAST(SB.DivisionCode AS NVARCHAR(20)),
                        CAST(0 AS INT), CAST(0 AS INT), CAST(0 AS INT),
                        CAST(0 AS INT), CAST(0 AS INT), CAST(0 AS INT),
                        CAST(0 AS INT), CAST(0 AS INT), CAST(0 AS INT),
                        CAST(0 AS INT), CAST(0 AS INT), CAST(0 AS INT),
                        CAST(0 AS INT), CAST(0 AS INT), CAST(0 AS INT),
                        CAST(0 AS INT), CAST(0 AS INT)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM [BBLive].[dbo].OCPR WHERE CardCode = T0.CardCode
                    )
                ) X
                FOR JSON PATH
            ) AS BPContactDetails,

            -- ── MST_MAP_BP_Division (JSON) ────────────────────────────────────────
            (
                SELECT
                    CAST(0  AS INT)             AS MapDivisionID,
                    CAST(0  AS INT)             AS AutoApprovalCreditLimit,
                    CAST(0.00 AS DECIMAL(18,2)) AS AutoApprovalCreditLimitBal,
                    CAST('' AS NVARCHAR(200))   AS BPRemarks,
                    CAST(0.00 AS DECIMAL(18,2)) AS CreditLimit,
                    CAST('' AS NVARCHAR(200))   AS Destination,
                    CAST(0  AS INT)             AS DiscountPer,
                    SB.DivisionCode             AS DivisionCode,
                    CAST(0.00 AS DECIMAL(18,2)) AS ExcessPer,
                    ISNULL(T0.U_Grade,        '') AS Grade,
                    CAST(1  AS INT)             AS IsActive,
                    CAST(0  AS INT)             AS IsOrderAutoApproval,
                    CAST(0  AS INT)             AS Outstandingdays,
                    ISNULL(T0.U_SalPriceCode, '') AS PriceLisCode,
                    CAST(0  AS INT)             AS ShowLimit,
                    CAST('' AS NVARCHAR(200))   AS TransporterName
                FOR JSON PATH
            ) AS MST_MAP_BP_Division,

            -- ── MST_MAP_BP_Brand (JSON) ───────────────────────────────────────────
            (
                SELECT
                    SB.Brand          AS Brand,
                    SB.DivisionCode   AS DivisionCode
                FOR JSON PATH
            ) AS MST_MAP_BP_Brand,

            -- ── MST_Map_BP_SubBrand (JSON) ────────────────────────────────────────
            (
                SELECT
                    SB.SubBrandName   AS SubBrandName,
                    SB.DivisionCode   AS DivisionCode
                FOR JSON PATH
            ) AS MST_Map_BP_SubBrand

        FROM [BBLive].[dbo].OCRD T0

        -- ── Cross join against all sub-brands so every customer gets every row ────
        CROSS JOIN (VALUES
            ('ARISER',   'ARISER',    'ARISER SHIRT'),
            ('ARISER',   'ARISER',    'ARISER MENS TROUSERS'),
            ('UATHAYAM', 'UATHAYAM',  'UATHAYAM DHOTIE'),
            ('UATHAYAM', 'UATHAYAM',  'UATHAYAM SHIRTING'),
            ('UATHAYAM', 'UATHAYAM',  'UATHAYAM RDY'),
            ('UATHAYAM', 'UATHAYAM',  'UATHAYAM RDY DHOTIE'),
            ('UATHAYAM', 'UATHAYAM',  'UATHAYAM HOS'),
            ('UATHAYAM', 'UATHAYAM',  'UATHAYAM KIDS SET'),
            ('UATHAYAM', 'UATHAYAM',  'UATHAYAM MENS SET'),
            ('EVERYDAY', 'EVERYDAY',  'EVERYDAY DHOTIE'),
            ('EVERYDAY', 'EVERYDAY',  'EVERYDAY SHIRTING'),
            ('EVERYDAY', 'EVERYDAY',  'EVERYDAY RDY'),
            ('ADD',      'ADD',       'ADD DHOTIE'),
            ('ADD',      'ADD',       'ADD SHIRTING'),
            ('ADD',      'ADD',       'ADD SHIRT')
        ) AS SB(DivisionCode, Brand, SubBrandName)

        WHERE T0.CardType='C' AND T0.validFor='Y' and T0.cardCode = 'C000020'

        ORDER BY T0.CardCode, SB.DivisionCode, SB.SubBrandName;
        `;

        // for (const { subBrandName, divisionCode } of BP_SUBBRAND_CONFIG) 
        {
            const result = await pool.request().query(query);
            allRows.push(...result.recordset);
        }

        return allRows;

    } catch (err) {
        console.log('❌ SQL Error (BP Master):', err);
        throw err;
    }
}

module.exports = {
    getProductData,
    getPriceListData,
    getImageData,
    getSchemeData,
    getBPMasterData 
};