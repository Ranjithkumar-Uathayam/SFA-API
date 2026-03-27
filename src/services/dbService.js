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
        WHERE t0.validFor='Y'
            AND t0.UpdateDate >= @lastSyncDate
            AND t0.U_SubGrp1 NOT IN (
                'ACCESSORIES','ADVERTISEMENT','ALL','SAMPLE','PRINTING & STATIONERY',
                'IMPERIAL COMPUTERS','PACKING MATERIAL','REPAIRS & MAINTENANCE',
                'SALES PROMOTION EXPENSES','EVERYDAY DHOTIE','ALLDAYS DHOTIE',
                'ADD DHOTIE','ADD SHIRT','EVERYDAY SHIRTING','EVERYDAY RDY'
            )
            AND t0.U_SubGrp1 = 'UATHAYAM SUITING'
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
                B.LineId										AS PriceID,
                CASE WHEN B.U_Lock = 'Y' THEN 0 ELSE 1 END     AS PriceIsActive
            FROM [BBLive].[dbo].OITM t0
            LEFT JOIN (
                SELECT
                    T0.DocEntry, T2.U_Brand, T1.U_ItemCode,
                    T0.U_State, T0.U_SelPrice, T0.U_MRP, T2.U_Lock, T0.LineId
                FROM [BBLive].[dbo].[@INS_PLM2] T0
                INNER JOIN [BBLive].[dbo].[@INS_OPLM] T1 ON T0.DocEntry = T1.DocEntry
                INNER JOIN [BBLive].[dbo].[@INS_PLM1] T2 ON T0.DocEntry = T2.DocEntry
            ) B ON B.U_ItemCode = t0.ItemCode
            WHERE B.U_SelPrice > 0
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
                t0.itemcode  AS skuCode,
                T0.U_SubGrp6 AS ColorCode,
                ''           AS fileName,
                ''           AS Description,
                ''           AS base64Data
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
            CAST(CONCAT(T0.[Object], T0.DocNum) AS NVARCHAR(50)) AS PolicyNumber,
            1 AS Revision,
            T0.DocEntry AS PolicyID,
            T0.Remark AS PolicyName,
            case when T1.U_Discunt='Quantity' then 'SC' WHEN T1.U_Discunt='Percentage' THEN 'DIS' END AS SavingType,
            T1.U_Discunt AS DiscountBasis,
            'P' AS Applicability,
            1 AS IsCustomerDefined,
            1 AS IsActive,
            CASE 
                WHEN T1.U_bran= 'UATHAYAM DHOTIE' THEN 'UATHAYAM'
                WHEN T1.U_bran='UATHAYAM SHIRTING' THEN 'UATHAYAM'
                WHEN T1.U_bran= 'UATHAYAM RDY'  THEN 'UATHAYAM'
                WHEN T1.U_bran= 'UATHAYAM HOS' THEN 'UATHAYAM'                   
                WHEN T1.U_bran='UATHAYAM KIDS SET' THEN 'UATHAYAM'
                WHEN T1.U_bran='UATHAYAM MENS SET' THEN 'UATHAYAM'
                WHEN T1.U_bran='ARISER SHIRT'   THEN 'ARISER'
                WHEN T1.U_bran='ARISER MENS TROUSERS' THEN 'ARISER'
                WHEN T1.U_bran='ARISER HOS' THEN 'ARISER'
                END	
        AS DivisionCode,
            GETDATE() AS FromDate,
            T0.U_ToDt AS ToDate,
            0 AS AllowDiscountForAllProducts,
            NULL AS DiscountPer,
    
            -- ── BP Category ───────────────────────────────────────────────────────
            (
                SELECT 'DEALER' AS BPCategory
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS SC_BpCategoryMapping,
    
            -- ── State Mapping ─────────────────────────────────────────────────────
            (
                SELECT DISTINCT L.U_Stat AS StateCode
                FROM [BBLive].[dbo]."@SCHEML" L
                WHERE L.DocEntry = T0.DocEntry AND L.U_Stat IS NOT NULL
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS StateMapping,
    
            -- ── Role Mapping ──────────────────────────────────────────────────────
            (
                SELECT 'DEALER' AS Role
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS RoleMapping,
    
            -- ── BP Exclusion ──────────────────────────────────────────────────────
            (
                SELECT NULL AS BPCode
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS SC_BpExclution,
    
            -- ── BP Inclusion ──────────────────────────────────────────────────────
            (
                SELECT DISTINCT C.CardCode AS BPCode
                FROM [BBLive].[dbo].OCRD C
                INNER JOIN [BBLive].[dbo].CRD1 D ON C.CardCode = D.CardCode AND D.AdresType = 'B'
                WHERE D.State IN (
                    SELECT DISTINCT L.U_Stat
                    FROM [BBLive].[dbo]."@SCHEML" L
                    WHERE L.DocEntry = T0.DocEntry
                )
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS SC_BpInclution,
    
            -- ── Product Mapping with nested SC_ProdAlternate per product ──────────
            (
                SELECT
                    I.ItemCode                          AS ProductCode,
                    I.U_Size                            AS SizeCode,
                    I.U_SubGrp6                         AS ColorCode,
                    CAST(L.U_BillsQty AS DECIMAL(10,2)) AS MinOrderQty,
                    CAST(L.U_OffersQty  AS DECIMAL(10,2)) AS FreeQty,
                    'S'                                 AS Applicability,
                    L.U_OffersQty                       AS AllowMultiplyFreeQty,
                    CAST(L.U_BillsQty AS DECIMAL(10,2)) AS MaxAllowedFreeQty,
                    1                                   AS IsActive,
                    1                                   AS MappingStatus,
                    -- ── Nested SC_ProdAlternate — alternate items for this product ─
                    (
                        SELECT
                            ALT.ItemCode  AS ProductCode,
                            ALT.U_Size    AS SizeCode,
                            ALT.U_SubGrp6 AS ColorCode,
                            0             AS IsActive
                        FROM [BBLive].[dbo].OITM ALT
                        WHERE ALT.ItemCode = I.ItemCode   -- alternate linked to parent item
                        AND ALT.validFor  = 'Y'           -- only active alternates
                        FOR JSON PATH, INCLUDE_NULL_VALUES
                    ) AS SC_ProdAlternate
                FROM [BBLive].[dbo]."@SCHEML" L
                INNER JOIN [BBLive].[dbo].OITM I ON I.U_SubGrp7 = L.U_Qual
                WHERE L.DocEntry = T0.DocEntry
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS SC_ProductMapping,
    
            -- ── Product Group Mapping ─────────────────────────────────────────────
            (
                SELECT
                    NULL AS GroupCode,
                    NULL AS StyleCode,
                    NULL AS MinOrderQty,
                    NULL AS FreeQty,
                    NULL AS Applicability,
                    0    AS AllowMultiplyFreeQty,
                    NULL AS MaxAllowedFreeQty,
                    NULL AS GroupName,
                    0    AS IsActive,
                    0    AS MappingStatus
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS SC_ProdGroupMapping,
    
            -- ── Top-level SC_ProdAlternate (standalone, outside ProductMapping) ───
            (
                SELECT
                    NULL AS ProductCode,
                    NULL AS SizeCode,
                    NULL AS ColorCode,
                    0    AS IsActive
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS SC_ProdAlternate,
    
            -- ── Product Group Alternate ───────────────────────────────────────────
            (
                SELECT
                    NULL AS GroupName,
                    NULL AS StyleCode,
                    0    AS IsActive
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS SC_ProdGroupAlternate,
    
            -- ── Brand Discount ────────────────────────────────────────────────────
            (
                SELECT
                    NULL AS Brand,
                    NULL AS DiscountType,
                    NULL AS DiscountVal,
                    0    AS IsActive
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS SC_Brand_Discount,
    
            -- ── Product Group Direct Discount ─────────────────────────────────────
            (
                SELECT
                    NULL AS DivisionCode,
                    NULL AS GroupCode,
                    NULL AS GroupName,
                    NULL AS StyleCode,
                    NULL AS StyleName,
                    NULL AS DiscountType,
                    NULL AS DiscountVal,
                    0    AS IsActive
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS SC_ProdGroupDirectDiscount,
    
            -- ── Product Direct Discount ───────────────────────────────────────────
            (
                SELECT
                    NULL AS ProductCode,
                    NULL AS SizeCode,
                    NULL AS ColorCode,
                    NULL AS DiscountType,
                    NULL AS DiscountVal,
                    0    AS IsActive
                FOR JSON PATH, INCLUDE_NULL_VALUES
            ) AS SC_ProductDirectDiscount
    
        FROM [BBLive].[dbo]."@SCHEM" T0
        INNER JOIN [BBLive].[dbo]."@SCHEML" T1 ON T0.DocEntry = T1.DocEntry
        WHERE T0.U_FrmDt >= '20250701'
        AND T0.U_ToDt  <= '20260430'
        `;

        const { recordset } = await pool.request().query(query);
        return recordset;

    } catch (err) {
        console.error('❌ SQL Error (Schemes):', err);
        throw err;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BP MASTER
//
// [@INS_BPDIV] does not exist in this database — removed that JOIN.
// MST_MAP_BP_Division is built from T0 columns only (Grade, PriceLisCode)
// with the remaining fields defaulted to 0 / ''.
//
// All other fixes retained:
//   • BillShipID  = T1.LineNum  (integer)
//   • PhoneNumber = T1.Tel1
//   • BPGroupCode = '' (hardcoded)
//   • MST_MAP_BP_Brand.Brand = SB.SubBrandName
//   • BillShipTo T0 refs use scalar subqueries (CROSS JOIN binding fix)
// ─────────────────────────────────────────────────────────────────────────────
async function getBPMasterData() {
    try {
        const pool = await getPool();

       const query = `
            WITH SubBrandMap AS (
                SELECT *
                FROM (VALUES
                    ('ARISER',   'ARISER SHIRT',           'U_Dis7'),
                    ('ARISER',   'ARISER HOS',             'U_Dis3'),
                    ('ARISER',   'ARISER MENS TROUSERS',   'U_Dis10'),
                    ('UATHAYAM', 'UATHAYAM DHOTIE',        'U_Dis1'),
                    ('UATHAYAM', 'UATHAYAM SHIRTING',      'U_Dis1'),
                    ('UATHAYAM', 'UATHAYAM RDY',           'U_Dis2'),
                    ('UATHAYAM', 'UATHAYAM HOS',           'U_Dis3'),
                    ('UATHAYAM', 'UATHAYAM KIDS SET',      'U_Dis9'),
                    ('UATHAYAM', 'UATHAYAM MENS SET',      'U_Dis11')
                ) AS X(DivisionCode, SubBrandName, DiscountColumn)
            )

            SELECT
                T0.CardCode   AS BPCode,
                T0.CardName   AS BPName,
                T0.Currency   AS DefaultCurrency,
                CASE WHEN T0.validFor = 'Y' THEN 1 ELSE 0 END AS IsActive,
                0 AS AllowCreditLimit,
                T0.CardFName AS DisplayName,
                CASE WHEN T0.GroupCode IN ('100','106') THEN 'Dealer' ELSE '' END AS BPCategory,
                '' AS BPGroupCode,
                T0.U_showcode AS SR_BPCode,

                CASE 
                    WHEN ISNULL(T0.U_Grade, '') IN ('', '-') THEN 'C'
                    ELSE REPLACE(T0.U_Grade, 'Grade', '')
                END AS GradeOfBP,

                '' AS CustomerRemark,
                CAST(0 AS DECIMAL(18,2)) AS Latitude,
                CAST(0 AS DECIMAL(18,2)) AS Longitude,
                T0.U_AreaCode AS AreaCode,

                SB.DivisionCode,
                SB.SubBrandName AS Brand,
                SB.SubBrandName,

                -- 🔹 Dynamic Discount Mapping (NO CASE REPEAT)
                CAST(
                    CASE SB.DiscountColumn
                        WHEN 'U_Dis1'  THEN ISNULL(T0.U_Dis1,0)
                        WHEN 'U_Dis2'  THEN ISNULL(T0.U_Dis2,0)
                        WHEN 'U_Dis3'  THEN ISNULL(T0.U_Dis3,0)
                        WHEN 'U_Dis4'  THEN ISNULL(T0.U_Dis4,0)
                        WHEN 'U_Dis5'  THEN ISNULL(T0.U_Dis5,0)
                        WHEN 'U_Dis6'  THEN ISNULL(T0.U_Dis6,0)
                        WHEN 'U_Dis7'  THEN ISNULL(T0.U_Dis7,0)
                        WHEN 'U_Dis8'  THEN ISNULL(T0.U_Dis8,0)
                        WHEN 'U_Dis9'  THEN ISNULL(T0.U_Dis9,0)
                        WHEN 'U_Dis10' THEN ISNULL(T0.U_Dis10,0)
                        WHEN 'U_Dis11' THEN ISNULL(T0.U_Dis11,0)
                        ELSE 0
                    END
                AS DECIMAL(18,6)) AS DiscountPer,

                ------------------------------------------------------------------
                -- BILL / SHIP
                ------------------------------------------------------------------
                (
                    SELECT
                        (C.CntctCode + T1.LineNum) AS BillShipID,
                        T1.AdresType AS Type,
                        T0.CardName AS DisplayName,
                        CASE WHEN T1.AdresType = 'B' THEN 'OFFICE' ELSE 'SHIP' END AS LocationName,
                        ISNULL(NULLIF(CAST(T1.Building AS NVARCHAR(MAX)),''), T1.City) AS Line1,
                        ISNULL(NULLIF(CAST(T1.Block AS NVARCHAR(MAX)),''), T1.City) AS Line2,
                        ISNULL(NULLIF(CAST(T1.Street AS NVARCHAR(MAX)),''), T1.City) AS Line3,
                        CASE WHEN T0.ShipToDef = T1.Address THEN 1 ELSE 0 END AS IsDefault,
                        ISNULL(T1.City,'') AS City,
                        ISNULL(T1.County, T1.Country) AS County,
                        ISNULL(T1.State,'') AS State,
                        ISNULL(T1.Country,'') AS Country,
                        ISNULL(T1.ZipCode,'') AS ZipCode,
                        RIGHT(ISNULL(T0.Phone1, T0.Phone2), 10) AS PhoneNumber,
                        RIGHT(ISNULL(T0.Phone2, T0.Cellular), 10) AS MobileNumber,
                        ISNULL(T0.E_Mail,'') AS Email,
                        ISNULL(T0.U_GSTIN,'') AS GSTNo,
                        CASE WHEN T0.validFor = 'Y' THEN 1 ELSE 0 END AS IsActive,
                        '' AS GstStatus
                    FROM [BBLive].[dbo].CRD1 T1
                    LEFT JOIN [BBLive].[dbo].OCPR C ON C.CardCode = T1.CardCode
                    WHERE T1.CardCode = T0.CardCode
                    FOR JSON PATH
                ) AS BillShipTo,
                 -- ── MST_MAP_BP_Division (FOR JSON) ────────────────────────────────────────
                (
                    SELECT
                        ISNULL(T0.UpdateTS, '')             AS MapDivisionID,
                        CAST(0 AS DECIMAL(18,2))            AS AutoApprovalCreditLimit,
                        CAST(0 AS DECIMAL(18,2))            AS AutoApprovalCreditLimitBal,
                        CAST('' AS NVARCHAR(200))           AS BPRemarks,
                        CAST(0 AS DECIMAL(18,2))            AS CreditLimit,
                        ISNULL(T0.City, '')                 AS Destination,
                        CAST(0 AS DECIMAL(18,2))            AS DiscountPer,
                        SB2.DivisionCode                    AS DivisionCode,
                        CAST(0 AS DECIMAL(18,2))            AS ExcessPer,
                        REPLACE(T0.U_Grade,'Grade ','')     AS Grade,
                        CAST(1 AS INT)                      AS IsActive,
                        CAST(0 AS INT)                      AS IsOrderAutoApproval,
                        CAST(0 AS INT)                      AS Outstandingdays,
                        ISNULL(T0.U_SalPriceCode, '')       AS PriceLisCode,
                        CAST(0 AS INT)                      AS ShowLimit,
                        CAST('Uathayam' AS NVARCHAR(200))   AS TransporterName

                    FROM (
                        SELECT DISTINCT DivisionCode 
                        FROM SubBrandMap
                    ) SB2 

                    FOR JSON PATH
                ) AS MST_MAP_BP_Division,
                ------------------------------------------------------------------
                -- CONTACTS
                ------------------------------------------------------------------
                (
                    SELECT
                        CntctCode                                  AS ContactPersonID,
                        Name                                       AS ContactPersonName,
                        ISNULL(Position, 'proprietor')             AS Designation,
                        RIGHT(ISNULL(Cellolar, T0.Cellular), 10)              AS MobileNum,
                        RIGHT(ISNULL(Cellolar, T0.Cellular), 10)              AS WhatsAppNum,
                        E_MailL                                    AS EmailID,
                        CASE WHEN Active = 'Y' THEN 1 ELSE 0 END  AS IsActive,
                        CAST(0 AS INT)                             AS IsSendOverDueReminder,
                        SB.DivisionCode                            AS DivisionCode,
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
                    FOR JSON PATH
                ) AS Map_BpContactDetails,

                ------------------------------------------------------------------
                -- BRAND JSON MST_MAP_BP_Brand (FOR JSON)
                ------------------------------------------------------------------
                (
                    SELECT
                        SB2.SubBrandName AS Brand,
                        SB2.DivisionCode AS DivisionCode
                    FROM SubBrandMap SB2  
                    FOR JSON PATH
                ) AS MST_MAP_BP_Brand,  
                ------------------------------------------------------------------
                -- SUB BRAND JSON
                ------------------------------------------------------------------
                (
                    SELECT
                        SB2.SubBrandName,
                        SB2.DivisionCode,
                        CAST(
                            CASE SB2.DiscountColumn
                                WHEN 'U_Dis1'  THEN ISNULL(T0.U_Dis1,0)
                                WHEN 'U_Dis2'  THEN ISNULL(T0.U_Dis2,0)
                                WHEN 'U_Dis3'  THEN ISNULL(T0.U_Dis3,0)
                                WHEN 'U_Dis4'  THEN ISNULL(T0.U_Dis4,0)
                                WHEN 'U_Dis5'  THEN ISNULL(T0.U_Dis5,0)
                                WHEN 'U_Dis6'  THEN ISNULL(T0.U_Dis6,0)
                                WHEN 'U_Dis7'  THEN ISNULL(T0.U_Dis7,0)
                                WHEN 'U_Dis8'  THEN ISNULL(T0.U_Dis8,0)
                                WHEN 'U_Dis9'  THEN ISNULL(T0.U_Dis9,0)
                                WHEN 'U_Dis10' THEN ISNULL(T0.U_Dis10,0)
                                WHEN 'U_Dis11' THEN ISNULL(T0.U_Dis11,0)
                                ELSE 0
                            END
                        AS DECIMAL(18,6)) AS DiscountPer
                    FROM SubBrandMap SB2
                    FOR JSON PATH
                ) AS MST_Map_BP_SubBrand,

                ------------------------------------------------------------------
                -- DISCOUNT JSON
                ------------------------------------------------------------------
                (
                    SELECT
                        'TRADE DISCOUNT' AS DiscountName,
                        SB3.DivisionCode,
                        SB3.SubBrandName AS Brand,
                        CAST(
                            CASE SB3.DiscountColumn
                                WHEN 'U_Dis1'  THEN ISNULL(T0.U_Dis1,0)
                                WHEN 'U_Dis2'  THEN ISNULL(T0.U_Dis2,0)
                                WHEN 'U_Dis3'  THEN ISNULL(T0.U_Dis3,0)
                                WHEN 'U_Dis4'  THEN ISNULL(T0.U_Dis4,0)
                                WHEN 'U_Dis5'  THEN ISNULL(T0.U_Dis5,0)
                                WHEN 'U_Dis6'  THEN ISNULL(T0.U_Dis6,0)
                                WHEN 'U_Dis7'  THEN ISNULL(T0.U_Dis7,0)
                                WHEN 'U_Dis8'  THEN ISNULL(T0.U_Dis8,0)
                                WHEN 'U_Dis9'  THEN ISNULL(T0.U_Dis9,0)
                                WHEN 'U_Dis10' THEN ISNULL(T0.U_Dis10,0)
                                WHEN 'U_Dis11' THEN ISNULL(T0.U_Dis11,0)
                                ELSE 0
                            END
                        AS DECIMAL(18,6)) AS DiscountPer,
                        CONVERT(VARCHAR(19), CAST('2019-04-01' AS DATETIME), 126) AS FromDate,
                        CONVERT(VARCHAR(19), CAST('2030-03-31' AS DATETIME), 126) AS ToDate
                    FROM SubBrandMap SB3
                    FOR JSON PATH
                ) AS Discount_BP_Division

            FROM [BBLive].[dbo].OCRD T0
            CROSS JOIN SubBrandMap SB

            WHERE T0.CardType = 'C'
            AND T0.validFor = 'Y'
            AND T0.U_AreaCode != ''
            AND ISNULL(CAST(T0.U_GSTIN AS NVARCHAR(MAX)), '') NOT IN ('UNREGISTERED','')

            ORDER BY T0.CardCode, SB.DivisionCode, SB.SubBrandName`

        const result = await pool.request().query(query);
        return result.recordset;

    } catch (err) {
        console.log('❌ SQL Error (BP Master):', err);
        throw err;
    }
}

async function getStockData() {
    try {
        const pool = await getPool();

        const query = `
            SELECT
                t0.DocEntry                                                        AS ExternalId,
                t0.DocEntry                                                        AS ProductMappingId,
                t0.ItemCode                                                 AS ProductCode,
                t0.U_SubGrp13                                                AS ColorCode,
                t0.U_SubGrp7                                                AS AttributeValue,
                t0.U_SubGrp4                                                AS StyleCode,
                t0.U_Size                                                   AS Size,
                CAST(t1.OnHand AS INT)                                      AS StockQuantity,
                'Stock'                                                     AS Type,
                CASE WHEN t0.ValidFor = 'Y' THEN CAST(1 AS BIT)
                    ELSE CAST(0 AS BIT) END                                AS IsActive,
                CASE WHEN t0.MinLevel > t1.OnHand THEN 'High Stock'
                    ELSE 'Low Stock' END                                   AS StockHighlightMessageDetails,
                CASE WHEN t0.MinLevel > t1.OnHand THEN 'Stock Available'
                    ELSE 'Very few stock left' END                         AS StockMessage
            FROM [BBLive].[dbo].OITM AS t0
            INNER JOIN [BBLive].[dbo].OITW AS t1 ON t0.ItemCode = t1.ItemCode
            WHERE t1.WhsCode = 'ASRS' 
            and T0.ValidFor = 'Y'
            ORDER BY t0.ItemCode
        `;

        const { recordset } = await pool.request().query(query);
        return recordset;

    } catch (err) {
        console.error('❌ SQL Error (Stock):', err);
        throw err;
    }
}

module.exports = {
    getProductData,
    getPriceListData,
    getImageData,
    getSchemeData,
    getBPMasterData,
    getStockData
};