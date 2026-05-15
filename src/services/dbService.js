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
        
        -- ✅ Filter: only items that exist in @INS_OPLM with SelPrice > 0
        WHERE EXISTS (
            SELECT 1
            FROM [BBLive].[dbo]."@INS_OPLM" plm
            INNER JOIN [BBLive].[dbo]."@INS_PLM2" plm2 ON plm2.DocEntry = plm.DocEntry
            WHERE plm.U_ItemCode = t0.ItemCode
            AND plm2.U_SelPrice > 0
        )
        
        AND t0.validFor = 'Y'
        -- AND t0.UpdateDate >= @lastSyncDate
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
            '2026-05-20T00:00:00' AS FromDate,
            '2026-07-01T00:00:00' AS ToDate,
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
        AND T0.U_ToDt  <= '20260531'
        `;

        const { recordset } = await pool.request().query(query);
        return recordset;

    } catch (err) {
        console.error('❌ SQL Error (Schemes):', err);
        throw err;
    }
}

// BP MASTER
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
                        (ISNULL(T0.DocEntry,1) + T1.LineNum) AS BillShipID,
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
                        CASE 
                            WHEN LEN(RIGHT(ISNULL(T0.Phone1,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Phone1,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Phone1,10)

                            WHEN LEN(RIGHT(ISNULL(T0.Phone2,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Phone2,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Phone2,10)

                            WHEN LEN(RIGHT(ISNULL(T0.Cellular,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Cellular,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Cellular,10)

                            ELSE ''
                        END AS PhoneNumber,

                        CASE 
                            WHEN LEN(RIGHT(ISNULL(T0.Phone1,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Phone1,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Phone1,10)

                            WHEN LEN(RIGHT(ISNULL(T0.Phone2,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Phone2,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Phone2,10)

                            WHEN LEN(RIGHT(ISNULL(T0.Cellular,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Cellular,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Cellular,10)

                            ELSE ''
                        END AS MobileNumber,
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
                         CASE 
                            WHEN LEN(RIGHT(ISNULL(T0.Phone1,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Phone1,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Phone1,10)

                            WHEN LEN(RIGHT(ISNULL(T0.Phone2,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Phone2,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Phone2,10)

                            WHEN LEN(RIGHT(ISNULL(T0.Cellular,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Cellular,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Cellular,10)

                            ELSE ''
                        END AS MobileNum,
                         CASE 
                            WHEN LEN(RIGHT(ISNULL(T0.Phone1,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Phone1,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Phone1,10)

                            WHEN LEN(RIGHT(ISNULL(T0.Phone2,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Phone2,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Phone2,10)

                            WHEN LEN(RIGHT(ISNULL(T0.Cellular,''),10)) = 10 
                                AND RIGHT(ISNULL(T0.Cellular,''),10) NOT LIKE '%[^0-9]%'
                            THEN RIGHT(T0.Cellular,10)

                            ELSE ''
                        END AS WhatsAppNum,
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
            AND T0.CardCode in ('C035580')
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
                CASE WHEN CAST(t0.MinLevel AS INT)  > CAST(t1.OnHand AS INT) THEN 'Very few stock left'
                    ELSE 'Stock Available' END                         AS StockMessage
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

async function getOutstandingData() {
    try {
        const pool = await getPool();

        const query = `
            SELECT
                CASE
                    WHEN tk.U_Brand LIKE '%UATHAYAM%' THEN 'UATHAYAM'
                    ELSE 'ARISER'
                END AS DivisionCode,
                CAST(tk.DocEntry AS NVARCHAR(20)) AS DocEntry,
                CASE
                    WHEN tk.U_Brand LIKE '%UATHAYAM%' THEN 'UATHAYAM'
                    ELSE 'ARISER'
                END AS DivisionName,
                tk.U_Brand AS Brand,
                CASE
                    WHEN k.TransType = '13' THEN 'AR Invoice'
                    WHEN k.TransType = '30' THEN 'Journal'
                    ELSE NULL
                END AS DocType,
                CASE
                    WHEN k.Ref1 NOT LIKE '%[^0-9]%' AND k.Ref1 IS NOT NULL
                    THEN CAST(k.Ref1 AS INT)
                    ELSE NULL
                END AS InvoiceNo,
                tk.DocDate AS InvoiceDate,
                k.duedat AS DueDate,
                k.CardCode AS CardCode,
                k.CardName AS CardName,
                lk.City AS City,
                lk.State AS STATE,
                k.memo AS DocumentRemarks,
                CASE
                    WHEN k.BalDueDeb > 0 THEN DATEDIFF(d, k.duedat, GETDATE())
                    ELSE 0
                END AS OverdueDays,
                GETDATE() AS OverdueDate,
                CASE
                    WHEN k.TransType = '30' THEN k.Debit
                    ELSE ISNULL(tk.DocTotal, 0)
                END AS DocumentValue,
                k.BalDueDeb AS BalanceToBePaid,
                CAST(0 AS BIT) AS BatchEnd
            FROM (
                SELECT
                    T2.CardCode,
                    T2.CardName,
                    T0.RefDate,
                    T1.BaseRef,
                    T1.Debit,
                    T1.Credit,
                    T0.TransId,
                    T1.BalDueDeb,
                    T1.LineMemo,
                    T2.MailCity,
                    T3.GroupName,
                    T1.Ref1,
                    CASE
                        WHEN T1.TransType IN ('-2', '30') THEN T1.DueDate
                        ELSE T0.RefDate
                    END AS duedat,
                    T1.OcrCode3 AS brand,
                    T0.memo,
                    T0.TransType
                FROM [BBLive].[dbo].OJDT T0
                INNER JOIN [BBLive].[dbo].JDT1 T1 ON T0.TransId = T1.TransId
                INNER JOIN [BBLive].[dbo].OCRD T2 ON T2.CardCode = T1.ShortName
                INNER JOIN [BBLive].[dbo].OCRG T3 ON T2.GroupCode = T3.GroupCode
                WHERE T1.BalDueDeb <> '0'
                  AND T2.CardType = 'C'
            ) k
            LEFT JOIN (
                SELECT
                    tl.DocNum,
                    tl.DocEntry,
                    tl.DocDate,
                    tl.DocTotal,
                    tl.CardCode,
                    tl.U_Brand,
                    tg.U_Remarks,
                    tl.TransId
                FROM [BBLive].[dbo].OINV tl
                LEFT JOIN [BBLive].[dbo].[@INCM_BND1] tg ON tg.U_Name = RTRIM(tl.U_Brand)
            ) tk ON CONVERT(NVARCHAR(20), tk.DocNum) = k.Ref1
                 AND tk.CardCode = k.CardCode
                 AND tk.TransId = k.TransId
            LEFT JOIN (
                SELECT
                    cr1.CardCode,
                    cr1.CardName,
                    cr1.U_AreaCode AS agent,
                    cr2.City,
                    cr2.State
                FROM [BBLive].[dbo].OCRD cr1
                LEFT JOIN [BBLive].[dbo].CRD1 cr2 ON cr2.CardCode = cr1.CardCode
                                   AND cr2.AdresType = 'B'
            ) lk ON lk.CardCode = k.CardCode
        `;

        const { recordset } = await pool.request().query(query);
        return recordset;

    } catch (err) {
        console.error('SQL Error (Outstanding):', err);
        throw err;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Paginated product list with push status joined from [SFA_RecordPushStatus]
// opts: { page, limit, search, pushStatus, division }
// Returns: { data, total, page, limit, totalPages, summary }
// ─────────────────────────────────────────────────────────────────────────────
async function getProductsPaged({ page = 1, limit = 50, search, pushStatus, division } = {}) {
    const pool   = await getPool();
    const offset = (page - 1) * limit;

    const searchVal   = search     ? `%${search}%` : null;
    const divVal      = division   || null;
    const statusVal   = pushStatus || null;

    const dataQuery = `
        SELECT
            COUNT(*) OVER() AS TotalCount,
            t0.ItemCode                                                     AS ProductCode,
            t0.ItemName                                                     AS ProductName,
            CASE WHEN t0.validFor = 'Y' THEN 1 ELSE 0 END                  AS ProductIsActive,
            t0.U_SubGrp1                                                    AS Brand,
            t0.U_SubGrp3                                                    AS CategoryName,
            t0.U_SubGrp4                                                    AS StyleCode,
            RTRIM(t0.U_SubGrp5)                                             AS SizeCode,
            RTRIM(t0.U_SubGrp6)                                             AS ColorCode,
            ISNULL(t0.U_SubGrp11, t0.U_SubGrp6)                            AS ColorName,
            t0.SalPackMsr                                                   AS UOM,
            t0.U_HSNCODE                                                    AS HSNCode,
            t0.U_taxrate                                                    AS TaxBelow2500,
            t0.U_taxrate1000                                                AS TaxAbove2500,
            CASE
                WHEN t0.U_SubGrp1 LIKE '%ARISER%'   THEN 'ARISER'
                WHEN t0.U_SubGrp1 LIKE '%UATHAYAM%' THEN 'UATHAYAM'
            END                                                             AS DivisionCode,
            ISNULL(ps.PushStatus, 'Pending')                               AS PushStatus,
            ps.LastPushedAt,
            ps.ErrorMessage                                                 AS PushError,
            ps.UpdatedAt                                                    AS StatusUpdatedAt
        FROM [BBLive].[dbo].oitm t0
        JOIN [BBLive].[dbo].oitb t1 ON t0.ItmsGrpCod = t1.ItmsGrpCod
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] ps
            ON ps.MasterType = 'products' AND ps.RecordKey = t0.ItemCode
        WHERE EXISTS (
            SELECT 1 FROM [BBLive].[dbo]."@INS_OPLM" plm
            INNER JOIN [BBLive].[dbo]."@INS_PLM2" plm2 ON plm2.DocEntry = plm.DocEntry
            WHERE plm.U_ItemCode = t0.ItemCode AND plm2.U_SelPrice > 0
        )
        AND t0.validFor = 'Y'
        AND t0.U_SubGrp1 NOT IN (
            'ACCESSORIES','ADVERTISEMENT','ALL','SAMPLE','PRINTING & STATIONERY',
            'IMPERIAL COMPUTERS','PACKING MATERIAL','REPAIRS & MAINTENANCE',
            'SALES PROMOTION EXPENSES','EVERYDAY DHOTIE','ALLDAYS DHOTIE',
            'ADD DHOTIE','ADD SHIRT','EVERYDAY SHIRTING','EVERYDAY RDY'
        )
        AND (@search    IS NULL OR t0.ItemCode LIKE @search OR t0.ItemName LIKE @search)
        AND (@division  IS NULL OR (
                (@division = 'ARISER'   AND t0.U_SubGrp1 LIKE '%ARISER%'  ) OR
                (@division = 'UATHAYAM' AND t0.U_SubGrp1 LIKE '%UATHAYAM%')
        ))
        AND (@pushStatus IS NULL OR ISNULL(ps.PushStatus, 'Pending') = @pushStatus)
        ORDER BY t0.ItemCode
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `;

    const summaryQuery = `
        SELECT ISNULL(ps.PushStatus, 'Pending') AS PushStatus, COUNT(*) AS Count
        FROM [BBLive].[dbo].oitm t0
        JOIN [BBLive].[dbo].oitb t1 ON t0.ItmsGrpCod = t1.ItmsGrpCod
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] ps
            ON ps.MasterType = 'products' AND ps.RecordKey = t0.ItemCode
        WHERE EXISTS (
            SELECT 1 FROM [BBLive].[dbo]."@INS_OPLM" plm
            INNER JOIN [BBLive].[dbo]."@INS_PLM2" plm2 ON plm2.DocEntry = plm.DocEntry
            WHERE plm.U_ItemCode = t0.ItemCode AND plm2.U_SelPrice > 0
        )
        AND t0.validFor = 'Y'
        AND t0.U_SubGrp1 NOT IN (
            'ACCESSORIES','ADVERTISEMENT','ALL','SAMPLE','PRINTING & STATIONERY',
            'IMPERIAL COMPUTERS','PACKING MATERIAL','REPAIRS & MAINTENANCE',
            'SALES PROMOTION EXPENSES','EVERYDAY DHOTIE','ALLDAYS DHOTIE',
            'ADD DHOTIE','ADD SHIRT','EVERYDAY SHIRTING','EVERYDAY RDY'
        )
        GROUP BY ISNULL(ps.PushStatus, 'Pending')
    `;

    const [dataRes, summaryRes] = await Promise.all([
        pool.request()
            .input('search',     sql.NVarChar(200), searchVal)
            .input('division',   sql.NVarChar(50),  divVal)
            .input('pushStatus', sql.NVarChar(20),  statusVal)
            .input('offset',     sql.Int,           offset)
            .input('limit',      sql.Int,           limit)
            .query(dataQuery),
        pool.request().query(summaryQuery)
    ]);

    const records = dataRes.recordset;
    const total   = records.length > 0 ? records[0].TotalCount : 0;

    // Build summary map
    const summaryMap = { Pending: 0, Pushing: 0, Pushed: 0, Failed: 0 };
    for (const row of summaryRes.recordset) {
        summaryMap[row.PushStatus] = row.Count;
    }
    // Records with no push status row count as Pending
    const trackedTotal = Object.values(summaryMap).reduce((a, b) => a + b, 0);

    return {
        data:       records.map(({ TotalCount, ...rest }) => rest),
        total,
        page,
        limit,
        totalPages: Math.ceil(total / limit),
        summary:    summaryMap,
    };
}

// ─────────────────────────────────────────────────────────────────────────────
// Fetch full product rows for specific product codes (used before SF push)
// ─────────────────────────────────────────────────────────────────────────────
async function getProductDataByCodes(productCodes) {
    if (!productCodes || productCodes.length === 0) return [];
    const pool = await getPool();

    const req          = pool.request();
    const placeholders = productCodes.map((code, i) => {
        req.input(`c${i}`, sql.NVarChar(50), code);
        return `@c${i}`;
    }).join(',');

    const query = `
        SELECT
            t0.ItemCode AS ProductCode,
            t0.ItemName AS ProductName,
            CASE WHEN t0.validFor='Y' THEN 1 ELSE 0 END AS ProductIsActive,
            t0.U_SubGrp7  AS ProductGroupCode,
            t0.U_SubGrp7  AS ShortDesc,
            t0.ItemName   AS DetailedDesc,
            t0.U_SubGrp3  AS CategoryName,
            t0.U_SubGrp4  AS StyleCode,
            RTRIM(t0.U_SubGrp5) AS SizeCode,
            CASE
                WHEN t0.U_SubGrp1 LIKE '%ARISER%'   THEN 'ARISER'
                WHEN t0.U_SubGrp1 LIKE '%UATHAYAM%' THEN 'UATHAYAM'
            END AS DivisionCode,
            t0.SalPackMsr AS UOM,
            t0.U_SubGrp3  AS AttributeSetName,
            RTRIM(t0.U_SubGrp5)  AS SizeGroup,
            t0.U_HSNCODE  AS HSNCode,
            t0.U_SubGrp1  AS Brand,
            t0.SalPackUn  AS SalPackUn,
            RTRIM(t0.U_SubGrp6)  AS ColorCode,
            ISNULL(t0.U_SubGrp11, T0.U_SUBGRP6) AS ColorName,
            ISNULL(t0.U_SubGrp17, T0.U_SubGrp6) AS Color,
            ISNULL(t0.U_SubGrp13, T0.U_SubGrp6) AS Shade,
            t0.MinLevel   AS Min_Qty,
            t0.MaxLevel   AS Max_Qty,
            CASE WHEN t0.U_SubGrp13='Core item' THEN 1 ELSE 0 END AS IsCoreColor,
            t0.U_taxrate  AS TaxBelow2500,
            t0.U_taxrate1000 AS TaxAbove2500,
            t0.U_SubGrp1  AS SubBrandCode
        FROM [BBLive].[dbo].oitm t0
        JOIN [BBLive].[dbo].oitb t1 ON t0.ItmsGrpCod = t1.ItmsGrpCod
        WHERE t0.ItemCode IN (${placeholders})
        ORDER BY t0.ItemCode
    `;

    const result = await req.query(query);
    return result.recordset;
}

// ─────────────────────────────────────────────────────────────────────────────
// PRICE LISTS — paged list + full data by codes
// ─────────────────────────────────────────────────────────────────────────────
async function getPriceListsPaged({ page = 1, limit = 50, search, pushStatus } = {}) {
    const pool       = await getPool();
    const offset     = (page - 1) * limit;
    const searchVal  = search     ? `%${search}%` : null;
    const statusVal  = pushStatus || null;

    const EXCLUDED_BRANDS = `'ACCESSORIES','ADVERTISEMENT','ALL','SAMPLE','PRINTING & STATIONERY',
        'IMPERIAL COMPUTERS','PACKING MATERIAL','REPAIRS & MAINTENANCE',
        'SALES PROMOTION EXPENSES','EVERYDAY DHOTIE','ALLDAYS DHOTIE',
        'ADD DHOTIE','ADD SHIRT','EVERYDAY SHIRTING','EVERYDAY RDY'`;

    const dataQuery = `
        WITH PriceSummary AS (
            SELECT
                t0.ItemCode  AS ProductCode,
                t0.ItemName  AS ProductName,
                B.U_Brand    AS Brand,
                COUNT(*)                  AS PriceEntries,
                COUNT(DISTINCT B.U_State) AS StateCount,
                MIN(B.U_SelPrice)         AS MinPrice,
                MAX(B.U_SelPrice)         AS MaxPrice
            FROM [BBLive].[dbo].OITM t0
            JOIN (
                SELECT T0.DocEntry, T2.U_Brand, T1.U_ItemCode,
                       T0.U_State, T0.U_SelPrice, T0.U_MRP, T2.U_Lock, T0.LineId
                FROM [BBLive].[dbo].[@INS_PLM2] T0
                INNER JOIN [BBLive].[dbo].[@INS_OPLM] T1 ON T0.DocEntry = T1.DocEntry
                INNER JOIN [BBLive].[dbo].[@INS_PLM1] T2 ON T0.DocEntry = T2.DocEntry
            ) B ON B.U_ItemCode = t0.ItemCode
            WHERE B.U_SelPrice > 0 AND B.U_Brand NOT IN (${EXCLUDED_BRANDS})
            GROUP BY t0.ItemCode, t0.ItemName, B.U_Brand
        )
        SELECT
            COUNT(*) OVER()                        AS TotalCount,
            ps.ProductCode, ps.ProductName, ps.Brand,
            ps.PriceEntries, ps.StateCount, ps.MinPrice, ps.MaxPrice,
            ISNULL(rp.PushStatus, 'Pending')       AS PushStatus,
            rp.LastPushedAt,
            rp.ErrorMessage                        AS PushError
        FROM PriceSummary ps
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] rp
            ON rp.MasterType = 'pricelists' AND rp.RecordKey = ps.ProductCode
        WHERE (@search IS NULL OR ps.ProductCode LIKE @search OR ps.ProductName LIKE @search OR ps.Brand LIKE @search)
          AND (@pushStatus IS NULL OR ISNULL(rp.PushStatus, 'Pending') = @pushStatus)
        ORDER BY ps.ProductCode
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `;

    const summaryQuery = `
        SELECT ISNULL(rp.PushStatus, 'Pending') AS PushStatus, COUNT(*) AS Count
        FROM (
            SELECT DISTINCT t0.ItemCode
            FROM [BBLive].[dbo].OITM t0
            JOIN (
                SELECT T2.U_Brand, T1.U_ItemCode, T0.U_SelPrice
                FROM [BBLive].[dbo].[@INS_PLM2] T0
                INNER JOIN [BBLive].[dbo].[@INS_OPLM] T1 ON T0.DocEntry = T1.DocEntry
                INNER JOIN [BBLive].[dbo].[@INS_PLM1] T2 ON T0.DocEntry = T2.DocEntry
            ) B ON B.U_ItemCode = t0.ItemCode
            WHERE B.U_SelPrice > 0 AND B.U_Brand NOT IN (${EXCLUDED_BRANDS})
        ) base
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] rp
            ON rp.MasterType = 'pricelists' AND rp.RecordKey = base.ItemCode
        GROUP BY ISNULL(rp.PushStatus, 'Pending')
    `;

    const [dataRes, summaryRes] = await Promise.all([
        pool.request()
            .input('search',     sql.NVarChar(200), searchVal)
            .input('pushStatus', sql.NVarChar(20),  statusVal)
            .input('offset',     sql.Int,           offset)
            .input('limit',      sql.Int,           limit)
            .query(dataQuery),
        pool.request().query(summaryQuery),
    ]);

    const records = dataRes.recordset;
    const total   = records.length > 0 ? records[0].TotalCount : 0;
    const summaryMap = { Pending: 0, Pushing: 0, Pushed: 0, Failed: 0 };
    for (const row of summaryRes.recordset) summaryMap[row.PushStatus] = row.Count;

    return {
        data:       records.map(({ TotalCount, ...rest }) => rest),
        total, page, limit,
        totalPages: Math.ceil(total / limit),
        summary:    summaryMap,
    };
}

async function getPriceListDataByCodes(productCodes) {
    if (!productCodes || productCodes.length === 0) return [];
    const pool = await getPool();
    const req  = pool.request();
    const placeholders = productCodes.map((c, i) => { req.input(`c${i}`, sql.NVarChar(50), c); return `@c${i}`; }).join(',');

    const query = `
        SELECT
            t0.ItemCode                                         AS ProductCode,
            B.DocEntry                                          AS PriceListID,
            B.U_Brand                                           AS SubBrandCode,
            t0.ItemCode                                         AS BPProductName,
            B.U_State                                           AS PriceListCode,
            NULL                                                AS EffectiveFrom,
            NULL                                                AS EffectiveTo,
            CASE WHEN B.U_Lock = 'Y' THEN 0 ELSE 1 END         AS PriceListIsActive,
            'Dealer'                                            AS BPCategory,
            B.U_SelPrice                                        AS Price,
            B.U_MRP                                             AS MRP,
            B.LineId                                            AS PriceID,
            CASE WHEN B.U_Lock = 'Y' THEN 0 ELSE 1 END         AS PriceIsActive
        FROM [BBLive].[dbo].OITM t0
        LEFT JOIN (
            SELECT T0.DocEntry, T2.U_Brand, T1.U_ItemCode, T0.U_State,
                   T0.U_SelPrice, T0.U_MRP, T2.U_Lock, T0.LineId
            FROM [BBLive].[dbo].[@INS_PLM2] T0
            INNER JOIN [BBLive].[dbo].[@INS_OPLM] T1 ON T0.DocEntry = T1.DocEntry
            INNER JOIN [BBLive].[dbo].[@INS_PLM1] T2 ON T0.DocEntry = T2.DocEntry
        ) B ON B.U_ItemCode = t0.ItemCode
        WHERE B.U_SelPrice > 0
          AND B.U_Brand NOT IN (
              'ACCESSORIES','ADVERTISEMENT','ALL','SAMPLE','PRINTING & STATIONERY',
              'IMPERIAL COMPUTERS','PACKING MATERIAL','REPAIRS & MAINTENANCE',
              'SALES PROMOTION EXPENSES','EVERYDAY DHOTIE','ALLDAYS DHOTIE',
              'ADD DHOTIE','ADD SHIRT','EVERYDAY SHIRTING','EVERYDAY RDY'
          )
          AND t0.ItemCode IN (${placeholders})
        ORDER BY t0.ItemCode
    `;
    const result = await req.query(query);
    return result.recordset;
}

// ─────────────────────────────────────────────────────────────────────────────
// BUSINESS PARTNERS — paged list + full data by codes
// ─────────────────────────────────────────────────────────────────────────────
async function getBPListPaged({ page = 1, limit = 50, search, pushStatus } = {}) {
    const pool      = await getPool();
    const offset    = (page - 1) * limit;
    const searchVal = search     ? `%${search}%` : null;
    const statusVal = pushStatus || null;

    const dataQuery = `
        SELECT
            COUNT(*) OVER()                             AS TotalCount,
            T0.CardCode                                 AS BPCode,
            T0.CardName                                 AS BPName,
            CASE WHEN T0.GroupCode IN ('100','106') THEN 'Dealer' ELSE '' END AS BPCategory,
            T0.U_AreaCode                               AS AreaCode,
            CASE
                WHEN ISNULL(T0.U_Grade,'') IN ('','-') THEN 'C'
                ELSE REPLACE(T0.U_Grade,'Grade','')
            END                                         AS GradeOfBP,
            ISNULL(T0.U_GSTIN,'')                       AS GSTNo,
            CASE
                WHEN LEN(RIGHT(ISNULL(T0.Phone1,''),10))=10
                 AND RIGHT(ISNULL(T0.Phone1,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Phone1,10)
                WHEN LEN(RIGHT(ISNULL(T0.Phone2,''),10))=10
                 AND RIGHT(ISNULL(T0.Phone2,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Phone2,10)
                ELSE ''
            END                                         AS Phone1,
            (SELECT TOP 1 City FROM [BBLive].[dbo].CRD1
             WHERE CardCode=T0.CardCode AND AdresType='B') AS City,
            ISNULL(rp.PushStatus,'Pending')             AS PushStatus,
            rp.LastPushedAt,
            rp.ErrorMessage                             AS PushError
        FROM [BBLive].[dbo].OCRD T0
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] rp
            ON rp.MasterType = 'businesspartners' AND rp.RecordKey = T0.CardCode
        WHERE T0.CardType = 'C'
          AND T0.validFor = 'Y'
          AND T0.U_AreaCode != ''
          AND (@search IS NULL OR T0.CardCode LIKE @search OR T0.CardName LIKE @search)
          AND (@pushStatus IS NULL OR ISNULL(rp.PushStatus,'Pending') = @pushStatus)
        ORDER BY T0.CardCode
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `;

    const summaryQuery = `
        SELECT ISNULL(rp.PushStatus,'Pending') AS PushStatus, COUNT(*) AS Count
        FROM [BBLive].[dbo].OCRD T0
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] rp
            ON rp.MasterType = 'businesspartners' AND rp.RecordKey = T0.CardCode
        WHERE T0.CardType='C' AND T0.validFor='Y' AND T0.U_AreaCode != ''
        GROUP BY ISNULL(rp.PushStatus,'Pending')
    `;

    const [dataRes, summaryRes] = await Promise.all([
        pool.request()
            .input('search',     sql.NVarChar(200), searchVal)
            .input('pushStatus', sql.NVarChar(20),  statusVal)
            .input('offset',     sql.Int,           offset)
            .input('limit',      sql.Int,           limit)
            .query(dataQuery),
        pool.request().query(summaryQuery),
    ]);

    const records = dataRes.recordset;
    const total   = records.length > 0 ? records[0].TotalCount : 0;
    const summaryMap = { Pending: 0, Pushing: 0, Pushed: 0, Failed: 0 };
    for (const row of summaryRes.recordset) summaryMap[row.PushStatus] = row.Count;

    return {
        data:       records.map(({ TotalCount, ...rest }) => rest),
        total, page, limit,
        totalPages: Math.ceil(total / limit),
        summary:    summaryMap,
    };
}

async function getBPMasterDataByCodes(cardCodes) {
    if (!cardCodes || cardCodes.length === 0) return [];
    const pool = await getPool();
    const req  = pool.request();
    const placeholders = cardCodes.map((c, i) => { req.input(`c${i}`, sql.NVarChar(50), c); return `@c${i}`; }).join(',');

    const query = `
        WITH SubBrandMap AS (
            SELECT * FROM (VALUES
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
            T0.CardCode AS BPCode, T0.CardName AS BPName, T0.Currency AS DefaultCurrency,
            CASE WHEN T0.validFor='Y' THEN 1 ELSE 0 END AS IsActive,
            0 AS AllowCreditLimit, T0.CardFName AS DisplayName,
            CASE WHEN T0.GroupCode IN ('100','106') THEN 'Dealer' ELSE '' END AS BPCategory,
            '' AS BPGroupCode, T0.U_showcode AS SR_BPCode,
            CASE WHEN ISNULL(T0.U_Grade,'') IN ('','-') THEN 'C' ELSE REPLACE(T0.U_Grade,'Grade','') END AS GradeOfBP,
            '' AS CustomerRemark,
            CAST(0 AS DECIMAL(18,2)) AS Latitude, CAST(0 AS DECIMAL(18,2)) AS Longitude,
            T0.U_AreaCode AS AreaCode,
            SB.DivisionCode, SB.SubBrandName AS Brand, SB.SubBrandName,
            CAST(CASE SB.DiscountColumn
                WHEN 'U_Dis1'  THEN ISNULL(T0.U_Dis1,0)  WHEN 'U_Dis2'  THEN ISNULL(T0.U_Dis2,0)
                WHEN 'U_Dis3'  THEN ISNULL(T0.U_Dis3,0)  WHEN 'U_Dis7'  THEN ISNULL(T0.U_Dis7,0)
                WHEN 'U_Dis9'  THEN ISNULL(T0.U_Dis9,0)  WHEN 'U_Dis10' THEN ISNULL(T0.U_Dis10,0)
                WHEN 'U_Dis11' THEN ISNULL(T0.U_Dis11,0) ELSE 0
            END AS DECIMAL(18,6)) AS DiscountPer,
            (SELECT (ISNULL(T0.DocEntry,1)+T1.LineNum) AS BillShipID, T1.AdresType AS Type,
                T0.CardName AS DisplayName,
                CASE WHEN T1.AdresType='B' THEN 'OFFICE' ELSE 'SHIP' END AS LocationName,
                ISNULL(NULLIF(CAST(T1.Building AS NVARCHAR(MAX)),''),T1.City) AS Line1,
                ISNULL(NULLIF(CAST(T1.Block AS NVARCHAR(MAX)),''),T1.City) AS Line2,
                ISNULL(NULLIF(CAST(T1.Street AS NVARCHAR(MAX)),''),T1.City) AS Line3,
                CASE WHEN T0.ShipToDef=T1.Address THEN 1 ELSE 0 END AS IsDefault,
                ISNULL(T1.City,'') AS City, ISNULL(T1.County,T1.Country) AS County,
                ISNULL(T1.State,'') AS State, ISNULL(T1.Country,'') AS Country,
                ISNULL(T1.ZipCode,'') AS ZipCode,
                CASE WHEN LEN(RIGHT(ISNULL(T0.Phone1,''),10))=10 AND RIGHT(ISNULL(T0.Phone1,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Phone1,10)
                     WHEN LEN(RIGHT(ISNULL(T0.Phone2,''),10))=10 AND RIGHT(ISNULL(T0.Phone2,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Phone2,10)
                     WHEN LEN(RIGHT(ISNULL(T0.Cellular,''),10))=10 AND RIGHT(ISNULL(T0.Cellular,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Cellular,10)
                     ELSE '' END AS PhoneNumber,
                CASE WHEN LEN(RIGHT(ISNULL(T0.Phone1,''),10))=10 AND RIGHT(ISNULL(T0.Phone1,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Phone1,10)
                     WHEN LEN(RIGHT(ISNULL(T0.Phone2,''),10))=10 AND RIGHT(ISNULL(T0.Phone2,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Phone2,10)
                     WHEN LEN(RIGHT(ISNULL(T0.Cellular,''),10))=10 AND RIGHT(ISNULL(T0.Cellular,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Cellular,10)
                     ELSE '' END AS MobileNumber,
                ISNULL(T0.E_Mail,'') AS Email, ISNULL(T0.U_GSTIN,'') AS GSTNo,
                CASE WHEN T0.validFor='Y' THEN 1 ELSE 0 END AS IsActive, '' AS GstStatus
             FROM [BBLive].[dbo].CRD1 T1
             LEFT JOIN [BBLive].[dbo].OCPR C ON C.CardCode=T1.CardCode
             WHERE T1.CardCode=T0.CardCode FOR JSON PATH) AS BillShipTo,
            (SELECT ISNULL(T0.UpdateTS,'') AS MapDivisionID,
                CAST(0 AS DECIMAL(18,2)) AS AutoApprovalCreditLimit,
                CAST(0 AS DECIMAL(18,2)) AS AutoApprovalCreditLimitBal,
                CAST('' AS NVARCHAR(200)) AS BPRemarks,
                CAST(0 AS DECIMAL(18,2)) AS CreditLimit,
                ISNULL(T0.City,'') AS Destination,
                CAST(0 AS DECIMAL(18,2)) AS DiscountPer,
                SB2.DivisionCode, CAST(0 AS DECIMAL(18,2)) AS ExcessPer,
                REPLACE(T0.U_Grade,'Grade ','') AS Grade,
                CAST(1 AS INT) AS IsActive, CAST(0 AS INT) AS IsOrderAutoApproval,
                CAST(0 AS INT) AS Outstandingdays, ISNULL(T0.U_SalPriceCode,'') AS PriceLisCode,
                CAST(0 AS INT) AS ShowLimit, CAST('Uathayam' AS NVARCHAR(200)) AS TransporterName
             FROM (SELECT DISTINCT DivisionCode FROM SubBrandMap) SB2
             FOR JSON PATH) AS MST_MAP_BP_Division,
            (SELECT CntctCode AS ContactPersonID, Name AS ContactPersonName,
                ISNULL(Position,'proprietor') AS Designation,
                CASE WHEN LEN(RIGHT(ISNULL(T0.Phone1,''),10))=10 AND RIGHT(ISNULL(T0.Phone1,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Phone1,10)
                     WHEN LEN(RIGHT(ISNULL(T0.Phone2,''),10))=10 AND RIGHT(ISNULL(T0.Phone2,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Phone2,10)
                     ELSE '' END AS MobileNum,
                CASE WHEN LEN(RIGHT(ISNULL(T0.Phone1,''),10))=10 AND RIGHT(ISNULL(T0.Phone1,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Phone1,10)
                     WHEN LEN(RIGHT(ISNULL(T0.Phone2,''),10))=10 AND RIGHT(ISNULL(T0.Phone2,''),10) NOT LIKE '%[^0-9]%' THEN RIGHT(T0.Phone2,10)
                     ELSE '' END AS WhatsAppNum,
                E_MailL AS EmailID,
                CASE WHEN Active='Y' THEN 1 ELSE 0 END AS IsActive,
                CAST(0 AS INT) AS IsSendOverDueReminder, SB.DivisionCode,
                CAST(0 AS INT) AS PaymentSMS, CAST(0 AS INT) AS PaymentEmail,
                CAST(1 AS INT) AS PaymentWhatsapp, CAST(1 AS INT) AS OrderEmail,
                CAST(1 AS INT) AS OrderSMS, CAST(1 AS INT) AS OrderWhatsapp,
                CAST(1 AS INT) AS InvoiceWhatsapp, CAST(0 AS INT) AS InvoiceEmail,
                CAST(0 AS INT) AS InvoiceSMS, CAST(0 AS INT) AS PaymentRequestSMS,
                CAST(0 AS INT) AS PaymentRequestEmail, CAST(0 AS INT) AS PaymentrequestWhatsapp,
                CAST(0 AS INT) AS OutstandingSMS, CAST(0 AS INT) AS OutstandingEmail,
                CAST(0 AS INT) AS OutstandingWhatsapp, CAST(0 AS INT) AS PaycollectionWhatsapp,
                CAST(0 AS INT) AS DistributorWhatsapp
             FROM [BBLive].[dbo].OCPR WHERE CardCode=T0.CardCode FOR JSON PATH) AS Map_BpContactDetails,
            (SELECT SB2.SubBrandName AS Brand, SB2.DivisionCode FROM SubBrandMap SB2 FOR JSON PATH) AS MST_MAP_BP_Brand,
            (SELECT SB2.SubBrandName, SB2.DivisionCode,
                CAST(CASE SB2.DiscountColumn
                    WHEN 'U_Dis1' THEN ISNULL(T0.U_Dis1,0) WHEN 'U_Dis2' THEN ISNULL(T0.U_Dis2,0)
                    WHEN 'U_Dis3' THEN ISNULL(T0.U_Dis3,0) WHEN 'U_Dis7' THEN ISNULL(T0.U_Dis7,0)
                    WHEN 'U_Dis9' THEN ISNULL(T0.U_Dis9,0) WHEN 'U_Dis10' THEN ISNULL(T0.U_Dis10,0)
                    WHEN 'U_Dis11' THEN ISNULL(T0.U_Dis11,0) ELSE 0
                END AS DECIMAL(18,6)) AS DiscountPer
             FROM SubBrandMap SB2 FOR JSON PATH) AS MST_Map_BP_SubBrand,
            (SELECT 'TRADE DISCOUNT' AS DiscountName, SB3.DivisionCode, SB3.SubBrandName AS Brand,
                CAST(CASE SB3.DiscountColumn
                    WHEN 'U_Dis1' THEN ISNULL(T0.U_Dis1,0) WHEN 'U_Dis2' THEN ISNULL(T0.U_Dis2,0)
                    WHEN 'U_Dis3' THEN ISNULL(T0.U_Dis3,0) WHEN 'U_Dis7' THEN ISNULL(T0.U_Dis7,0)
                    WHEN 'U_Dis9' THEN ISNULL(T0.U_Dis9,0) WHEN 'U_Dis10' THEN ISNULL(T0.U_Dis10,0)
                    WHEN 'U_Dis11' THEN ISNULL(T0.U_Dis11,0) ELSE 0
                END AS DECIMAL(18,6)) AS DiscountPer,
                CONVERT(VARCHAR(19),CAST('2019-04-01' AS DATETIME),126) AS FromDate,
                CONVERT(VARCHAR(19),CAST('2030-03-31' AS DATETIME),126) AS ToDate
             FROM SubBrandMap SB3 FOR JSON PATH) AS Discount_BP_Division
        FROM [BBLive].[dbo].OCRD T0
        CROSS JOIN SubBrandMap SB
        WHERE T0.CardType='C' AND T0.validFor='Y' AND T0.U_AreaCode != ''
          AND T0.CardCode IN (${placeholders})
        ORDER BY T0.CardCode, SB.DivisionCode, SB.SubBrandName
    `;
    const result = await req.query(query);
    return result.recordset;
}

// ─────────────────────────────────────────────────────────────────────────────
// SCHEMES — paged list + full data by DocEntry codes
// ─────────────────────────────────────────────────────────────────────────────
async function getSchemesPaged({ page = 1, limit = 50, search, pushStatus } = {}) {
    const pool      = await getPool();
    const offset    = (page - 1) * limit;
    const searchVal = search     ? `%${search}%` : null;
    const statusVal = pushStatus || null;

    const dataQuery = `
        WITH SchemeSummary AS (
            SELECT
                T0.DocEntry,
                CAST(CONCAT(T0.[Object], T0.DocNum) AS NVARCHAR(50)) AS PolicyNumber,
                T0.Remark                                             AS PolicyName,
                MAX(CASE
                    WHEN T1.U_bran LIKE '%UATHAYAM%' THEN 'UATHAYAM'
                    WHEN T1.U_bran LIKE '%ARISER%'   THEN 'ARISER'
                    ELSE NULL END)                                    AS DivisionCode,
                MAX(T1.U_Discunt)                                     AS DiscountBasis,
                CONVERT(VARCHAR(10), T0.U_FrmDt, 120)                AS FromDate,
                CONVERT(VARCHAR(10), T0.U_ToDt,  120)                AS ToDate,
                COUNT(DISTINCT T1.LineId)                             AS LineCount
            FROM [BBLive].[dbo]."@SCHEM" T0
            INNER JOIN [BBLive].[dbo]."@SCHEML" T1 ON T0.DocEntry = T1.DocEntry
            WHERE T0.U_FrmDt >= '20250701' AND T0.U_ToDt <= '20260531'
            GROUP BY T0.DocEntry, T0.DocNum, T0.[Object], T0.Remark, T0.U_FrmDt, T0.U_ToDt
        )
        SELECT
            COUNT(*) OVER()                             AS TotalCount,
            ss.DocEntry, ss.PolicyNumber, ss.PolicyName,
            ss.DivisionCode, ss.DiscountBasis,
            ss.FromDate, ss.ToDate, ss.LineCount,
            ISNULL(rp.PushStatus,'Pending')             AS PushStatus,
            rp.LastPushedAt,
            rp.ErrorMessage                             AS PushError
        FROM SchemeSummary ss
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] rp
            ON rp.MasterType = 'schemes' AND rp.RecordKey = CAST(ss.DocEntry AS NVARCHAR(100))
        WHERE (@search IS NULL OR ss.PolicyNumber LIKE @search OR ss.PolicyName LIKE @search)
          AND (@pushStatus IS NULL OR ISNULL(rp.PushStatus,'Pending') = @pushStatus)
        ORDER BY ss.DocEntry
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `;

    const summaryQuery = `
        SELECT ISNULL(rp.PushStatus,'Pending') AS PushStatus, COUNT(*) AS Count
        FROM (
            SELECT DISTINCT T0.DocEntry
            FROM [BBLive].[dbo]."@SCHEM" T0
            WHERE T0.U_FrmDt >= '20250701' AND T0.U_ToDt <= '20260531'
        ) base
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] rp
            ON rp.MasterType='schemes' AND rp.RecordKey=CAST(base.DocEntry AS NVARCHAR(100))
        GROUP BY ISNULL(rp.PushStatus,'Pending')
    `;

    const [dataRes, summaryRes] = await Promise.all([
        pool.request()
            .input('search',     sql.NVarChar(200), searchVal)
            .input('pushStatus', sql.NVarChar(20),  statusVal)
            .input('offset',     sql.Int,           offset)
            .input('limit',      sql.Int,           limit)
            .query(dataQuery),
        pool.request().query(summaryQuery),
    ]);

    const records = dataRes.recordset;
    const total   = records.length > 0 ? records[0].TotalCount : 0;
    const summaryMap = { Pending: 0, Pushing: 0, Pushed: 0, Failed: 0 };
    for (const row of summaryRes.recordset) summaryMap[row.PushStatus] = row.Count;

    return {
        data:       records.map(({ TotalCount, ...rest }) => rest),
        total, page, limit,
        totalPages: Math.ceil(total / limit),
        summary:    summaryMap,
    };
}

async function getSchemeDataByCodes(docEntries) {
    if (!docEntries || docEntries.length === 0) return [];
    const pool = await getPool();
    const req  = pool.request();
    const placeholders = docEntries.map((c, i) => { req.input(`c${i}`, sql.Int, Number(c)); return `@c${i}`; }).join(',');

    const query = `
        SELECT
            CAST(CONCAT(T0.[Object], T0.DocNum) AS NVARCHAR(50)) AS PolicyNumber,
            1 AS Revision,
            T0.DocEntry AS PolicyID,
            T0.Remark AS PolicyName,
            CASE WHEN T1.U_Discunt='Quantity' THEN 'SC' WHEN T1.U_Discunt='Percentage' THEN 'DIS' END AS SavingType,
            T1.U_Discunt AS DiscountBasis,
            'P' AS Applicability,
            1 AS IsCustomerDefined, 1 AS IsActive,
            CASE
                WHEN T1.U_bran='UATHAYAM DHOTIE'     THEN 'UATHAYAM'
                WHEN T1.U_bran='UATHAYAM SHIRTING'   THEN 'UATHAYAM'
                WHEN T1.U_bran='UATHAYAM RDY'        THEN 'UATHAYAM'
                WHEN T1.U_bran='UATHAYAM HOS'        THEN 'UATHAYAM'
                WHEN T1.U_bran='UATHAYAM KIDS SET'   THEN 'UATHAYAM'
                WHEN T1.U_bran='UATHAYAM MENS SET'   THEN 'UATHAYAM'
                WHEN T1.U_bran='ARISER SHIRT'        THEN 'ARISER'
                WHEN T1.U_bran='ARISER MENS TROUSERS' THEN 'ARISER'
                WHEN T1.U_bran='ARISER HOS'          THEN 'ARISER'
            END AS DivisionCode,
            '2026-05-20T00:00:00' AS FromDate, '2026-07-01T00:00:00' AS ToDate,
            0 AS AllowDiscountForAllProducts, NULL AS DiscountPer,
            (SELECT 'DEALER' AS BPCategory FOR JSON PATH, INCLUDE_NULL_VALUES) AS SC_BpCategoryMapping,
            (SELECT DISTINCT L.U_Stat AS StateCode FROM [BBLive].[dbo]."@SCHEML" L
             WHERE L.DocEntry=T0.DocEntry AND L.U_Stat IS NOT NULL
             FOR JSON PATH, INCLUDE_NULL_VALUES) AS StateMapping,
            (SELECT 'DEALER' AS Role FOR JSON PATH, INCLUDE_NULL_VALUES) AS RoleMapping,
            (SELECT NULL AS BPCode FOR JSON PATH, INCLUDE_NULL_VALUES) AS SC_BpExclution,
            (SELECT DISTINCT C.CardCode AS BPCode FROM [BBLive].[dbo].OCRD C
             INNER JOIN [BBLive].[dbo].CRD1 D ON C.CardCode=D.CardCode AND D.AdresType='B'
             WHERE D.State IN (SELECT DISTINCT L.U_Stat FROM [BBLive].[dbo]."@SCHEML" L WHERE L.DocEntry=T0.DocEntry)
             FOR JSON PATH, INCLUDE_NULL_VALUES) AS SC_BpInclution,
            (SELECT I.ItemCode AS ProductCode, I.U_Size AS SizeCode, I.U_SubGrp6 AS ColorCode,
                CAST(L.U_BillsQty AS DECIMAL(10,2)) AS MinOrderQty,
                CAST(L.U_OffersQty AS DECIMAL(10,2)) AS FreeQty,
                'S' AS Applicability, L.U_OffersQty AS AllowMultiplyFreeQty,
                CAST(L.U_BillsQty AS DECIMAL(10,2)) AS MaxAllowedFreeQty,
                1 AS IsActive, 1 AS MappingStatus,
                (SELECT ALT.ItemCode AS ProductCode, ALT.U_Size AS SizeCode, ALT.U_SubGrp6 AS ColorCode, 0 AS IsActive
                 FROM [BBLive].[dbo].OITM ALT WHERE ALT.ItemCode=I.ItemCode AND ALT.validFor='Y'
                 FOR JSON PATH, INCLUDE_NULL_VALUES) AS SC_ProdAlternate
             FROM [BBLive].[dbo]."@SCHEML" L
             INNER JOIN [BBLive].[dbo].OITM I ON I.U_SubGrp7=L.U_Qual
             WHERE L.DocEntry=T0.DocEntry FOR JSON PATH, INCLUDE_NULL_VALUES) AS SC_ProductMapping,
            (SELECT NULL AS GroupCode, NULL AS StyleCode, NULL AS MinOrderQty, NULL AS FreeQty,
                NULL AS Applicability, 0 AS AllowMultiplyFreeQty, NULL AS MaxAllowedFreeQty,
                NULL AS GroupName, 0 AS IsActive, 0 AS MappingStatus
             FOR JSON PATH, INCLUDE_NULL_VALUES) AS SC_ProdGroupMapping,
            (SELECT NULL AS ProductCode, NULL AS SizeCode, NULL AS ColorCode, 0 AS IsActive
             FOR JSON PATH, INCLUDE_NULL_VALUES) AS SC_ProdAlternate,
            (SELECT NULL AS GroupName, NULL AS StyleCode, 0 AS IsActive FOR JSON PATH, INCLUDE_NULL_VALUES) AS SC_ProdGroupAlternate,
            (SELECT NULL AS Brand, NULL AS DiscountType, NULL AS DiscountVal, 0 AS IsActive FOR JSON PATH, INCLUDE_NULL_VALUES) AS SC_Brand_Discount,
            (SELECT NULL AS DivisionCode, NULL AS GroupCode, NULL AS GroupName, NULL AS StyleCode,
                NULL AS StyleName, NULL AS DiscountType, NULL AS DiscountVal, 0 AS IsActive
             FOR JSON PATH, INCLUDE_NULL_VALUES) AS SC_ProdGroupDirectDiscount,
            (SELECT NULL AS ProductCode, NULL AS SizeCode, NULL AS ColorCode,
                NULL AS DiscountType, NULL AS DiscountVal, 0 AS IsActive
             FOR JSON PATH, INCLUDE_NULL_VALUES) AS SC_ProductDirectDiscount
        FROM [BBLive].[dbo]."@SCHEM" T0
        INNER JOIN [BBLive].[dbo]."@SCHEML" T1 ON T0.DocEntry = T1.DocEntry
        WHERE T0.DocEntry IN (${placeholders})
    `;
    const result = await req.query(query);
    return result.recordset;
}

// ─────────────────────────────────────────────────────────────────────────────
// STOCK INVENTORY — paged list + full data by codes
// ─────────────────────────────────────────────────────────────────────────────
async function getStockPaged({ page = 1, limit = 50, search, pushStatus } = {}) {
    const pool      = await getPool();
    const offset    = (page - 1) * limit;
    const searchVal = search     ? `%${search}%` : null;
    const statusVal = pushStatus || null;

    const dataQuery = `
        WITH StockSummary AS (
            SELECT
                t0.ItemCode  AS ProductCode,
                t0.ItemName  AS ProductName,
                t0.U_SubGrp4 AS StyleCode,
                t0.U_SubGrp1 AS Brand,
                CAST(SUM(t1.OnHand) AS INT) AS StockQuantity
            FROM [BBLive].[dbo].OITM t0
            INNER JOIN [BBLive].[dbo].OITW t1 ON t0.ItemCode = t1.ItemCode
            WHERE t1.WhsCode = 'ASRS' AND t0.ValidFor = 'Y'
            GROUP BY t0.ItemCode, t0.ItemName, t0.U_SubGrp4, t0.U_SubGrp1
        )
        SELECT
            COUNT(*) OVER()                             AS TotalCount,
            ss.ProductCode, ss.ProductName, ss.StyleCode, ss.Brand, ss.StockQuantity,
            ISNULL(rp.PushStatus,'Pending')             AS PushStatus,
            rp.LastPushedAt,
            rp.ErrorMessage                             AS PushError
        FROM StockSummary ss
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] rp
            ON rp.MasterType = 'stockInventory' AND rp.RecordKey = ss.ProductCode
        WHERE (@search IS NULL OR ss.ProductCode LIKE @search OR ss.ProductName LIKE @search)
          AND (@pushStatus IS NULL OR ISNULL(rp.PushStatus,'Pending') = @pushStatus)
        ORDER BY ss.ProductCode
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `;

    const summaryQuery = `
        SELECT ISNULL(rp.PushStatus,'Pending') AS PushStatus, COUNT(*) AS Count
        FROM (
            SELECT DISTINCT t0.ItemCode
            FROM [BBLive].[dbo].OITM t0
            INNER JOIN [BBLive].[dbo].OITW t1 ON t0.ItemCode=t1.ItemCode
            WHERE t1.WhsCode='ASRS' AND t0.ValidFor='Y'
        ) base
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] rp
            ON rp.MasterType='stockInventory' AND rp.RecordKey=base.ItemCode
        GROUP BY ISNULL(rp.PushStatus,'Pending')
    `;

    const [dataRes, summaryRes] = await Promise.all([
        pool.request()
            .input('search',     sql.NVarChar(200), searchVal)
            .input('pushStatus', sql.NVarChar(20),  statusVal)
            .input('offset',     sql.Int,           offset)
            .input('limit',      sql.Int,           limit)
            .query(dataQuery),
        pool.request().query(summaryQuery),
    ]);

    const records = dataRes.recordset;
    const total   = records.length > 0 ? records[0].TotalCount : 0;
    const summaryMap = { Pending: 0, Pushing: 0, Pushed: 0, Failed: 0 };
    for (const row of summaryRes.recordset) summaryMap[row.PushStatus] = row.Count;

    return {
        data:       records.map(({ TotalCount, ...rest }) => rest),
        total, page, limit,
        totalPages: Math.ceil(total / limit),
        summary:    summaryMap,
    };
}

async function getStockDataByCodes(itemCodes) {
    if (!itemCodes || itemCodes.length === 0) return [];
    const pool = await getPool();
    const req  = pool.request();
    const placeholders = itemCodes.map((c, i) => { req.input(`c${i}`, sql.NVarChar(50), c); return `@c${i}`; }).join(',');

    const query = `
        SELECT
            t0.DocEntry                                                AS ExternalId,
            t0.DocEntry                                                AS ProductMappingId,
            t0.ItemCode                                                AS ProductCode,
            t0.U_SubGrp13                                              AS ColorCode,
            t0.U_SubGrp7                                               AS AttributeValue,
            t0.U_SubGrp4                                               AS StyleCode,
            t0.U_Size                                                  AS Size,
            CAST(t1.OnHand AS INT)                                     AS StockQuantity,
            'Stock'                                                    AS Type,
            CASE WHEN t0.ValidFor='Y' THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS IsActive,
            CASE WHEN t0.MinLevel > t1.OnHand THEN 'High Stock' ELSE 'Low Stock' END AS StockHighlightMessageDetails,
            CASE WHEN CAST(t0.MinLevel AS INT) > CAST(t1.OnHand AS INT) THEN 'Very few stock left' ELSE 'Stock Available' END AS StockMessage
        FROM [BBLive].[dbo].OITM AS t0
        INNER JOIN [BBLive].[dbo].OITW AS t1 ON t0.ItemCode = t1.ItemCode
        WHERE t1.WhsCode = 'ASRS'
          AND t0.ValidFor = 'Y'
          AND t0.ItemCode IN (${placeholders})
        ORDER BY t0.ItemCode
    `;
    const result = await req.query(query);
    return result.recordset;
}

// ─────────────────────────────────────────────────────────────────────────────
// OUTSTANDING / RECEIVABLES — paged list + full data by CardCode
// ─────────────────────────────────────────────────────────────────────────────
async function getOutstandingPaged({ page = 1, limit = 50, search, pushStatus } = {}) {
    const pool      = await getPool();
    const offset    = (page - 1) * limit;
    const searchVal = search     ? `%${search}%` : null;
    const statusVal = pushStatus || null;

    const dataQuery = `
        WITH BaseData AS (
            SELECT
                k.CardCode, k.CardName,
                lk.City, lk.State,
                CASE WHEN tk.U_Brand LIKE '%UATHAYAM%' THEN 'UATHAYAM' ELSE 'ARISER' END AS DivisionCode,
                k.BalDueDeb AS BalanceToBePaid,
                CASE WHEN k.BalDueDeb > 0 THEN DATEDIFF(d, k.duedat, GETDATE()) ELSE 0 END AS OverdueDays
            FROM (
                SELECT T2.CardCode, T2.CardName, T1.BalDueDeb, T1.Ref1, T0.TransId,
                    CASE WHEN T1.TransType IN ('-2','30') THEN T1.DueDate ELSE T0.RefDate END AS duedat,
                    T0.TransType
                FROM [BBLive].[dbo].OJDT T0
                INNER JOIN [BBLive].[dbo].JDT1 T1 ON T0.TransId=T1.TransId
                INNER JOIN [BBLive].[dbo].OCRD T2 ON T2.CardCode=T1.ShortName
                INNER JOIN [BBLive].[dbo].OCRG T3 ON T2.GroupCode=T3.GroupCode
                WHERE T1.BalDueDeb<>'0' AND T2.CardType='C'
            ) k
            LEFT JOIN (
                SELECT tl.DocNum, tl.CardCode, tl.U_Brand, tl.TransId
                FROM [BBLive].[dbo].OINV tl
                LEFT JOIN [BBLive].[dbo].[@INCM_BND1] tg ON tg.U_Name=RTRIM(tl.U_Brand)
            ) tk ON CONVERT(NVARCHAR(20),tk.DocNum)=k.Ref1 AND tk.CardCode=k.CardCode AND tk.TransId=k.TransId
            LEFT JOIN (
                SELECT cr1.CardCode, cr2.City, cr2.State
                FROM [BBLive].[dbo].OCRD cr1
                LEFT JOIN [BBLive].[dbo].CRD1 cr2 ON cr2.CardCode=cr1.CardCode AND cr2.AdresType='B'
            ) lk ON lk.CardCode=k.CardCode
        ),
        OutstandingSummary AS (
            SELECT
                CardCode,
                MAX(CardName)          AS CardName,
                MAX(City)              AS City,
                MAX(State)             AS State,
                MAX(DivisionCode)      AS DivisionCode,
                COUNT(*)               AS InvoiceCount,
                SUM(BalanceToBePaid)   AS TotalBalance,
                MAX(OverdueDays)       AS MaxOverdueDays
            FROM BaseData
            GROUP BY CardCode
        )
        SELECT
            COUNT(*) OVER()                             AS TotalCount,
            os.CardCode, os.CardName, os.City, os.State,
            os.DivisionCode, os.InvoiceCount,
            os.TotalBalance, os.MaxOverdueDays,
            ISNULL(rp.PushStatus,'Pending')             AS PushStatus,
            rp.LastPushedAt,
            rp.ErrorMessage                             AS PushError
        FROM OutstandingSummary os
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] rp
            ON rp.MasterType='outstanding' AND rp.RecordKey=os.CardCode
        WHERE (@search IS NULL OR os.CardCode LIKE @search OR os.CardName LIKE @search)
          AND (@pushStatus IS NULL OR ISNULL(rp.PushStatus,'Pending') = @pushStatus)
        ORDER BY os.CardCode
        OFFSET @offset ROWS FETCH NEXT @limit ROWS ONLY
    `;

    const summaryQuery = `
        WITH BaseData AS (
            SELECT DISTINCT k.CardCode
            FROM (
                SELECT T2.CardCode, T1.BalDueDeb
                FROM [BBLive].[dbo].OJDT T0
                INNER JOIN [BBLive].[dbo].JDT1 T1 ON T0.TransId=T1.TransId
                INNER JOIN [BBLive].[dbo].OCRD T2 ON T2.CardCode=T1.ShortName
                INNER JOIN [BBLive].[dbo].OCRG T3 ON T2.GroupCode=T3.GroupCode
                WHERE T1.BalDueDeb<>'0' AND T2.CardType='C'
            ) k
        )
        SELECT ISNULL(rp.PushStatus,'Pending') AS PushStatus, COUNT(*) AS Count
        FROM BaseData b
        LEFT JOIN [BBLive].[dbo].[SFA_RecordPushStatus] rp
            ON rp.MasterType='outstanding' AND rp.RecordKey=b.CardCode
        GROUP BY ISNULL(rp.PushStatus,'Pending')
    `;

    const [dataRes, summaryRes] = await Promise.all([
        pool.request()
            .input('search',     sql.NVarChar(200), searchVal)
            .input('pushStatus', sql.NVarChar(20),  statusVal)
            .input('offset',     sql.Int,           offset)
            .input('limit',      sql.Int,           limit)
            .query(dataQuery),
        pool.request().query(summaryQuery),
    ]);

    const records = dataRes.recordset;
    const total   = records.length > 0 ? records[0].TotalCount : 0;
    const summaryMap = { Pending: 0, Pushing: 0, Pushed: 0, Failed: 0 };
    for (const row of summaryRes.recordset) summaryMap[row.PushStatus] = row.Count;

    return {
        data:       records.map(({ TotalCount, ...rest }) => rest),
        total, page, limit,
        totalPages: Math.ceil(total / limit),
        summary:    summaryMap,
    };
}

async function getOutstandingDataByCodes(cardCodes) {
    if (!cardCodes || cardCodes.length === 0) return [];
    const pool = await getPool();
    const req  = pool.request();
    const placeholders = cardCodes.map((c, i) => { req.input(`c${i}`, sql.NVarChar(50), c); return `@c${i}`; }).join(',');

    const query = `
        SELECT
            CASE WHEN tk.U_Brand LIKE '%UATHAYAM%' THEN 'UATHAYAM' ELSE 'ARISER' END AS DivisionCode,
            CAST(tk.DocEntry AS NVARCHAR(20)) AS DocEntry,
            CASE WHEN tk.U_Brand LIKE '%UATHAYAM%' THEN 'UATHAYAM' ELSE 'ARISER' END AS DivisionName,
            tk.U_Brand AS Brand,
            CASE WHEN k.TransType='13' THEN 'AR Invoice' WHEN k.TransType='30' THEN 'Journal' ELSE NULL END AS DocType,
            CASE WHEN k.Ref1 NOT LIKE '%[^0-9]%' AND k.Ref1 IS NOT NULL THEN CAST(k.Ref1 AS INT) ELSE NULL END AS InvoiceNo,
            tk.DocDate AS InvoiceDate,
            k.duedat AS DueDate,
            k.CardCode, k.CardName,
            lk.City, lk.State,
            k.memo AS DocumentRemarks,
            CASE WHEN k.BalDueDeb>0 THEN DATEDIFF(d,k.duedat,GETDATE()) ELSE 0 END AS OverdueDays,
            GETDATE() AS OverdueDate,
            CASE WHEN k.TransType='30' THEN k.Debit ELSE ISNULL(tk.DocTotal,0) END AS DocumentValue,
            k.BalDueDeb AS BalanceToBePaid,
            CAST(0 AS BIT) AS BatchEnd
        FROM (
            SELECT T2.CardCode, T2.CardName, T0.RefDate, T1.BaseRef, T1.Debit, T1.Credit,
                T0.TransId, T1.BalDueDeb, T1.LineMemo, T2.MailCity, T3.GroupName, T1.Ref1,
                CASE WHEN T1.TransType IN ('-2','30') THEN T1.DueDate ELSE T0.RefDate END AS duedat,
                T1.OcrCode3 AS brand, T0.memo, T0.TransType
            FROM [BBLive].[dbo].OJDT T0
            INNER JOIN [BBLive].[dbo].JDT1 T1 ON T0.TransId=T1.TransId
            INNER JOIN [BBLive].[dbo].OCRD T2 ON T2.CardCode=T1.ShortName
            INNER JOIN [BBLive].[dbo].OCRG T3 ON T2.GroupCode=T3.GroupCode
            WHERE T1.BalDueDeb<>'0' AND T2.CardType='C'
              AND T2.CardCode IN (${placeholders})
        ) k
        LEFT JOIN (
            SELECT tl.DocNum, tl.DocEntry, tl.DocDate, tl.DocTotal, tl.CardCode, tl.U_Brand, tg.U_Remarks, tl.TransId
            FROM [BBLive].[dbo].OINV tl
            LEFT JOIN [BBLive].[dbo].[@INCM_BND1] tg ON tg.U_Name=RTRIM(tl.U_Brand)
        ) tk ON CONVERT(NVARCHAR(20),tk.DocNum)=k.Ref1 AND tk.CardCode=k.CardCode AND tk.TransId=k.TransId
        LEFT JOIN (
            SELECT cr1.CardCode, cr2.City, cr2.State
            FROM [BBLive].[dbo].OCRD cr1
            LEFT JOIN [BBLive].[dbo].CRD1 cr2 ON cr2.CardCode=cr1.CardCode AND cr2.AdresType='B'
        ) lk ON lk.CardCode=k.CardCode
    `;
    const result = await req.query(query);
    return result.recordset;
}

module.exports = {
    getProductData,
    getProductsPaged,
    getProductDataByCodes,
    getPriceListData,
    getPriceListsPaged,
    getPriceListDataByCodes,
    getImageData,
    getSchemeData,
    getSchemesPaged,
    getSchemeDataByCodes,
    getBPMasterData,
    getBPListPaged,
    getBPMasterDataByCodes,
    getStockData,
    getStockPaged,
    getStockDataByCodes,
    getOutstandingData,
    getOutstandingPaged,
    getOutstandingDataByCodes,
};
