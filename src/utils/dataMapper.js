/**
 * Maps the flat SQL result set to the Salesforce Product Upsert JSON structure.
 */

const TAX_REMAP = { "12.00": "18.00" };

function formatTax(val) {
    if (val == null) return null;
    const formatted = Number(val).toFixed(2);
    return TAX_REMAP[formatted] ?? formatted;
}

function mapToSalesforcePayload(rows) {
    if (!rows || rows.length === 0) return [];

    const map = new Map();

    for (const row of rows) {
        const code = row.ProductCode;

        if (!map.has(code)) {
            map.set(code, {
                Product: {
                    ProductCode     : row.ProductCode,
                    ProductName     : row.ProductName,
                    IsActive        : row.ProductIsActive,
                    GroupCode       : row.ProductGroupCode,
                    ShortDesc       : row.ShortDesc,
                    DetailedDesc    : row.DetailedDesc,
                    CategoryName    : row.CategoryName,
                    StyleCode       : row.StyleCode,
                    SizeCode        : row.SizeCode,
                    DivisionCode    : row.DivisionCode,
                    UOM             : row.UOM,
                    AttributeSetName: row.AttributeSetName,
                    SizeGroup       : row.SizeGroup,
                    HSNCode         : row.HSNCode,
                    Brand           : row.Brand,
                    SalPackUn       : row.SalPackUn,
                    DfltWH          : null,
                    Sku             : row.ProductCode,
                    Popularity      : 0,
                    HideItem        : 0,
                    SortBy          : 0,
                    PreBooking      : 1,
                    Tag             : row.ProductName
                },
                ProductColors        : [],
                ProductAttributes    : [],
                ProductTaxes         : [[]],
                ProductSubBrands     : [],
                ProductDefaults      : [],
                PROD_PRODUCTGROUP    : [],
                ProductGroupGroupping: []
            });
        }

        const product = map.get(code);

        if (row.ColorCode && !product.ProductColors.some(c => c.ColorCode === row.ColorCode)) {
            product.ProductColors.push({
                ColorCode  : row.ColorCode,
                ColorName  : row.ColorName,
                Color      : row.Color,
                IsActive   : 1,
                Shade      : row.Shade,
                Min_Qty    : row.Min_Qty || 1,
                Max_Qty    : row.Max_Qty || 100000,
                IsCoreColor: row.IsCoreColor
            });
        }

        if (row.AttrVal && row.AttributeName) {
            const attrKey = `${row.AttributeName}_${row.AttrVal}`;
            if (!product.ProductAttributes.some(a => `${a.Attribute.AttributeName}_${a.AttrVal}` === attrKey)) {
                product.ProductAttributes.push({
                    AttrVal     : row.AttrVal,
                    IsActive    : 1,
                    Attribute   : {
                        AttributeName      : row.AttributeName,
                        IsMainAttribute    : row.IsMainAttribute    ?? 1,
                        IsFilterApplicable : row.IsFilterApplicable ?? 1,
                        AttributeValType   : row.AttributeValType   ?? 1,
                        SortingVal         : row.AttrSortingVal     ?? 1,
                        IsActive           : 1
                    },
                    AttributeSet: { AttributeSetName: row.AttributeSetName, IsActive: 1 }
                });
            }
        }

        const taxes = product.ProductTaxes[0];
        if (row.TaxBelow2500 != null && !taxes.some(t => t.EvalExpression === 'Price < 2500'))
            taxes.push({ TaxPer: formatTax(row.TaxBelow2500), EvalExpression: 'Price < 2500' });
        if (row.TaxAbove2500 != null && !taxes.some(t => t.EvalExpression === 'Price >= 2500'))
            taxes.push({ TaxPer: formatTax(row.TaxAbove2500), EvalExpression: 'Price >= 2500' });

        if (row.SubBrandCode && product.ProductSubBrands.length === 0)
            product.ProductSubBrands.push({
                SubBrandCode: row.SubBrandCode, BPProductName: row.ProductName,
                DisplayName: row.ProductName, IsActive: 1, SKU: null, AltSKU: null
            });

        if (product.ProductDefaults.length === 0)
            product.ProductDefaults.push({
                GroupCode: row.ProductGroupCode, StyleCode: row.StyleCode,
                SizeCode: row.SizeCode, ColorCode: row.ColorCode,
                IsActive: 1, DivisionCode: row.DivisionCode
            });

        if (product.PROD_PRODUCTGROUP.length === 0)
            product.PROD_PRODUCTGROUP.push({ GroupCode: row.ProductGroupCode, IsActive: 1, SortingVal: 1 });

        if (product.ProductGroupGroupping.length === 0)
            product.ProductGroupGroupping.push({
                GroupingName: row.CategoryName, GroupCode: row.ProductGroupCode, IsActive: 1
            });
    }

    for (const product of map.values()) {
        if (!product.ProductAttributes || product.ProductAttributes.length === 0) {
            product.ProductAttributes.push({
                AttrVal: "Default", IsActive: 1,
                Attribute: {
                    AttributeName: "General", IsMainAttribute: 1,
                    IsFilterApplicable: 0, AttributeValType: 1, SortingVal: 1, IsActive: 1
                },
                AttributeSet: { AttributeSetName: product.Product.AttributeSetName, IsActive: 1 }
            });
        }
    }

    return Array.from(map.values());
}

function mapToPriceListPayload(sqlRows) {
    if (!sqlRows || sqlRows.length === 0) return [];
    const productMap = new Map();

    for (const row of sqlRows) {
        const productCode = row.ProductCode;
        const priceListId = row.PriceListID;
        if (!productMap.has(productCode)) productMap.set(productCode, new Map());
        const priceListMap = productMap.get(productCode);
        if (!priceListMap.has(priceListId)) {
            priceListMap.set(priceListId, {
                PriceListID  : priceListId,
                SubBrandCode : row.SubBrandCode  ?? null,
                BPProductName: row.BPProductName ?? productCode,
                PriceLisCode : row.PriceListCode ?? null,
                EffectiveFrom: row.EffectiveFrom ?? null,
                EffectiveTo  : row.EffectiveTo   ?? null,
                IsActive     : row.PriceListIsActive ?? 1,
                Prices       : []
            });
        }
        const entry = priceListMap.get(priceListId);
        if (!entry.Prices.some(p => p.BPCategory === row.BPCategory)) {
            entry.Prices.push({
                PriceListID: priceListId,
                BPCategory : row.BPCategory   ?? null,
                Price      : row.Price         ?? 0,
                MRP        : row.MRP           ?? 0,
                IsActive   : row.PriceIsActive ?? 1
            });
        }
    }

    const result = [];
    for (const [productCode, priceListMap] of productMap)
        result.push({ ProductCode: productCode, PriceList: Array.from(priceListMap.values()) });
    return result;
}

function mapToImagePayload(sqlRows) {
    if (!sqlRows || sqlRows.length === 0) return [];
    return sqlRows.map(row => ({
        skuCode    : row.skuCode,
        ColorCode  : row.ColorCode,
        fileName   : row.fileName,
        Description: row.Description,
        base64Data : row.base64Data
    }));
}

const INDIA_STATE_NAMES = {
    'AN':'Andaman and Nicobar Islands','AP':'Andhra Pradesh','AR':'Arunachal Pradesh',
    'AS':'Assam','BR':'Bihar','CH':'Chandigarh','CG':'Chhattisgarh',
    'DN':'Dadra and Nagar Haveli and Daman and Diu','DL':'Delhi','GA':'Goa',
    'GJ':'Gujarat','HR':'Haryana','HP':'HimachalPradesh','JK':'Jammu and Kashmir',
    'JH':'Jharkhand','KA':'Karnataka','KL':'Kerala','LA':'Ladakh','LD':'Lakshadweep',
    'MP':'Madhya Pradesh','MH':'Maharashtra','MN':'Manipur','ML':'Meghalaya',
    'MZ':'Mizoram','NL':'Nagaland','OD':'Odisha','OR':'Odisha','PY':'Puducherry',
    'PB':'Punjab','RJ':'Rajasthan','SK':'Sikkim','TN':'Tamil Nadu','TS':'Telangana',
    'TR':'Tripura','UP':'UttarPradesh','UK':'Uttarakhand','UT':'Uttarakhand','WB':'WestBengal'
};

function resolveStateName(code) {
    if (!code) return null;
    return INDIA_STATE_NAMES[code.trim().toUpperCase()] ?? code;
}

const COUNTRY_NAMES = {
    'IN': 'India',
    'US': 'United States',
    'AE': 'United Arab Emirates',
    'SG': 'Singapore',
    'MY': 'Malaysia',
    'LK': 'Sri Lanka',
    'NP': 'Nepal',
    'BD': 'Bangladesh',
    'AU': 'Australia',
    'CA': 'Canada',
    'GB': 'United Kingdom'
};

function resolveCountryName(code) {
    if (!code) return null;
    return COUNTRY_NAMES[code.trim().toUpperCase()] ?? code;
}

function mapToSchemePayload(rows) {
    if (!rows || rows.length === 0) return [];
    const policyMap = new Map();

    for (const row of rows) {
        const policyId = row.PolicyID;
        if (!policyMap.has(policyId)) {
            policyMap.set(policyId, {
                Policy: {
                    PolicyNumber               : row.PolicyNumber,
                    Revision                   : row.Revision,
                    PolicyID                   : row.PolicyID,
                    PolicyName                 : row.PolicyName,
                    SavingType                 : row.SavingType,
                    DiscountBasis              : row.DiscountBasis,
                    Applicability              : row.Applicability,
                    IsCustomerDefined          : row.IsCustomerDefined,
                    IsActive                   : row.IsActive,
                    DivisionCode               : row.DivisionCode,
                    FromDate                   : row.FromDate,
                    ToDate                     : row.ToDate,
                    AllowDiscountForAllProducts: row.AllowDiscountForAllProducts,
                    DiscountPer                : row.DiscountPer,
                    SC_BpCategoryMapping       : [{ BPCategory: 'DEALER' }],
                    StateMapping               : [{ StateCode: resolveStateName(row.StateCode) }],
                    RoleMapping                : [{ Role: 'RBM' }],
                    SC_BpExclution             : [{ BPCode: null }],
                    SC_BpInclution             : [{ BPCode: row.BPCode ?? null }],
                    SC_ProductMapping          : [],
                    SC_ProdGroupMapping        : [{ GroupCode:null, StyleCode:null, MinOrderQty:null, FreeQty:null, Applicability:null, AllowMultiplyFreeQty:0, MaxAllowedFreeQty:null, GroupName:null, IsActive:0, MappingStatus:0 }],
                    SC_ProdAlternate           : [{ ProductCode:null, SizeCode:null, ColorCode:null, IsActive:0 }],
                    SC_ProdGroupAlternate      : [{ GroupName:null, StyleCode:null, IsActive:0 }],
                    SC_Brand_Discount          : [{ Brand:null, DiscountType:null, DiscountVal:null, IsActive:0 }],
                    SC_ProdGroupDirectDiscount : [{ DivisionCode:null, GroupCode:null, GroupName:null, StyleCode:null, StyleName:null, DiscountType:null, DiscountVal:null, IsActive:0 }],
                    SC_ProductDirectDiscount   : [{ ProductCode:null, SizeCode:null, ColorCode:null, DiscountType:null, DiscountVal:null, IsActive:0 }]
                }
            });
        }
        const policy = policyMap.get(policyId).Policy;
        if (row.ProductCode) {
            const dup = policy.SC_ProductMapping.some(
                p => p.ProductCode === row.ProductCode && p.SizeCode === row.SizeCode && p.ColorCode === row.ColorCode
            );
            if (!dup) {
                policy.SC_ProductMapping.push({
                    ProductCode         : row.ProductCode,
                    SizeCode            : row.SizeCode            ?? null,
                    ColorCode           : row.ColorCode           ?? null,
                    MinOrderQty         : row.MinOrderQty         ?? 0,
                    FreeQty             : row.FreeQty             ?? 0,
                    Applicability       : row.ProductApplicability ?? 'S',
                    AllowMultiplyFreeQty: row.AllowMultiplyFreeQty ?? 0,
                    MaxAllowedFreeQty   : row.MaxAllowedFreeQty   ?? 0,
                    IsActive            : row.ProductIsActive      ?? 1,
                    MappingStatus       : row.MappingStatus        ?? 1
                });
            }
        }
    }
    return Array.from(policyMap.values());
}

// ─────────────────────────────────────────────────────────────────────────────
// BP MASTER MAPPER
// ─────────────────────────────────────────────────────────────────────────────

function safeParse(val) {
    if (val == null) return [];
    if (typeof val === 'object') return Array.isArray(val) ? val : [val];
    if (typeof val === 'string') {
        const t = val.trim();
        if (!t || t === 'null') return [];
        try { return JSON.parse(t); } catch { return []; }
    }
    return [];
}

/**
 * parseBillShipTo
 *
 * SQL now returns BillShipTo as a FOR JSON PATH flat array where:
 *   BillShipID = T1.LineNum  (integer — matches expected JSON 35163 / 36183)
 *
 * All address columns are flat at the top level of each row.
 * This function reshapes them into the nested Address object expected by SF.
 *
 * Expected output per entry:
 * {
 *   BillShipID  : <number>        ← T1.LineNum
 *   Type        : "B" | "S"
 *   DisplayName : <string>
 *   LocationName: "OFFICE"|"SHIP"
 *   Address: {
 *     Line1, Line2, Line3, IsDefault, City, County, State, Country,
 *     ZipCode, PhoneNumber, MobileNumber, Email, GSTNo, IsActive, GstStatus
 *   }
 * }
 */
function parseBillShipTo(val) {
    const rows = safeParse(val);
    return rows.map(r => ({
        BillShipID  : r.BillShipID,          // integer from T1.LineNum
        Type        : r.Type,
        DisplayName : r.DisplayName,
        LocationName: r.LocationName,
        Address: {
            Line1       : r.Line1        ?? '',
            Line2       : r.Line2        ?? '',
            Line3       : r.Line3        ?? '',
            IsDefault   : r.IsDefault    ?? 0,
            City        : r.City         ?? '',
            County      : r.County       ?? '',
            State       : resolveStateName(r.State)   ?? '',
            Country     : resolveCountryName(r.Country)      ?? '',
            ZipCode     : r.ZipCode      ?? '',
            PhoneNumber : r.PhoneNumber  ?? '',   // from T1.Tel1
            MobileNumber: r.MobileNumber ?? '',
            Email       : r.Email        ?? '',
            GSTNo       : r.GSTNo        ?? '',
            IsActive    : r.IsActive     ?? 1,
            GstStatus   : r.GstStatus    ?? ''
        }
    }));
}

/**
 * mapToBPPayload
 *
 * Changes vs previous version
 * ───────────────────────────
 * 1. parseBillShipTo uses BillShipID (= LineNum integer) for dedup
 * 2. Discount_BP_Division — one entry per DivisionCode
 * 3. MST_MAP_BP_Brand.Brand deduped by SubBrandName (not DivisionCode Brand)
 * 4. All FOR JSON cols handled via safeParse with correct dedup keys
 * 5. MST_MAP_BP_Division — hardcoded defaults (no external UDT table join)
 */
function mapToBPPayload(rows) {
    if (!rows || rows.length === 0) return { businessPartners: [] };

    const bpMap = new Map();

    for (const row of rows) {
        const bpCode = row.BPCode;

        if (!bpMap.has(bpCode)) {
            bpMap.set(bpCode, {
                BPCode          : row.BPCode,
                BPName          : row.BPName,
                DefaultCurrency : row.DefaultCurrency  ?? 'INR',
                IsActive        : row.IsActive         ?? 1,
                AllowCreditLimit: row.AllowCreditLimit ?? 0,
                DisplayName     : row.DisplayName      ?? row.BPName,
                BPCategory      : row.BPCategory       ?? '',
                BPGroupCode     : '',                          // always '' per expected JSON
                SR_BPCode       : row.SR_BPCode        ?? '',
                GradeOfBP       : row.GradeOfBP        ?? '',
                CustomerRemark  : row.CustomerRemark   ?? '',
                Latitude        : row.Latitude         ?? 0,
                Longitude       : row.Longitude        ?? 0,
                AreaCode        : row.AreaCode         ?? '',

                BillShipTo          : [],
                BPContactDetails    : [],
                Discount_BP_Division: [],
                MST_MAP_BP_Division : [],
                MST_MAP_BP_Brand    : [],
                MST_Map_BP_SubBrand : []
            });
        }

        const bp = bpMap.get(bpCode);

        // ── BillShipTo ────────────────────────────────────────────────────────
        // Parse once on first encounter (same data for all sub-brand rows).
        // BillShipID is now an integer (LineNum) — dedup by LineNum + Type.
        if (bp.BillShipTo.length === 0) {
            for (const entry of parseBillShipTo(row.BillShipTo)) {
                const isDup = bp.BillShipTo.some(
                    b => b.BillShipID === entry.BillShipID && b.Type === entry.Type
                );
                if (!isDup) bp.BillShipTo.push(entry);
            }
        }

        // ── BPContactDetails — deduplicate by ContactPersonID + DivisionCode ──
        for (const c of safeParse(row.BPContactDetails)) {
            const isDup = bp.BPContactDetails.some(
                x => x.ContactPersonID === c.ContactPersonID && x.DivisionCode === c.DivisionCode
            );
            if (!isDup) bp.BPContactDetails.push(c);
        }

        // ── Discount_BP_Division — ONE entry per DivisionCode ─────────────────
        if (!bp.Discount_BP_Division.some(d => d.DivisionCode === row.DivisionCode)) {
            bp.Discount_BP_Division.push({
                DiscountName: 'TRADE DISCOUNT',
                DivisionCode: row.DivisionCode,
                DiscountPer : row.DiscountPer ?? 0,
                FromDate    : '2019-04-01T00:00:00',
                ToDate      : '2030-03-31T00:00:00'
            });
        }

        // ── MST_MAP_BP_Division — deduplicate by DivisionCode ─────────────────
        for (const d of safeParse(row.MST_MAP_BP_Division)) {
            if (!bp.MST_MAP_BP_Division.some(x => x.DivisionCode == d.DivisionCode))
            bp.MST_MAP_BP_Division.push({
                ...d,
                AutoApprovalCreditLimitBal: decimal(d.AutoApprovalCreditLimitBal),
                CreditLimit: decimal(d.CreditLimit),
                ExcessPer: decimal(d.ExcessPer)
            });
        }

        // ── MST_MAP_BP_Brand — deduplicate by Brand + DivisionCode ────────────
        // Brand field = SubBrandName (e.g. "ARISER SHIRT") per expected JSON.
        for (const b of safeParse(row.MST_MAP_BP_Brand)) {
            if (!bp.MST_MAP_BP_Brand.some(x => x.Brand == b.Brand && x.DivisionCode == b.DivisionCode))
                bp.MST_MAP_BP_Brand.push(b);
        }

        // ── MST_Map_BP_SubBrand — deduplicate by SubBrandName + DivisionCode ──
        for (const s of safeParse(row.MST_Map_BP_SubBrand)) {
            if (!bp.MST_Map_BP_SubBrand.some(x => x.SubBrandName == s.SubBrandName && x.DivisionCode == s.DivisionCode))
                bp.MST_Map_BP_SubBrand.push(s);
        }
    }

    return { businessPartners: Array.from(bpMap.values()) };
}

function decimal(val) {
    return Number(val ?? 0).toFixed(2);
}

// ── Legacy helpers ────────────────────────────────────────────────────────────
function groupByProduct(rows) {
    const map = {};
    for (const row of rows) {
        if (!map[row.ProductCode]) map[row.ProductCode] = { base: row, colors: [], taxes: [] };
        if (row.ColorCode && !map[row.ProductCode].colors.some(c => c.ColorCode === row.ColorCode))
            map[row.ProductCode].colors.push({ ColorCode: row.ColorCode, ColorName: row.ColorName, Color: row.Color, IsActive: 1, Shade: row.Shade, Min_Qty: row.Min_Qty || 1, Max_Qty: row.Max_Qty || 100000, IsCoreColor: row.IsCoreColor });
        if (row.TaxBelow2500 !== null) map[row.ProductCode].taxes.push({ TaxPer: formatTax(row.TaxBelow2500), EvalExpression: "Price < 2500" });
        if (row.TaxAbove2500 !== null) map[row.ProductCode].taxes.push({ TaxPer: formatTax(row.TaxAbove2500), EvalExpression: "Price >= 2500" });
    }
    return Object.values(map);
}

function buildPayload(product) {
    const r = product.base;
    return {
        Product: { ProductCode: r.ProductCode, ProductName: r.ProductName, IsActive: r.ProductIsActive, GroupCode: r.ProductGroupCode, ShortDesc: r.ShortDesc, DetailedDesc: r.DetailedDesc, CategoryName: r.CategoryName, StyleCode: r.StyleCode, SizeCode: r.SizeCode, DivisionCode: r.DivisionCode, UOM: r.UOM, AttributeSetName: r.AttributeSetName, SizeGroup: r.SizeGroup, HSNCode: r.HSNCode, Brand: r.Brand, SalPackUn: r.SalPackUn, DfltWH: r.DfltWH, Sku: r.ProductCode, Popularity: 0, HideItem: 0, SortBy: 0, PreBooking: 1, Tag: r.ProductName },
        ProductColors       : product.colors,
        ProductTaxes        : [product.taxes],
        ProductSubBrands    : [{ SubBrandCode: r.SubBrandCode, BPProductName: r.ProductName, DisplayName: r.ProductName, IsActive: 1, SKU: null, AltSKU: null }],
        ProductDefaults     : [{ GroupCode: r.ProductGroupCode, StyleCode: r.StyleCode, SizeCode: r.SizeCode, ColorCode: product.colors[0]?.ColorCode || null, IsActive: 1, DivisionCode: r.DivisionCode }],
        PROD_PRODUCTGROUP   : [{ GroupCode: r.ProductGroupCode, IsActive: 1, SortingVal: 1 }],
        ProductGroupGroupping: [{ GroupingName: r.CategoryName, GroupCode: r.ProductGroupCode, IsActive: 1 }]
    };
}

module.exports = {
    mapToSalesforcePayload,
    mapToPriceListPayload,
    mapToImagePayload,
    mapToSchemePayload,
    mapToBPPayload,
    groupByProduct,
    buildPayload
};