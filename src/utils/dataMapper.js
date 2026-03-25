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
                    DfltWH          : 'ASRS',
                    Sku             : row.ProductCode,
                    Popularity      : 0,
                    HideItem        : 0,
                    SortBy          : 0,
                    PreBooking      : 1,
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

// ─────────────────────────────────────────────────────────────────────────────
// PRICE LIST MAPPER
//
// Changes vs previous version:
//   1. PriceID    — passed through from row.PriceID (ROW_NUMBER in SQL).
//   2. EffectiveFrom / EffectiveTo — taken directly from DB strings.
//                  SQL guarantees a non-null ISO datetime string via ISNULL
//                  fallback, so no extra defaulting needed here.
// ─────────────────────────────────────────────────────────────────────────────
function mapToPriceListPayload(sqlRows) {
    if (!sqlRows || sqlRows.length === 0) return [];

    // productCode → priceListId → priceList entry
    const productMap = new Map();

    for (const row of sqlRows) {
        const productCode = row.ProductCode;
        const priceListId = row.PriceListID;

        if (!productMap.has(productCode)) {
            productMap.set(productCode, new Map());
        }

        const priceListMap = productMap.get(productCode);

        if (!priceListMap.has(priceListId)) {
            priceListMap.set(priceListId, {
                PriceListID  : priceListId,
                SubBrandCode : row.SubBrandCode  ?? null,
                BPProductName: row.BPProductName ?? productCode,
                PriceLisCode : row.PriceListCode ?? null,
                EffectiveFrom: row.EffectiveFrom ?? '2025-01-01T00:00:00',   
                EffectiveTo  : row.EffectiveTo   ?? '2055-12-31T00:00:00',   
                IsActive     : row.PriceListIsActive ?? 1,
                Prices       : []
            });
        }

        const entry = priceListMap.get(priceListId);

        // Deduplicate by BPCategory — each category gets one Prices entry.
        if (!entry.Prices.some(p => p.BPCategory === row.BPCategory)) {
            entry.Prices.push({
                PriceListID: priceListId,
                PriceID    : row.PriceID    ?? null,   // ← new field
                BPCategory : row.BPCategory ?? null,
                Price      : row.Price      ?? 0,
                MRP        : row.MRP        ?? 0,
                IsActive   : row.PriceIsActive ?? 1
            });
        }
    }

    const result = [];
    for (const [productCode, priceListMap] of productMap) {
        result.push({
            ProductCode: productCode,
            PriceList  : Array.from(priceListMap.values())
        });
    }
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
                    SC_BpCategoryMapping       : JSON.parse(row.SC_BpCategoryMapping),
                    StateMapping               : JSON.parse(row.StateMapping),
                    RoleMapping                : [
                                                    {
                                                        "Role": "RBM"
                                                    }
                                                ],//JSON.parse(row.RoleMapping),
                    SC_BpExclution             : JSON.parse(row.SC_BpExclution),
                    SC_BpInclution             :[
                                                    {
                                                        "BPCode": null
                                                    }
                                                ],//JSON.parse(row.SC_BpInclution),
                    SC_ProductMapping          : JSON.parse(row.SC_ProductMapping),
                    SC_ProdGroupMapping        : JSON.parse(row.SC_ProdGroupMapping),
                    SC_ProdAlternate           : JSON.parse(row.SC_ProdAlternate),
                    SC_ProdGroupAlternate      : JSON.parse(row.SC_ProdGroupAlternate),
                    SC_Brand_Discount          : JSON.parse(row.SC_Brand_Discount),
                    SC_ProdGroupDirectDiscount : JSON.parse(row.SC_ProdGroupDirectDiscount),
                    SC_ProductDirectDiscount   : JSON.parse(row.SC_ProductDirectDiscount)
                }
            });
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

function parseBillShipTo(val) {
    const rows = safeParse(val);
    return rows.map(r => ({
        BillShipID  : r.BillShipID,
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
            State       : resolveStateName(r.State)    ?? '',
            Country     : resolveCountryName(r.Country) ?? '',
            ZipCode     : r.ZipCode      ?? '',
            PhoneNumber : r.PhoneNumber  ?? '',
            MobileNumber: r.MobileNumber ?? '',
            Email       : r.Email        ?? '',
            GSTNo       : r.GSTNo        ?? '',
            IsActive    : r.IsActive     ?? 1,
            GstStatus   : r.GstStatus    ?? ''
        }
    }));
}

function mapToBPPayload(rows) {
    if (!rows || rows.length === 0) return { businessPartners: [] };

    const bpMap = new Map();

    for (const row of rows) {
        const bpCode = row.BPCode;
        let MST_MAP_BP_Division = JSON.parse(row.MST_MAP_BP_Division)
        if (!bpMap.has(bpCode)) {
            bpMap.set(bpCode, {
                BPCode          : row.BPCode,
                BPName          : row.BPName,
                DefaultCurrency : row.DefaultCurrency  ?? 'INR',
                IsActive        : row.IsActive         ?? 1,
                AllowCreditLimit: row.AllowCreditLimit ?? 0,
                DisplayName     : row.DisplayName      ?? row.BPName,
                BPCategory      : row.BPCategory       ?? '',
                BPGroupCode     : '',
                SR_BPCode       : row.SR_BPCode        ?? '',
                GradeOfBP       : row.GradeOfBP        ?? '',
                CustomerRemark  : row.CustomerRemark   ?? '',
                Latitude        : decimal(row.Latitude)         ?? 0.00,
                Longitude       : decimal(row.Longitude)        ?? 0.00,
                AreaCode        : row.AreaCode         ?? '',

                BillShipTo          : [],
                Map_BpContactDetails    : [],
                Discount_BP_Division: JSON.parse(row.Discount_BP_Division) ,
                MST_MAP_BP_Division : MST_MAP_BP_Division.map(d => ({
                                        ...d,
                                        AutoApprovalCreditLimit   : decimal(d.AutoApprovalCreditLimit),
                                        AutoApprovalCreditLimitBal: decimal(d.AutoApprovalCreditLimitBal),
                                        CreditLimit               : decimal(d.CreditLimit),
                                        DiscountPer               : decimal(d.DiscountPer),
                                        ExcessPer                 : decimal(d.ExcessPer),
                                    })),
                MST_MAP_BP_Brand    : JSON.parse(row.MST_MAP_BP_Brand),
                MST_Map_BP_SubBrand : JSON.parse(row.MST_Map_BP_SubBrand)
            });
        }

        const bp = bpMap.get(bpCode);
        if (bp.BillShipTo.length === 0) {
            for (const entry of parseBillShipTo(row.BillShipTo)) {
                const isDup = bp.BillShipTo.some(
                    b => b.BillShipID === entry.BillShipID && b.Type === entry.Type
                );
                if (!isDup) bp.BillShipTo.push(entry);
            }
        }

        for (const c of safeParse(row.Map_BpContactDetails)) {
            const isDup = bp.Map_BpContactDetails.some(
                x => x.ContactPersonID === c.ContactPersonID && x.DivisionCode === c.DivisionCode
            );
            if (!isDup) bp.Map_BpContactDetails.push(c);
        }
    }

    return { businessPartners: Array.from(bpMap.values()) };
}

function decimal(val) {
    return Number(val ?? 0).toFixed(2)
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

function mapToStockPayload(rows) {
    if (!rows || rows.length === 0) return { businessPartners: [] };

        const stockData = []

        for (const row of rows) {
        {
            stockData.push({
                "method": "PATCH",
                "url": `/services/data/v60.0/sobjects/dmpl__AccountStock__c/ExternalId__c/Stock${row.ExternalId}`,
                "referenceId": "AccountStock01",
                "body": {
                    "ProductMappingId__c": row.ProductMappingId,
                    "ProductCode__c": row.ProductCode,
                    "ColorCode__c": row.ColorCode,
                    "AttributeValue__c": row.AttributeValue,
                    "StyleCode__c": row.StyleCode,
                    "Size__c": row.Size,
                    "StockQuantity__c": row.StockQuantity,
                    "Type__c": row.Type,
                    "IsActive__c": row.IsActive,
                
                    "StockHighlightMessageDetails__c": row.StockHighlightMessageDetails,
                    "StockMessage__c": row.StockMessage
                }
            })
        }
        
    }

    return { stockData };
}

module.exports = {
    mapToSalesforcePayload,
    mapToPriceListPayload,
    mapToImagePayload,
    mapToSchemePayload,
    mapToBPPayload,
    groupByProduct,
    buildPayload,
    mapToStockPayload
};