/**
 * Maps the flat SQL result set to the Salesforce Product Upsert JSON structure.
 * Groups data by ProductCode to create nested arrays for Colors, Attributes, etc.
 */

// Formats a numeric tax value to a 2-decimal-place string e.g. "5.00"
// Business rule: 12% tax is automatically upgraded to 18%
const TAX_REMAP = {
  "12.00": "18.00"
};

function formatTax(val) {
  if (val == null) return null;
  const formatted = Number(val).toFixed(2);
  return TAX_REMAP[formatted] ?? formatted;
}

function mapToSalesforcePayload(rows) {
  if (!rows || rows.length === 0) return [];

  const map = new Map();

  // ------------------ BUILD MAP ------------------
  for (const row of rows) {
    const code = row.ProductCode;

    // ---------- INIT PRODUCT ----------
    if (!map.has(code)) {
      map.set(code, {
        Product: {
          ProductCode: row.ProductCode,
          ProductName: row.ProductName,
          IsActive: row.ProductIsActive,
          GroupCode: row.ProductGroupCode,
          ShortDesc: row.ShortDesc,
          DetailedDesc: row.DetailedDesc,
          CategoryName: row.CategoryName,
          StyleCode: row.StyleCode,
          SizeCode: row.SizeCode,
          DivisionCode: row.DivisionCode,
          UOM: row.UOM,
          AttributeSetName: row.AttributeSetName,
          SizeGroup: row.SizeGroup,
          HSNCode: row.HSNCode,
          Brand: row.Brand,
          SalPackUn: row.SalPackUn,
          DfltWH: null,
          Sku: row.ProductCode,
          Popularity: 0,
          HideItem: 0,
          SortBy: 0,
          PreBooking: 1,
          Tag: row.ProductName
        },

        ProductColors: [],
        ProductAttributes: [],
        ProductTaxes: [[]],
        ProductSubBrands: [],
        ProductDefaults: [],
        PROD_PRODUCTGROUP: [],
        ProductGroupGroupping: []
      });
    }

    const product = map.get(code);

    // ---------- PRODUCT COLORS ----------
    if (
      row.ColorCode &&
      !product.ProductColors.some(c => c.ColorCode === row.ColorCode)
    ) {
      product.ProductColors.push({
        ColorCode: row.ColorCode,
        ColorName: row.ColorName,
        Color: row.Color,
        IsActive: 1,
        Shade: row.Shade,
        Min_Qty: row.Min_Qty || 1,
        Max_Qty: row.Max_Qty || 100000,
        IsCoreColor: row.IsCoreColor
      });
    }

    // ---------- PRODUCT ATTRIBUTES ----------
    if (row.AttrVal && row.AttributeName) {
      const attrKey = `${row.AttributeName}_${row.AttrVal}`;

      if (
        !product.ProductAttributes.some(
          a => `${a.Attribute.AttributeName}_${a.AttrVal}` === attrKey
        )
      ) {
        product.ProductAttributes.push({
          AttrVal: row.AttrVal,
          IsActive: 1,
          Attribute: {
            AttributeName: row.AttributeName,
            IsMainAttribute: row.IsMainAttribute ?? 1,
            IsFilterApplicable: row.IsFilterApplicable ?? 1,
            AttributeValType: row.AttributeValType ?? 1,
            SortingVal: row.AttrSortingVal ?? 1,
            IsActive: 1
          },
          AttributeSet: {
            AttributeSetName: row.AttributeSetName,
            IsActive: 1
          }
        });
      }
    }

    // ---------- PRODUCT TAXES ----------
    const taxes = product.ProductTaxes[0];

    if (
      row.TaxBelow2500 != null &&
      !taxes.some(t => t.EvalExpression === 'Price < 2500')
    ) {
      taxes.push({
        TaxPer: formatTax(row.TaxBelow2500),
        EvalExpression: 'Price < 2500'
      });
    }

    if (
      row.TaxAbove2500 != null &&
      !taxes.some(t => t.EvalExpression === 'Price >= 2500')
    ) {
      taxes.push({
        TaxPer: formatTax(row.TaxAbove2500),
        EvalExpression: 'Price >= 2500'
      });
    }

    // ---------- PRODUCT SUB BRAND ----------
    if (
      row.SubBrandCode &&
      product.ProductSubBrands.length === 0
    ) {
      product.ProductSubBrands.push({
        SubBrandCode: row.SubBrandCode,
        BPProductName: row.ProductName,
        DisplayName: row.ProductName,
        IsActive: 1,
        SKU: null,
        AltSKU: null
      });
    }

    // ---------- PRODUCT DEFAULTS ----------
    if (product.ProductDefaults.length === 0) {
      product.ProductDefaults.push({
        GroupCode: row.ProductGroupCode,
        StyleCode: row.StyleCode,
        SizeCode: row.SizeCode,
        ColorCode: row.ColorCode,
        IsActive: 1,
        DivisionCode: row.DivisionCode
      });
    }

    // ---------- PROD_PRODUCTGROUP ----------
    if (product.PROD_PRODUCTGROUP.length === 0) {
      product.PROD_PRODUCTGROUP.push({
        GroupCode: row.ProductGroupCode,
        IsActive: 1,
        SortingVal: 1
      });
    }

    // ---------- PRODUCT GROUP GROUPPING ----------
    if (product.ProductGroupGroupping.length === 0) {
      product.ProductGroupGroupping.push({
        GroupingName: row.CategoryName,
        GroupCode: row.ProductGroupCode,
        IsActive: 1
      });
    }
  }

  // ------------------ SAFETY FIX (MANDATORY ATTRIBUTE) ------------------
  for (const product of map.values()) {
    if (!product.ProductAttributes || product.ProductAttributes.length === 0) {
      product.ProductAttributes.push({
        AttrVal: "Default",
        IsActive: 1,
        Attribute: {
          AttributeName: "General",
          IsMainAttribute: 1,
          IsFilterApplicable: 0,
          AttributeValType: 1,
          SortingVal: 1,
          IsActive: 1
        },
        AttributeSet: {
          AttributeSetName: product.Product.AttributeSetName,
          IsActive: 1
        }
      });
    }
  }

  return Array.from(map.values());
}

/**
 * Maps flat SQL rows into the nested PriceList payload format:
 *
 * [
 *   {
 *     "ProductCode": "...",
 *     "PriceList": [
 *       {
 *         "PriceListID": 678,
 *         "SubBrandCode": "...",
 *         "BPProductName": "...",
 *         "PriceLisCode": "TN",
 *         "EffectiveFrom": "...",
 *         "EffectiveTo": "...",
 *         "IsActive": 1,
 *         "Prices": [
 *           { "PriceListID": 678, "BPCategory": "Showroom", "Price": 2516.0, "MRP": 4240.0, "IsActive": 1 },
 *           ...
 *         ]
 *       }
 *     ]
 *   }
 * ]
 *
 * Grouping strategy:
 *   Level 1 — ProductCode
 *   Level 2 — PriceListID  (one entry per unique DocEntry / state)
 *   Level 3 — Prices[]     (one entry per BPCategory row)
 */
function mapToPriceListPayload(sqlRows) {
  if (!sqlRows || sqlRows.length === 0) return [];

  // Map<ProductCode, Map<PriceListID, priceListEntry>>
  const productMap = new Map();

  for (const row of sqlRows) {
    const productCode  = row.ProductCode;
    const priceListId  = row.PriceListID;

    // ── Level 1: ProductCode ──────────────────────────────────────────────
    if (!productMap.has(productCode)) {
      productMap.set(productCode, new Map());
    }
    const priceListMap = productMap.get(productCode);

    // ── Level 2: PriceListID ─────────────────────────────────────────────
    if (!priceListMap.has(priceListId)) {
      priceListMap.set(priceListId, {
        PriceListID   : priceListId,
        SubBrandCode  : row.SubBrandCode  ?? null,
        BPProductName : row.BPProductName ?? productCode,
        PriceLisCode  : row.PriceListCode ?? null,
        EffectiveFrom : row.EffectiveFrom ?? null,
        EffectiveTo   : row.EffectiveTo   ?? null,
        IsActive      : row.PriceListIsActive ?? 1,
        Prices        : []
      });
    }

    // ── Level 3: Price entry (one per BPCategory) ────────────────────────
    const priceListEntry = priceListMap.get(priceListId);

    // Avoid duplicate BPCategory entries for the same PriceListID
    const alreadyAdded = priceListEntry.Prices.some(
      p => p.BPCategory === row.BPCategory
    );

    if (!alreadyAdded) {
      priceListEntry.Prices.push({
        PriceListID : priceListId,
        BPCategory  : row.BPCategory  ?? null,
        Price       : row.Price        ?? 0,
        MRP         : row.MRP          ?? 0,
        IsActive    : row.PriceIsActive ?? 1
      });
    }
  }

  // ── Flatten to final array ────────────────────────────────────────────
  const result = [];

  for (const [productCode, priceListMap] of productMap) {
    result.push({
      ProductCode : productCode,
      PriceList   : Array.from(priceListMap.values())
    });
  }

  return result;
}

function mapToImagePayload(sqlRows) {
    if (!sqlRows || sqlRows.length === 0) return [];

    return sqlRows.map(row => ({
        skuCode: row.skuCode,
        ColorCode: row.ColorCode,
        fileName: row.fileName,
        Description: row.Description,
        base64Data: row.base64Data
    }));
}

function groupByProduct(rows) {
  const map = {};

  for (const row of rows) {
    if (!map[row.ProductCode]) {
      map[row.ProductCode] = {
        base: row,
        colors: [],
        taxes: []
      };
    }

    if (
      row.ColorCode &&
      !map[row.ProductCode].colors.some(c => c.ColorCode === row.ColorCode)
    ) {
      map[row.ProductCode].colors.push({
        ColorCode: row.ColorCode,
        ColorName: row.ColorName,
        Color: row.Color,
        IsActive: 1,
        Shade: row.Shade,
        Min_Qty: row.Min_Qty || 1,
        Max_Qty: row.Max_Qty || 100000,
        IsCoreColor: row.IsCoreColor
      });
    }

    if (row.TaxBelow2500 !== null) {
      map[row.ProductCode].taxes.push({
        TaxPer: formatTax(row.TaxBelow2500),
        EvalExpression: "Price < 2500"
      });
    }

    if (row.TaxAbove2500 !== null) {
      map[row.ProductCode].taxes.push({
        TaxPer: formatTax(row.TaxAbove2500),
        EvalExpression: "Price >= 2500"
      });
    }
  }

  return Object.values(map);
}

function buildPayload(product) {
  const r = product.base;

  return {
    Product: {
      ProductCode: r.ProductCode,
      ProductName: r.ProductName,
      IsActive: r.ProductIsActive,
      GroupCode: r.ProductGroupCode,
      ShortDesc: r.ShortDesc,
      DetailedDesc: r.DetailedDesc,
      CategoryName: r.CategoryName,
      StyleCode: r.StyleCode,
      SizeCode: r.SizeCode,
      DivisionCode: r.DivisionCode,
      UOM: r.UOM,
      AttributeSetName: r.AttributeSetName,
      SizeGroup: r.SizeGroup,
      HSNCode: r.HSNCode,
      Brand: r.Brand,
      SalPackUn: r.SalPackUn,
      DfltWH: r.DfltWH,
      Sku: r.ProductCode,
      Popularity: 0,
      HideItem: 0,
      SortBy: 0,
      PreBooking: 1,
      Tag: r.ProductName
    },

    ProductColors: product.colors,

    ProductTaxes: [product.taxes],

    ProductSubBrands: [
      {
        SubBrandCode: r.SubBrandCode,
        BPProductName: r.ProductName,
        DisplayName: r.ProductName,
        IsActive: 1,
        SKU: null,
        AltSKU: null
      }
    ],

    ProductDefaults: [
      {
        GroupCode: r.ProductGroupCode,
        StyleCode: r.StyleCode,
        SizeCode: r.SizeCode,
        ColorCode: product.colors[0]?.ColorCode || null,
        IsActive: 1,
        DivisionCode: r.DivisionCode
      }
    ],

    PROD_PRODUCTGROUP: [
      {
        GroupCode: r.ProductGroupCode,
        IsActive: 1,
        SortingVal: 1
      }
    ],

    ProductGroupGroupping: [
      {
        GroupingName: r.CategoryName,
        GroupCode: r.ProductGroupCode,
        IsActive: 1
      }
    ]
  };
}

module.exports = {
    mapToSalesforcePayload,
    mapToPriceListPayload,
    mapToImagePayload,
    groupByProduct,
    buildPayload
};