const sql = require('mssql');
const config = require('../config/dbConfig');

let pool;
async function getPool() {
    if (!pool) {
        pool = await sql.connect(config);
    }
    return pool;
}

const SYNC_TYPES = [
    { syncType: 'products',         displayName: 'Product Master' },
    { syncType: 'pricelists',       displayName: 'Price Lists' },
    { syncType: 'images',           displayName: 'Product Images' },
    { syncType: 'schemes',          displayName: 'Schemes / Promotions' },
    { syncType: 'businesspartners', displayName: 'Business Partners' },
    { syncType: 'stockInventory',   displayName: 'Stock Inventory' },
    { syncType: 'outstanding',      displayName: 'Outstanding / Receivables' },
];

async function ensureTable() {
    const p = await getPool();

    await p.request().query(`
        IF OBJECT_ID('[BBLive].[dbo].[SFA_SyncDashboard]', 'U') IS NULL
        BEGIN
            CREATE TABLE [BBLive].[dbo].[SFA_SyncDashboard] (
                Id           INT IDENTITY(1,1) PRIMARY KEY,
                SyncType     NVARCHAR(50)   NOT NULL,
                DisplayName  NVARCHAR(100)  NOT NULL,
                IsPushed     BIT            NOT NULL DEFAULT 0,
                LastStatus   NVARCHAR(20)   NULL,
                LastPushedAt DATETIME       NULL,
                TotalRecords INT            NULL,
                SuccessCount INT            NULL,
                FailedCount  INT            NULL,
                ErrorMessage NVARCHAR(2000) NULL,
                UpdatedAt    DATETIME       NOT NULL DEFAULT GETDATE(),
                CONSTRAINT UQ_SFA_SyncType UNIQUE (SyncType)
            )
        END
    `);

    for (const s of SYNC_TYPES) {
        await p.request()
            .input('syncType',    sql.NVarChar(50),  s.syncType)
            .input('displayName', sql.NVarChar(100), s.displayName)
            .query(`
                IF NOT EXISTS (
                    SELECT 1 FROM [BBLive].[dbo].[SFA_SyncDashboard]
                    WHERE SyncType = @syncType
                )
                INSERT INTO [BBLive].[dbo].[SFA_SyncDashboard] (SyncType, DisplayName)
                VALUES (@syncType, @displayName)
            `);
    }
}

async function getAllStatus() {
    await ensureTable();
    const p = await getPool();
    const { recordset } = await p.request().query(
        `SELECT * FROM [BBLive].[dbo].[SFA_SyncDashboard] ORDER BY Id`
    );
    return recordset;
}

async function setRunning(syncType) {
    const p = await getPool();
    await p.request()
        .input('syncType', sql.NVarChar(50), syncType)
        .query(`
            UPDATE [BBLive].[dbo].[SFA_SyncDashboard]
            SET LastStatus = 'Running', UpdatedAt = GETDATE()
            WHERE SyncType = @syncType
        `);
}

async function setResult(syncType, { success, totalRecords, successCount, failedCount, errorMessage }) {
    const p = await getPool();
    const errMsg = errorMessage ? String(errorMessage).slice(0, 2000) : null;

    await p.request()
        .input('syncType',     sql.NVarChar(50),   syncType)
        .input('isPushed',     sql.Bit,             success ? 1 : 0)
        .input('lastStatus',   sql.NVarChar(20),    success ? 'Success' : 'Failed')
        .input('totalRecords', sql.Int,             totalRecords ?? null)
        .input('successCount', sql.Int,             successCount ?? null)
        .input('failedCount',  sql.Int,             failedCount  ?? null)
        .input('errorMessage', sql.NVarChar(2000),  errMsg)
        .query(`
            UPDATE [BBLive].[dbo].[SFA_SyncDashboard]
            SET
                IsPushed     = @isPushed,
                LastStatus   = @lastStatus,
                LastPushedAt = GETDATE(),
                TotalRecords = @totalRecords,
                SuccessCount = @successCount,
                FailedCount  = @failedCount,
                ErrorMessage = @errorMessage,
                UpdatedAt    = GETDATE()
            WHERE SyncType = @syncType
        `);
}

module.exports = { getAllStatus, setRunning, setResult, ensureTable };
