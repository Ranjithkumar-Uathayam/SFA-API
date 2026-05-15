const sql = require('mssql');
const config = require('../config/dbConfig');

let pool;
async function getPool() {
    if (!pool) {
        pool = await sql.connect(config);
    }
    return pool;
}

async function ensureTable() {
    const p = await getPool();
    await p.request().query(`
        IF OBJECT_ID('[BBLive].[dbo].[SFA_RecordPushStatus]', 'U') IS NULL
        CREATE TABLE [BBLive].[dbo].[SFA_RecordPushStatus] (
            Id           INT IDENTITY(1,1) PRIMARY KEY,
            MasterType   NVARCHAR(50)   NOT NULL,
            RecordKey    NVARCHAR(100)  NOT NULL,
            PushStatus   NVARCHAR(20)   NOT NULL DEFAULT 'Pending',
            LastPushedAt DATETIME       NULL,
            ErrorMessage NVARCHAR(2000) NULL,
            UpdatedAt    DATETIME       NOT NULL DEFAULT GETDATE(),
            CONSTRAINT UQ_MasterRecord UNIQUE (MasterType, RecordKey)
        )
    `);
}

async function setStatus(masterType, recordKey, status, errorMessage = null) {
    await ensureTable();
    const p = await getPool();
    const errMsg = errorMessage ? String(errorMessage).slice(0, 2000) : null;

    await p.request()
        .input('masterType',   sql.NVarChar(50),   masterType)
        .input('recordKey',    sql.NVarChar(100),  recordKey)
        .input('status',       sql.NVarChar(20),   status)
        .input('pushedAt',     sql.DateTime,       status === 'Pushed' || status === 'Failed' ? new Date() : null)
        .input('errorMessage', sql.NVarChar(2000), errMsg)
        .query(`
            MERGE [BBLive].[dbo].[SFA_RecordPushStatus] AS tgt
            USING (SELECT @masterType AS MT, @recordKey AS RK) AS src ON tgt.MasterType = src.MT AND tgt.RecordKey = src.RK
            WHEN MATCHED THEN
                UPDATE SET PushStatus = @status, LastPushedAt = ISNULL(@pushedAt, LastPushedAt),
                           ErrorMessage = @errorMessage, UpdatedAt = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (MasterType, RecordKey, PushStatus, LastPushedAt, ErrorMessage)
                VALUES (@masterType, @recordKey, @status, @pushedAt, @errorMessage);
        `);
}

// Bulk-upsert results from sfService — expects [{recordKey, success, errorMessage}]
async function setBulkResults(masterType, results) {
    for (const r of results) {
        await setStatus(
            masterType,
            r.recordKey,
            r.success ? 'Pushed' : 'Failed',
            r.errorMessage ?? null
        );
    }
}

// Mark many records as 'Pushing' in one go (before push starts)
async function setManyPushing(masterType, recordKeys) {
    for (const key of recordKeys) {
        await setStatus(masterType, key, 'Pushing');
    }
}

// Summary counts for a master type (overall, no filter)
async function getSummary(masterType) {
    await ensureTable();
    const p = await getPool();
    return p.request()
        .input('masterType', sql.NVarChar(50), masterType)
        .query(`
            SELECT PushStatus, COUNT(*) AS Count
            FROM [BBLive].[dbo].[SFA_RecordPushStatus]
            WHERE MasterType = @masterType
            GROUP BY PushStatus
        `);
}

module.exports = { ensureTable, setStatus, setBulkResults, setManyPushing, getSummary };
