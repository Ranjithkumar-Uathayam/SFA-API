require('dotenv').config();

const config = {
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    server: process.env.DB_SERVER,
    database: process.env.DB_NAME,
    port: parseInt(process.env.DB_PORT, 10) || 1433,
    options: {
        encrypt: false, // Use true for Azure, false for local dev usually
        trustServerCertificate: true // Change to false for production if using valid certs
    }
};

module.exports = config;
