const sql = require('mssql');

const sqlConfig = {
    user: 'cultyvate',
    password: process.env.DB_PASSWORD,
    database: 'cultYvate',
    server: 'cultyvate-sql.database.windows.net',
    pool: {
        max: 10,
        min: 0,
        idleTimeoutMillis: 30000
    },
    options: {
        encrypt: true, // for azure
        trustServerCertificate: true // change to true for local dev / self-signed certs
    }
}

module.exports = { sqlConfig };

