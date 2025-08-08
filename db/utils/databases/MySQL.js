const mysql = require('mysql2/promise');
const BaseDatabase = require('./BaseDatabase');

class MySQL extends BaseDatabase {
    async connect() {
        this.connection = await mysql.createConnection({
            host: this.config.host,
            port: this.config.port || 3306,
            user: this.config.username,
            password: this.config.password,
            database: this.config.database
        });
        return this;
    }

    async query(sql, params) {
        const [rows, fields] = await this.connection.execute(sql, params);
        return {
            rows,
            fields: fields.map(f => ({
                name: f.name,
                type: f.type
            })),
            rowCount: rows.affectedRows || rows.length
        };
    }

    async testConnection() {
        const conn = await mysql.createConnection({
            host: this.config.host,
            user: this.config.username,
            password: this.config.password,
            database: this.config.database,
            connectTimeout: 5000
        });
        try {
            await conn.query('SELECT 1');
            return true;
        } finally {
            await conn.end();
        }
    }

    async listTables() {
        const { rows } = await this.query('SHOW TABLES');
        return rows.map(row => row[`Tables_in_${this.config.database}`]);
    }

    async getTableStructure(tableName) {
        const { rows } = await this.query(`DESCRIBE ${tableName}`);
        return rows.map(row => ({
            name: row.Field,
            type: row.Type,
            nullable: row.Null === 'YES',
            key: row.Key,
            default: row.Default,
            extra: row.Extra
        }));
    }

    static getType() {
        return 'mysql';
    }
}

module.exports = MySQL;