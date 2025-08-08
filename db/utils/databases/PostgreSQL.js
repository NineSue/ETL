const { Client } = require('pg');
const BaseDatabase = require('./BaseDatabase');

class PostgreSQL extends BaseDatabase {
    async connect() {
        this.connection = new Client({
            host: this.config.host,
            port: this.config.port || 5432,
            user: this.config.username,
            password: this.config.password,
            database: this.config.database
        });
        await this.connection.connect();
        return this;
    }

    async query(sql, params) {
        const result = await this.connection.query(sql, params);
        return {
            rows: result.rows,
            fields: result.fields.map(f => ({
                name: f.name,
                type: f.dataTypeID
            })),
            rowCount: result.rowCount || result.rows.length
        };
    }

    async testConnection() {
        const client = new Client({
            host: this.config.host,
            user: this.config.username,
            password: this.config.password,
            database: this.config.database,
            connectTimeout: 5000
        });
        try {
            await client.connect();
            await client.query('SELECT 1');
            return true;
        } finally {
            await client.end();
        }
    }

    async listTables() {
        const { rows } = await this.query(
            `SELECT table_name
             FROM information_schema.tables
             WHERE table_schema NOT IN ('pg_catalog', 'information_schema')`
        );
        return rows.map(row => row.table_name);
    }

    async getTableStructure(tableName) {
        const { rows } = await this.query(
            `SELECT column_name, data_type, is_nullable, column_default
             FROM information_schema.columns
             WHERE table_name = $1`,
            [tableName]
        );
        return rows.map(row => ({
            name: row.column_name,
            type: row.data_type,
            nullable: row.is_nullable === 'YES',
            default: row.column_default
        }));
    }

    static getType() {
        return 'postgresql';
    }
}

module.exports = PostgreSQL;