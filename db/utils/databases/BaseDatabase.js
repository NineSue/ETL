class BaseDatabase {
    constructor(config) {
        this.config = config;
        this.connection = null;
    }

    async connect() {
        throw new Error('connect() must be implemented by subclass');
    }

    async query(sql, params) {
        throw new Error('query() must be implemented by subclass');
    }

    async testConnection() {
        throw new Error('testConnection() must be implemented by subclass');
    }

    async end() {
        if (this.connection) await this.connection.end();
    }

    async listTables() {
        throw new Error('listTables() must be implemented by subclass');
    }

    async getTableStructure(tableName) {
        throw new Error('getTableStructure() must be implemented by subclass');
    }

    static getType() {
        throw new Error('getType() must be implemented by subclass');
    }
}

module.exports = BaseDatabase;