const MySQL = require('./MySQL');
const PostgreSQL = require('./PostgreSQL');

const databaseTypes = {
    [MySQL.getType()]: MySQL,
    [PostgreSQL.getType()]: PostgreSQL
};

class DatabaseFactory {
    static getDatabaseClass(type) {
        const DbClass = databaseTypes[type.toLowerCase()];
        if (!DbClass) throw new Error(`不支持的数据库类型: ${type}`);
        return DbClass;
    }

    static async createConnection(type, config) {
        const DbClass = this.getDatabaseClass(type);
        const instance = new DbClass(config);
        await instance.connect();
        return instance;
    }
}

module.exports = DatabaseFactory;