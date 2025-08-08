const odbc = require('odbc');
const BaseDatabase = require('./BaseDatabase');

class AccessDatabase extends BaseDatabase {
    async connect() {
        // Access数据库连接配置
        const connectionString = this.buildConnectionString();

        try {
            this.connection = await odbc.connect(connectionString);
            return this;
        } catch (error) {
            throw new Error(`连接Access数据库失败: ${error.message}`);
        }
    }

    buildConnectionString() {
        // 验证必要配置
        if (!this.config.filePath) {
            throw new Error('Access数据库配置必须包含filePath');
        }

        // 构建连接字符串
        let connectionString = `Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=${this.config.filePath};`;

        // 可选密码
        if (this.config.password) {
            connectionString += `PWD=${this.config.password};`;
        }

        return connectionString;
    }

    async query(sql, params = []) {
        if (!this.connection) {
            throw new Error('未建立数据库连接');
        }

        try {
            const result = await this.connection.query(sql, params);

            // 格式化结果以保持与MySQL类似的返回结构
            return {
                rows: result,
                fields: result.columns ? result.columns.map(col => ({
                    name: col.name,
                    type: col.dataType
                })) : [],
                rowCount: result.length || (result.affectedRows || 0)
            };
        } catch (error) {
            throw new Error(`执行查询失败: ${error.message}`);
        }
    }

    async testConnection() {
        const connectionString = this.buildConnectionString();

        try {
            const testConn = await odbc.connect(connectionString);
            await testConn.query('SELECT 1 FROM MSysObjects WHERE 1=0');
            await testConn.close();
            return true;
        } catch (error) {
            return false;
        }
    }

    async listTables() {
        // Access系统表查询获取所有用户表
        const { rows } = await this.query(
            "SELECT Name FROM MSysObjects WHERE Type=1 AND Flags=0 AND Name NOT LIKE '~*' AND Name NOT LIKE 'MSys*'"
        );
        return rows.map(row => row.Name);
    }

    async getTableStructure(tableName) {
        // 获取表结构信息
        const { rows } = await this.query(
            `SELECT * FROM [${tableName}] WHERE 1=0`
        );

        // 从结果中提取列信息
        if (!rows.columns) {
            throw new Error('无法获取表结构信息');
        }

        return rows.columns.map(column => ({
            name: column.name,
            type: this.mapAccessType(column.dataType),
            nullable: true, // Access不直接提供是否为NULL的信息
            key: '',        // Access的主键信息需要额外查询
            default: '',    // Access的默认值需要额外查询
            extra: ''       // Access的额外信息
        }));
    }

    // 映射Access数据类型到通用类型
    mapAccessType(accessType) {
        const typeMap = {
            2: 'INTEGER',    // SmallInt
            3: 'INTEGER',    // Integer
            4: 'REAL',       // Single
            5: 'DOUBLE',     // Double
            6: 'CURRENCY',   // Currency
            7: 'DATETIME',   // DateTime
            11: 'BOOLEAN',   // Boolean
            17: 'BINARY',    // Byte
            72: 'GUID',      // GUID
            130: 'TEXT',     // Text (Wide)
            131: 'NUMERIC',  // Decimal
            133: 'DATETIME', // Date
            134: 'TIME',     // Time
            135: 'DATETIME'  // Timestamp
        };

        return typeMap[accessType] || 'UNKNOWN';
    }

    static getType() {
        return 'access';
    }
}

module.exports = AccessDatabase;