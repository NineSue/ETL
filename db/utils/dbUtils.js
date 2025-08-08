const mysql = require('mysql2/promise');
const DatabaseFactory = require('./databases/databaseFactory');
const getDatabaseClass = (type) => DatabaseFactory.getDatabaseClass(type);
const logger = require('../utils/logger');

class ConnectionManager {
    constructor() {
        this._initializeMainPool();
        this.activeConnections = new Map(); // 改用Map提高性能
    }

    // 初始化主连接池
    _initializeMainPool() {
        this.mainPool = mysql.createPool({
            host: process.env.DB_HOST || 'localhost',
            user: process.env.DB_USER || 'root',
            password: process.env.DB_PASSWORD,
            database: process.env.DB_NAME || 'connection_manager',
            waitForConnections: true,
            connectionLimit: parseInt(process.env.DB_POOL_LIMIT) || 10,
            queueLimit: 0,
            timezone: '+00:00' // 使用UTC时区
        });

        // 连接池事件监听
        this.mainPool.on('acquire', (connection) => {
            logger.debug(`Connection ${connection.threadId} acquired`);
        });

        this.mainPool.on('release', (connection) => {
            logger.debug(`Connection ${connection.threadId} released`);
        });
    }

    // 主数据库操作封装
    async get(sql, params) {
        try {
            const [rows] = await this.mainPool.query(sql, params);
            return rows[0];
        } catch (error) {
            logger.error('Database get operation failed', { sql, params, error });
            throw this._wrapDatabaseError(error);
        }
    }

    async query(sql, params) {
        try {
            const [rows] = await this.mainPool.query(sql, params);
            return rows;
        } catch (error) {
            logger.error('Database query failed', { sql, params, error });
            throw this._wrapDatabaseError(error);
        }
    }

    async run(sql, params) {
        const conn = await this.mainPool.getConnection();
        try {
            const [result] = await conn.query(sql, params);
            return {
                insertId: result.insertId,
                affectedRows: result.affectedRows,
                warningCount: result.warningStatus
            };
        } catch (error) {
            logger.error('Database run operation failed', { sql, params, error });
            throw this._wrapDatabaseError(error);
        } finally {
            conn.release();
        }
    }

    // 连接管理增强版
    async getDynamicConnection(connectionId) {
        if (!connectionId) {
            throw new Error('Connection ID is required');
        }

        // 检查缓存
        if (this.activeConnections.has(connectionId)) {
            const cached = this.activeConnections.get(connectionId);
            if (await this._isConnectionAlive(cached)) {
                return cached;
            }
            await this.releaseConnection(connectionId);
        }

        // 获取配置
        const connConfig = await this._getConnectionConfig(connectionId);
        if (!connConfig) {
            throw new Error(`Connection config not found for ID: ${connectionId}`);
        }

        // 创建连接
        try {
            const instance = await this._createDatabaseInstance(connConfig);
            this.activeConnections.set(connectionId, instance);
            return instance;
        } catch (error) {
            logger.error('Failed to create database connection', {
                connectionId,
                error
            });
            throw this._wrapDatabaseError(error);
        }
    }

    async releaseConnection(connectionId) {
        if (!this.activeConnections.has(connectionId)) return;

        try {
            const instance = this.activeConnections.get(connectionId);
            await instance.end();
            this.activeConnections.delete(connectionId);
        } catch (error) {
            logger.error('Failed to release connection', {
                connectionId,
                error
            });
        }
    }

    async testConnection(type, config) {
        try {
            const DbClass = getDatabaseClass(type);
            const instance = new DbClass(config);
            const isAlive = await instance.testConnection();
            return {
                success: isAlive,
                config: config // 返回配置信息供路由使用
            };
        } catch (error) {
            return {
                success: false,
                message: error.message,
                details: error.stack
            };
        }
    }

    async getAllConnections() {
        const rows = await this.query(`
            SELECT id, name, type, config,
                   DATE_FORMAT(created_at, '%Y-%m-%d %H:%i:%s') as created_at,
                   DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s') as updated_at
            FROM connections
        `);

        return rows.map(row => ({
            ...row,
            config: this._safeParseJSON(row.config)
        }));
    }

    // 私有方法
    async _getConnectionConfig(connectionId) {
        const config = await this.get(
            'SELECT * FROM connections WHERE id = ?',
            [connectionId]
        );
        if (!config) return null;

        try {
            return {
                ...config,
                config: this._safeParseJSON(config.config)
            };
        } catch (error) {
            logger.error('Invalid connection config format', {
                connectionId,
                error
            });
            throw new Error('Invalid connection configuration');
        }
    }

    async _createDatabaseInstance(connConfig) {
        const DbClass = getDatabaseClass(connConfig.type);
        const instance = new DbClass(connConfig.config);
        await instance.connect();
        return instance;
    }

    async _isConnectionAlive(instance) {
        try {
            return await instance.testConnection();
        } catch {
            return false;
        }
    }

    _safeParseJSON(jsonString) {
        try {
            return JSON.parse(jsonString);
        } catch {
            throw new Error('Invalid JSON configuration');
        }
    }

    _wrapDatabaseError(error) {
        error.isDatabaseError = true;
        return error;
    }

    // 清理资源
    async shutdown() {
        // 关闭所有动态连接
        await Promise.all(
            Array.from(this.activeConnections.keys()).map(id =>
                this.releaseConnection(id)
            )
        );

        // 关闭主连接池
        if (this.mainPool) {
            await this.mainPool.end();
        }
    }
}

// 导出单例实例
module.exports = new ConnectionManager();