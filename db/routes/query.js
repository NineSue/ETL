const express = require('express');
const router = express.Router();
const connectionManager = require('../utils/dbUtils');
const { validateSQL } = require('../utils/validation');

// 执行SQL查询（预览）
router.post('/preview', async (req, res) => {
    const { connectionId, sql } = req.body;

    try {
        // 1. 参数校验
        if (!connectionId || !sql?.trim()) {
            return res.status(400).json({ code: 400, message: '参数不完整' });
        }

        // 2. 获取连接实例
        const dbInstance = await connectionManager.getDynamicConnection(connectionId);

        // 3. SQL验证（安全性和语法）
        const validationError = validateSQL(sql);
        if (validationError) {
            return res.status(403).json(validationError);
        }

        // 4. 执行查询
        const { rows, fields, rowCount } = await dbInstance.query(sql);

        res.json({
            code: 200,
            data: {
                columns: fields || [],
                rows: rows || [],
                sql,
                connectionId,
                rowCount: rowCount || 0
            }
        });

    } catch (error) {
        console.error('查询执行错误:', {
            error: error.message,
            stack: error.stack,
            sql,
            connectionId
        });

        res.status(500).json({
            code: 500,
            message: `查询执行失败: ${error.message}`,
            sql: sql
        });
    }
});

// 获取数据库表列表
router.post('/list-tables', async (req, res) => {
    const { connectionId } = req.body;

    try {
        // 1. 参数验证
        if (!connectionId) {
            throw new Error('缺少connectionId参数');
        }

        // 2. 获取数据库实例
        const dbInstance = await connectionManager.getDynamicConnection(connectionId);

        // 3. 获取表列表
        const tables = await dbInstance.listTables();

        res.json({
            code: 200,
            data: {
                tables,
                connectionId
            }
        });

    } catch (error) {
        console.error('获取表列表失败:', error);
        res.status(500).json({
            code: 500,
            message: `获取表列表失败: ${error.message}`
        });
    }
});

// 获取表结构信息
router.post('/table-structure', async (req, res) => {
    const { connectionId, tableName } = req.body;

    try {
        // 1. 参数验证
        if (!connectionId || !tableName) {
            throw new Error('缺少必要参数');
        }

        // 2. 获取数据库实例
        const dbInstance = await connectionManager.getDynamicConnection(connectionId);

        // 3. 获取表结构
        const columns = await dbInstance.getTableStructure(tableName);

        res.json({
            code: 200,
            data: {
                tableName,
                columns
            }
        });

    } catch (error) {
        console.error('获取表结构失败:', {
            error: error.message,
            stack: error.stack,
            connectionId,
            tableName
        });

        res.status(500).json({
            code: 500,
            message: `获取表结构失败: ${error.message}`
        });
    }
});

module.exports = router;