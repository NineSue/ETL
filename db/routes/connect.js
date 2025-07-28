const express = require('express');
const router = express.Router();
const connectionManager = require('../utils/dbUtils');
const { validateConnectionConfig } = require('../utils/validation');

// 保存数据库连接配置
router.post('/', async (req, res) => {
    const { name, type, config } = req.body;

    try {
        // 1. 参数验证
        const validationError = validateConnectionConfig({ name, type, config });
        if (validationError) {
            return res.status(400).json(validationError);
        }

        // 2. 测试连接有效性
        const testResult = await connectionManager.testConnection(type, config);
        if (!testResult.success) {
            return res.status(400).json({
                code: 400,
                message: '连接测试失败: ' + testResult.message
            });
        }

        // 3. 检查名称唯一性
        const existingConn = await connectionManager.get(
            'SELECT * FROM connections WHERE name = ? LIMIT 1',
            [name]
        );
        if (existingConn) {
            return res.status(400).json({
                code: 400,
                message: '连接名称已存在'
            });
        }

        // 4. 保存配置
        const result = await connectionManager.run(
            `INSERT INTO connections (name, type, config, created_at, updated_at) 
             VALUES (?, ?, ?, NOW(), NOW())`,
            [name, type, JSON.stringify(config)]
        );

        res.json({
            code: 200,
            message: "数据库连接配置保存成功",
            data: {
                id: result.insertId,
                name,
                type,
                config
            }
        });

    } catch (error) {
        console.error('保存连接配置出错:', {
            error: error.message,
            stack: error.stack,
            config: req.body
        });
        res.status(500).json({
            code: 500,
            message: '服务器内部错误'
        });
    }
});

// 获取所有连接配置
router.get('/', async (req, res) => {
    try {
        const connections = await connectionManager.getAllConnections();
        res.json({
            code: 200,
            data: connections
        });
    } catch (error) {
        console.error('获取连接配置出错:', error);
        res.status(500).json({
            code: 500,
            message: '获取连接配置失败'
        });
    }
});

// 更新连接配置
router.put('/:id', async (req, res) => {
    const { id } = req.params;
    const { name, type, config } = req.body;

    try {
        // 1. 参数验证
        const validationError = validateConnectionConfig({ name, type, config });
        if (validationError) {
            return res.status(400).json(validationError);
        }

        // 2. 测试连接
        const testResult = await connectionManager.testConnection(type, config);
        if (!testResult.success) {
            return res.status(400).json({
                code: 400,
                message: '连接测试失败: ' + testResult.message
            });
        }

        // 3. 检查名称冲突
        const existingConn = await connectionManager.get(
            'SELECT * FROM connections WHERE name = ? AND id != ?',
            [name, id]
        );
        if (existingConn) {
            return res.status(400).json({
                code: 400,
                message: '连接名称已存在'
            });
        }

        // 4. 更新配置
        await connectionManager.run(
            `UPDATE connections
             SET name = ?, type = ?, config = ?, updated_at = NOW()
             WHERE id = ?`,
            [name, type, JSON.stringify(config), id]
        );

        res.json({
            code: 200,
            data: { id, name, type, config }
        });

    } catch (error) {
        console.error('更新连接配置出错:', {
            error: error.message,
            stack: error.stack,
            id,
            config: req.body
        });
        res.status(500).json({
            code: 500,
            message: '更新配置失败'
        });
    }
});

// 删除连接配置
router.delete('/:id', async (req, res) => {
    const { id } = req.params;

    try {
        // 1. 检查连接是否存在
        const conn = await connectionManager.get(
            'SELECT * FROM connections WHERE id = ?',
            [id]
        );
        if (!conn) {
            return res.status(404).json({
                code: 404,
                message: '连接配置不存在'
            });
        }

        // 2. 删除配置
        await connectionManager.run(
            'DELETE FROM connections WHERE id = ?',
            [id]
        );

        // 3. 关闭活跃连接（如果存在）
        await connectionManager.releaseConnection(id);

        res.json({
            code: 200,
            message: '删除成功'
        });

    } catch (error) {
        console.error('删除连接配置出错:', {
            error: error.message,
            stack: error.stack,
            id
        });
        res.status(500).json({
            code: 500,
            message: '删除失败'
        });
    }
});

module.exports = router;