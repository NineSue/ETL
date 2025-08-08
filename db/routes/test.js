const express = require('express');
const router = express.Router();
const connectionManager = require('../utils/dbUtils');
const { validateConnectionConfig } = require('../utils/validation');

router.post('/', async (req, res) => {
    const { name, type, config } = req.body;

    try {
        // 1. 参数验证
        const validationError = validateConnectionConfig({ name, type, config });
        if (validationError) {
            return res.status(validationError.code).json(validationError);
        }

        // 2. 测试连接
        const result = await connectionManager.testConnection(type, config);

        if (result.success) {
            res.json({
                code: 200,
                message: `${type} connection successful`, // 动态显示数据库类型
                data: {
                    database: config.database,
                    port: config.port || (type === 'mysql' ? 3306 : 5432) // 自动填充默认端口
                }
            });
        } else {
            res.status(400).json({
                code: 400,
                message: result.message || 'Connection test failed',
                details: result.details
            });
        }

    } catch (error) {
        console.error('Connection test error:', error);
        res.status(500).json({
            code: 500,
            message: 'Internal server error during connection test'
        });
    }
});

module.exports = router;