const express = require('express');
const router = express.Router();
const connectionManager = require('../utils/dbUtils');

router.post('/execute', async (req, res) => {
    const { connectionId, sql, batch } = req.body;

    // 参数验证
    if (!connectionId) {
        return res.status(400).json({
            code: 400,
            message: '缺少必要参数: connectionId'
        });
    }

    try {
        const dbInstance = await connectionManager.getDynamicConnection(connectionId);
        const statements = [];

        // 解析SQL语句
        if (batch) {
            statements.push(...parseBatchInput(batch));
        } else if (sql) {
            statements.push(sql.trim());
        } else {
            return res.status(400).json({
                code: 400,
                message: '必须提供 sql 或 batch 参数'
            });
        }

        // 执行SQL语句
        const results = [];
        for (const statement of statements) {
            if (!statement) continue;

            try {
                const [result] = await dbInstance.query(statement);
                results.push({
                    sql: statement,
                    affectedRows: result?.affectedRows ?? 0,
                    insertId: result?.insertId ?? null,
                    success: true
                });
            } catch (stmtError) {
                const errorResponse = handleStatementError(stmtError, statement);
                if (errorResponse.code === 409 || errorResponse.code === 404) {
                    // 对于数据已存在和表不存在错误，直接返回错误响应
                    return res.status(errorResponse.code).json(errorResponse);
                }
                results.push({
                    sql: statement,
                    error: errorResponse.message,
                    success: false
                });
            }
        }

        // 返回统一格式
        res.json({
            code: 200,
            data: {
                results: results,
                summary: {
                    total: statements.length,
                    success: results.filter(r => r.success).length
                }
            }
        });

    } catch (error) {
        const errorResponse = handleStatementError(error, sql || batch);
        res.status(errorResponse.code).json(errorResponse);
    }
});

// 语句级错误处理
function handleStatementError(error, sql) {
    let code = 500;
    let message = error.message;

    if (error.message.includes('Duplicate entry')) {
        code = 409; // 冲突
        message = `数据已存在: ${error.message.split('for key')[0].trim()}`;
    } else if (error.message.includes('ER_NO_SUCH_TABLE') ||
        error.message.includes('Table') && error.message.includes('doesn\'t exist')) {
        code = 404; // 未找到
        message = '表不存在，请检查表名是否正确';
    }

    return {
        code: code,
        message: `SQL执行失败: ${message}`,
        ...(code === 500 && { sql: sql })
    };
}

// 解析批量输入
function parseBatchInput(input) {
    if (Array.isArray(input)) {
        return input.map(s => s?.trim()).filter(s => s && s.length > 0);
    }
    if (typeof input === 'string') {
        return input.split(';').map(s => s.trim()).filter(s => s.length > 0);
    }
    throw new Error('batch参数必须是数组或分号分隔的SQL字符串');
}

module.exports = router;