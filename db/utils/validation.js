// utils/validation.js
function validateConnectionConfig({ name, type, config }) {
    // 基础字段检查
    if (!name?.trim() || !type?.trim() || !config) {
        return {
            code: 400,
            message: '缺少必填字段: 连接名称、类型或配置'
        };
    }

    // 类型白名单检查
    const validTypes = ['mysql', 'postgresql']; // 可扩展其他类型
    if (!validTypes.includes(type.toLowerCase())) {
        return {
            code: 400,
            message: `不支持的数据库类型，当前支持: ${validTypes.join(', ')}`
        };
    }

    // 配置完整性检查
    if (!config.host || !config.database) {
        return {
            code: 400,
            message: '配置必须包含 host 和 database 字段'
        };
    }

    return null;
}

function validateSQL(sql) {
    const sanitized = sql.trim().toUpperCase();

    // 空查询检查
    if (!sanitized) {
        return { code: 400, message: 'SQL不能为空' };
    }

    // 仅允许SELECT查询（预览功能安全限制）
    if (!sanitized.startsWith('SELECT')) {
        return { code: 403, message: '预览功能仅支持SELECT查询' };
    }

    // 高危操作检查
    const forbiddenKeywords = ['DROP', 'DELETE', 'UPDATE', 'TRUNCATE', 'INSERT'];
    if (forbiddenKeywords.some(kw => sanitized.includes(kw))) {
        return { code: 403, message: '包含禁止的操作类型' };
    }

    return null;
}

module.exports = {
    validateConnectionConfig,
    validateSQL
};