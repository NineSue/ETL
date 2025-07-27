// src/utils/logger.js
const { createLogger, format, transports } = require('winston');
const path = require('path');

// 日志级别阶梯
const levels = {
    error: 0,
    warn: 1,
    info: 2,
    http: 3,
    debug: 4
};

// 根据环境确定日志级别
const getLevel = () => {
    const env = process.env.NODE_ENV || 'development';
    return env === 'development' ? 'debug' : 'info';
};

// 日志格式
const logFormat = format.combine(
    format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    format.errors({ stack: true }),
    format.splat(),
    format.json()
);

// 控制台日志特殊格式（开发环境）
const consoleFormat = format.combine(
    format.colorize(),
    format.printf(
        ({ timestamp, level, message, stack }) =>
            `${timestamp} [${level}]: ${message}${stack ? `\n${stack}` : ''}`
    )
);

const logger = createLogger({
    level: getLevel(),
    levels,
    format: logFormat,
    transports: [
        // 错误日志（持久化）
        new transports.File({
            filename: path.join(__dirname, '../../logs/error.log'),
            level: 'error',
            maxsize: 1024 * 1024 * 5 // 5MB
        }),
        // 综合日志（持久化）
        new transports.File({
            filename: path.join(__dirname, '../../logs/combined.log'),
            maxsize: 1024 * 1024 * 10 // 10MB
        }),
        // 控制台输出（仅开发环境）
        ...(process.env.NODE_ENV === 'development'
            ? [new transports.Console({ format: consoleFormat })]
            : [])
    ]
});

// 处理未捕获异常
process.on('uncaughtException', (error) => {
    logger.error('Uncaught Exception:', error);
    process.exit(1);
});

// 处理未处理的Promise拒绝
process.on('unhandledRejection', (reason) => {
    logger.error('Unhandled Rejection:', reason);
});

module.exports = logger;