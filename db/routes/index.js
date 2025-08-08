const express = require('express');
const router = express.Router();

const testRoute = require('./test');
const connectRoute = require('./connect');
const queryRoute = require('./query'); // 添加查询路由
const outputRoute = require('./output'); // 添加输出路由

router.use('/test', testRoute);
router.use('/connect', connectRoute);
router.use('/query', queryRoute); // 注册查询路由
router.use('/output', outputRoute); // 注册输出路由

module.exports = router;