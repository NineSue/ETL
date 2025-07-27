package plugin.output;

import anno.Output;
import core.Channel;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IOutput;
import tool.Log;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.Condition;

@Output(type = "sqlfile")
public class SQLFileOutput implements IOutput {

    /**
     * 支持的数据库类型枚举
     */
    public enum DatabaseType {
        MYSQL,       // MySQL数据库
        POSTGRESQL   // PostgreSQL数据库
    }

    // 配置参数
    private DatabaseType dbType;          // 数据库类型
    private String filename;              // 输出文件名
    private String dateFormat;            // 日期格式
    private String tableName;             // 目标表名
    private boolean createTable;          // 是否自动建表
    private boolean overwrite;            // 是否覆盖已有文件
    private boolean createParentDir;      // 是否自动创建父目录

    // 同步控制
    private volatile boolean initialized = false;  // 初始化完成标志
    private final ReentrantLock initLock = new ReentrantLock();  // 初始化锁
    private final Condition initCondition = initLock.newCondition();  // 初始化条件

    @Override
    public void init(Map<String, Object> cfg) {
        initLock.lock();
        try {
            // 1. 解析数据库类型配置（默认MySQL）
            String dbTypeStr = ((String) cfg.getOrDefault("dbtype", "mysql")).toUpperCase();
            try {
                this.dbType = DatabaseType.valueOf(dbTypeStr);
                Log.debug("SQLFileOutput", "数据库类型设置为: " + dbType);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("不支持的数据库类型: " + dbTypeStr +
                        "，支持的类型: " + Arrays.toString(DatabaseType.values()));
            }

            // 2. 校验必要配置项
            this.filename = (String) cfg.get("filename");
            if (filename == null) {
                throw new IllegalArgumentException("文件名配置(filename)不能为空");
            }
            Log.debug("SQLFileOutput", "输出文件路径: " + filename);

            this.tableName = (String) cfg.get("table_name");
            if (tableName == null) {
                throw new IllegalArgumentException("表名配置(table_name)不能为空");
            }
            Log.debug("SQLFileOutput", "目标表名: " + tableName);

            // 3. 设置可选配置（带默认值）
            this.dateFormat = (String) cfg.getOrDefault("date_format", "yyyyMMdd");
            this.createTable = (Boolean) cfg.getOrDefault("create_table", false);
            this.overwrite = (Boolean) cfg.getOrDefault("overwrite", false);
            this.createParentDir = (Boolean) cfg.getOrDefault("create_parent_dir", true);

            Log.debug("SQLFileOutput", String.format("配置参数: 日期格式=%s, 自动建表=%b, 覆盖模式=%b, 创建目录=%b",
                    dateFormat, createTable, overwrite, createParentDir));

            // 4. 标记初始化完成
            this.initialized = true;
            initCondition.signalAll();
            Log.info("SQLFileOutput", "插件初始化完成");
        } finally {
            initLock.unlock();
        }
    }

    @Override
    public void consume(Channel input) throws Exception {
        // 阶段1：等待初始化完成
        awaitInitialization();

        // 阶段2：准备输出文件
        String finalFilename = processFilenameWithDate();
        Path outputPath = prepareOutputFile(finalFilename);
        Log.info("SQLFileOutput", "开始写入" + dbType + "格式SQL文件: " + finalFilename);

        // 阶段3：数据预处理
        CountDownLatch completionLatch = new CountDownLatch(1);
        AtomicInteger processedRows = new AtomicInteger(0);
        RowSetTable header = awaitTableHeader(input);

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            // 写入建表语句（如果配置）
            if (createTable) {
                writeCreateTableStatement(writer, header);
            }

            // 注册数据处理器
            input.onReceive(
                    data -> processData(writer, data, header, processedRows),
                    () -> onProcessingComplete(completionLatch, processedRows, finalFilename)
            );

            // 等待处理完成（最多等待5分钟）
            if (!completionLatch.await(5, TimeUnit.MINUTES)) {
                Log.error("SQLFileOutput", "数据处理超时，已等待5分钟");
                throw new TimeoutException("数据处理超时");
            }
        } catch (IOException e) {
            Log.error("SQLFileOutput", "文件写入失败: " + e.getMessage());
            throw e;
        }
    }

    // ==================== 同步控制方法 ==================== //

    /**
     * 等待初始化完成（带超时）
     */
    private void awaitInitialization() throws InterruptedException {
        initLock.lock();
        try {
            long start = System.currentTimeMillis();
            while (!initialized) {
                Log.debug("SQLFileOutput", "等待初始化完成...");
                if (!initCondition.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("初始化等待超时（5秒）");
                }
            }
            Log.debug("SQLFileOutput", String.format("初始化等待完成，耗时%dms",
                    System.currentTimeMillis() - start));
        } finally {
            initLock.unlock();
        }
    }

    /**
     * 等待表头信息到达（带重试机制）
     */
    private RowSetTable awaitTableHeader(Channel input) throws InterruptedException {
        Log.debug("SQLFileOutput", "开始等待表头信息...");
        RowSetTable header = input.getHeader();
        int attempts = 0;
        while (header == null && attempts++ < 10) {
            Thread.sleep(100);
            header = input.getHeader();
        }

        if (header == null) {
            Log.error("SQLFileOutput", "等待表头信息超时（1秒）");
            throw new IllegalStateException("未能获取表头信息");
        }

        Log.debug("SQLFileOutput", "成功获取表头，字段数: " + header.getField().size());
        return header;
    }

    // ==================== 文件操作方法 ==================== //

    /**
     * 准备输出文件（包含目录检查和创建）
     */
    private Path prepareOutputFile(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        Log.debug("SQLFileOutput", "准备输出文件: " + path.toAbsolutePath());

        // 1. 处理父目录
        Path parent = path.getParent();
        if (parent != null) {
            if (createParentDir) {
                Log.debug("SQLFileOutput", "尝试创建目录: " + parent);
                Files.createDirectories(parent);
                Log.info("SQLFileOutput", "目录创建成功: " + parent);
            } else if (!Files.exists(parent)) {
                throw new IOException("父目录不存在且未启用自动创建: " + parent);
            }
        }

        // 2. 处理已存在文件
        if (Files.exists(path)) {
            if (overwrite) {
                Log.warn("SQLFileOutput", "文件已存在，将覆盖: " + path);
                Files.delete(path);
            } else {
                throw new IOException("文件已存在且未启用覆盖模式: " + path);
            }
        }

        // 3. 创建新文件
        Path createdFile = Files.createFile(path);
        Log.info("SQLFileOutput", "文件创建成功: " + createdFile);
        return createdFile;
    }

    /**
     * 写入建表SQL语句
     */
    private void writeCreateTableStatement(BufferedWriter writer, RowSetTable header) throws IOException {
        Log.debug("SQLFileOutput", "开始生成建表语句...");
        writer.write(generateCreateTableStatement(header));
        writer.newLine();
        writer.newLine();
        writer.flush();
        Log.info("SQLFileOutput", "建表语句写入完成");
    }

    // ==================== 数据处理方法 ==================== //

    /**
     * 处理接收到的数据
     */
    private void processData(BufferedWriter writer, Object data, RowSetTable header,
                             AtomicInteger counter) {
        try {
            if (data instanceof RowSetTable) {
                processBatch(writer, (RowSetTable) data, counter);
            } else if (data instanceof Row) {
                processSingleRow(writer, (Row) data, header, counter);
            } else if (data != null) {
                Log.warn("SQLFileOutput", "无法识别的数据类型: " + data.getClass().getName());
            }
        } catch (Exception e) {
            Log.error("SQLFileOutput", "数据处理异常: " + e.getMessage());
            throw new RuntimeException("数据处理失败", e);
        }
    }

    /**
     * 处理批量数据
     */
    private void processBatch(BufferedWriter writer, RowSetTable batch,
                              AtomicInteger counter) throws IOException {
        logDataBatch(batch);
        for (String sql : generateInsertStatements(batch)) {
            writer.write(sql);
            writer.newLine();
        }
        int added = batch.getRowList().size();
        counter.addAndGet(added);
        writer.flush();
        Log.debug("SQLFileOutput", "已写入 " + added + " 行数据");
    }

    /**
     * 处理单行数据
     */
    private void processSingleRow(BufferedWriter writer, Row row, RowSetTable header,
                                  AtomicInteger counter) throws IOException {
        RowSetTable wrapper = new RowSetTable(header.getField());
        wrapper.addRow(row);

        for (String sql : generateInsertStatements(wrapper)) {
            writer.write(sql);
            writer.newLine();
        }
        counter.incrementAndGet();
        writer.flush();
        Log.debug("SQLFileOutput", "已写入单行数据: " + row);
    }

    /**
     * 处理完成回调
     */
    private void onProcessingComplete(CountDownLatch latch, AtomicInteger counter,
                                      String filename) {
        int total = counter.get();
        Log.success("SQLFileOutput",
                String.format("数据处理完成，共写入 %d 行数据到文件: %s", total, filename));
        latch.countDown();
    }

    // ==================== SQL生成方法 ==================== //

    /**
     * 生成建表SQL语句
     */
    private String generateCreateTableStatement(RowSetTable header) {
        StringBuilder ddl = new StringBuilder("CREATE TABLE ");
        if (createTable) ddl.append("IF NOT EXISTS ");
        ddl.append(quoteIdentifier(tableName)).append(" (\n");

        List<String> fields = header.getField();
        for (int i = 0; i < fields.size(); i++) {
            String fieldName = fields.get(i);
            ddl.append("  ").append(quoteIdentifier(fieldName)).append(" ");
            ddl.append(dbType == DatabaseType.MYSQL ? "VARCHAR(255)" : "TEXT");

            if (i < fields.size() - 1) {
                ddl.append(",");
            }
            ddl.append("\n");
        }

        ddl.append(");");
        return ddl.toString();
    }

    /**
     * 生成INSERT语句数组
     */
    private String[] generateInsertStatements(RowSetTable table) {
        return table.getRowList().stream()
                .map(row -> buildInsertStatement(row, table.getField()))
                .toArray(String[]::new);
    }

    /**
     * 构建单条INSERT语句
     */
    private String buildInsertStatement(Row row, List<String> fields) {
        StringBuilder sql = new StringBuilder("INSERT INTO ")
                .append(quoteIdentifier(tableName))
                .append(" (")
                .append(String.join(", ", fields.stream()
                        .map(this::quoteIdentifier)
                        .toArray(String[]::new)))
                .append(") VALUES (");

        for (int i = 0; i < row.size(); i++) {
            Object value = row.get(i);
            if (value == null) {
                sql.append("NULL");
            } else {
                String escaped = dbType == DatabaseType.MYSQL ?
                        escapeMySQL(value.toString()) :
                        escapePostgreSQL(value.toString());
                sql.append("'").append(escaped).append("'");
            }

            if (i < row.size() - 1) sql.append(", ");
        }

        return sql.append(");").toString();
    }

    // ==================== 辅助方法 ==================== //

    /**
     * 记录数据批次信息
     */
    private void logDataBatch(RowSetTable batch) {
        if (batch == null) {
            Log.warn("SQLFileOutput", "接收到空数据批次");
            return;
        }

        List<Row> rows = batch.getRowList();
        Log.debug("SQLFileOutput",
                String.format("开始处理数据批次 - 总行数: %d", rows.size()));

        // 打印前3行样本数据
        int sampleSize = Math.min(3, rows.size());
        for (int i = 0; i < sampleSize; i++) {
            Log.debug("SQLFileOutput", String.format("样本行%d: %s", i+1, rows.get(i)));
        }
    }

    /**
     * 处理带日期变量的文件名
     */
    private String processFilenameWithDate() {
        if (filename.contains("${date}")) {
            String dateStr = new SimpleDateFormat(dateFormat).format(new Date());
            return filename.replace("${date}", dateStr);
        }
        return filename;
    }

    /**
     * 转义标识符（表名/字段名）
     */
    private String quoteIdentifier(String identifier) {
        return dbType == DatabaseType.MYSQL ?
                "`" + identifier + "`" :
                "\"" + identifier + "\"";
    }

    /**
     * MySQL特殊字符转义
     */
    private String escapeMySQL(String value) {
        return value.replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }

    /**
     * PostgreSQL特殊字符转义
     */
    private String escapePostgreSQL(String value) {
        return value.replace("\\", "\\\\")
                .replace("'", "''");
    }
}