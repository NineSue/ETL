package org.gugu.etl.SQLFileOutput;

import core.Scheduler;
import runtask.Step;
import runtask.StepList;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class SQLFileOutputTest {

    @Test
    public void testToMySQL() throws Exception {

        // 第一步：CSV输入
        Step input = new Step()
                .withStepId(1)
                .withDes("读取csv文件")
                .withDomain("input")
                .withSubType("csv")
                .withConfig("filePath", "src/test/java/org/gugu/etl/CsvInput/smallFile.csv")
                .withConfig("delimiter", ",")
                .withConfig("quoteChar", "\"")
                .withConfig("hasHeader", true);

        // 第二步：SQL文件输出(MySQL格式)
        Step output = new Step()
                .withStepId(2)
                .withDes("输出MySQL格式SQL文件")
                .withDomain("output")
                .withSubType("sqlfile")
                .withParentStepId(Collections.singletonList("1"))
                .withConfig("dbtype", "mysql")
                .withConfig("filename", "src/test/java/org/gugu/etl/SQLFileOutput/output/mysql_output.sql")
                .withConfig("table_name", "test1")
                .withConfig("create_table", true)
                .withConfig("overwrite", true);

        StepList stepList = new StepList(Arrays.asList(input, output));
        new Scheduler(stepList).execute();

        // 验证输出文件
        File outputFile = new File("src/test/java/org/gugu/etl/SQLFileOutput/output/mysql_output.sql");
        assertTrue(outputFile.exists());


    }

    @Test
    public void testToPostgreSQL() throws Exception {
        // 第一步：CSV输入
        Step input = new Step()
                .withStepId(1)
                .withDes("读取csv文件")
                .withDomain("input")
                .withSubType("csv")
                .withConfig("filePath", "src/test/java/org/gugu/etl/CsvInput/smallFile.csv")
                .withConfig("hasHeader", true);

        // 第二步：SQL文件输出(PostgreSQL格式)
        Step output = new Step()
                .withStepId(2)
                .withDes("输出PostgreSQL格式SQL文件")
                .withDomain("output")
                .withSubType("sqlfile")
                .withParentStepId(Collections.singletonList("1"))
                .withConfig("dbtype", "postgresql")
                .withConfig("filename", "src/test/java/org/gugu/etl/SQLFileOutput/output/pg_output.sql")
                .withConfig("table_name", "test2")
                .withConfig("create_table", true)
                .withConfig("overwrite", true);

        StepList stepList = new StepList(Arrays.asList(input, output));
        new Scheduler(stepList).execute();

        // 验证输出文件
        File outputFile = new File("src/test/java/org/gugu/etl/SQLFileOutput/output/pg_output.sql");
        assertTrue(outputFile.exists());
        assertTrue(outputFile.length() > 0);
    }

    @Test
    public void testAutoDirCreation() throws Exception {
        // 删除测试目录（如果存在）
        File testDir = new File("src/test/java/org/gugu/etl/SQLFileOutput/new");
        if (testDir.exists()) {
            Files.walk(testDir.toPath())
                    .sorted((a, b) -> -a.compareTo(b))
                    .forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    });
        }

        // 第一步：CSV输入
        Step input = new Step()
                .withStepId(1)
                .withDes("读取csv文件")
                .withDomain("input")
                .withSubType("csv")
                .withConfig("filePath", "src/test/java/org/gugu/etl/CsvInput/smallFile.csv")
                .withConfig("hasHeader", true);

        // 第二步：SQL文件输出(测试自动创建目录)
        Step output = new Step()
                .withStepId(2)
                .withDes("测试自动目录创建")
                .withDomain("output")
                .withSubType("sqlfile")
                .withParentStepId(Collections.singletonList("1"))
                .withConfig("dbtype", "mysql")
                .withConfig("filename", "src/test/java/org/gugu/etl/SQLFileOutput/new/output.sql")
                .withConfig("table_name", "test3")
                .withConfig("create_parent_dir", true);

        StepList stepList = new StepList(Arrays.asList(input, output));
        new Scheduler(stepList).execute();

        // 验证目录和文件
        assertTrue(testDir.exists());
        File outputFile = new File(testDir, "output.sql");
        assertTrue(outputFile.exists());
        assertTrue(outputFile.length() > 0);
    }
}