package org.gugu.etl.ProcessDemo;

import core.Scheduler;
import org.junit.jupiter.api.Test;
import runtask.Step;
import runtask.StepList;
import java.util.Arrays;
import java.util.Collections;

public class DemoTest {
    @Test
    public void test_csv_demo_output_flow() throws InterruptedException {
        // 1. CSV输入步骤：读取测试数据
        Step input = new Step();
        input.withStepId(1)
                .withDes("读取CSV文件")
                .withDomain("input")
                .withSubType("csv")
                .withConfig("filePath", "src/test/java/org/gugu/etl/CsvInput/smallFile.csv")
                .withConfig("delimiter", ",")
                .withConfig("quoteChar", "\"")
                .withConfig("hasHeader", true)
                .withParentStepId(Collections.emptyList());

        // 2. Demo处理步骤：按年龄分流（>28 和 <=28）
        Step process = new Step();
        process.withStepId(2)
                .withDes("年龄分流处理器")
                .withDomain("process")
                .withSubType("demo")
                .withParentStepId(Collections.singletonList("1"));

        // 3. 输出步骤1：输出年龄 > 28 的记录
        Step output1 = new Step();
        output1.withStepId(3)
                .withDes("输出年龄>28的记录")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("2"));

        // 4. 输出步骤2：输出年龄 <= 28 的记录
        Step output2 = new Step();
        output2.withStepId(4)
                .withDes("输出年龄<=28的记录")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("2"));

        // 创建步骤列表并执行流程
        StepList stepList = new StepList(Arrays.asList(input, process, output1, output2));
//        while(true) {
            new Scheduler(stepList).execute();
//        }
    }
}