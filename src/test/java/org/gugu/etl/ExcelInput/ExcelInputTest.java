package org.gugu.etl.ExcelInput;

import core.Scheduler;
import org.junit.jupiter.api.Test;
import runtask.Step;
import runtask.StepList;
import java.util.Arrays;
import java.util.Collections;

public class ExcelInputTest {

    @Test
    public void test_excel_with_header() throws InterruptedException {
        Step input = new Step();
        input.withStepId(1)
                .withDes("读取带表头的Excel文件")
                .withDomain("input")
                .withSubType("excel")
                .withConfig("filePath", "src/test/java/org/gugu/etl/ExcelInput/orange.xlsx")
                .withConfig("sheetName", "StandardData")
                .withConfig("hasHeader", true)
                .withConfig("startRow", 0);

        Step output = new Step();
        output.withStepId(2)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(input, output));
        new Scheduler(stepList).execute();
    }

    // 测试无表头的Excel文件
    @Test
    public void test_excel_without_header() throws InterruptedException {
        Step input = new Step();
        input.withStepId(1)
                .withDes("读取不带表头的Excel文件")
                .withDomain("input")
                .withSubType("excel")
                .withConfig("filePath", "src/test/java/org/gugu/etl/ExcelInput/orange.xlsx")
                .withConfig("sheetName", "NoHeaderData")
                .withConfig("hasHeader", false);

        Step output = new Step();
        output.withStepId(2)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(input, output));
        new Scheduler(stepList).execute();
    }
}