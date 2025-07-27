package org.gugu.etl.ExcelInput;

import core.Scheduler;
import runtask.Step;
import runtask.StepList;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;

public class ExcelInputTest {

    /**
     * 测试静态路径读取Excel（单文件读取）
     */
    @Test
    public void test_staticExcelInput() throws InterruptedException {
        Step input = new Step()
                .withStepId(1)
                .withDes("读取Excel-静态路径")
                .withDomain("process")
                .withSubType("excel")
                .withConfig("filePath", "src/test/java/org/gugu/etl/ExcelInput/data.xlsx")
//                .withConfig("sheetName", "NoHeaderData")
                .withConfig("sheetName", "StandardData")
                .withConfig("headerRow", true)
                .withConfig("includeFilenameInOutput", true)
                .withConfig("rowNumField", "row_num")
                .withConfig("nullToEmpty", true)
                .withConfig("trimValues", true);

        Step output = new Step()
                .withStepId(2)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(input, output));
        new Scheduler(stepList).execute();
    }

    /**
     * 测试动态路径读取Excel（多个路径来源于上游CSV）
     */
    @Test
    public void test_dynamicExcelInput() throws InterruptedException {
        Step csvInput = new Step()
                .withStepId(1)
                .withDes("CSV输入：路径列表")
                .withDomain("input")
                .withSubType("csv")
                .withConfig("filePath", "src/test/java/org/gugu/etl/ExcelInput/dymamicPath.csv")
                .withConfig("hasHeader", true);

        Step excelInput = new Step()
                .withStepId(2)
                .withDes("Excel输入-动态路径")
                .withDomain("process")
                .withSubType("excel")
                .withParentStepId(Collections.singletonList("1"))
                .withConfig("fileNameField", "excelPath")
                .withConfig("sheetName", "NoHeaderData")
                .withConfig("headerRow", false)
                .withConfig("includeFilenameInOutput", true)
                .withConfig("rowNumField", "row_num")
                .withConfig("nullToEmpty", true)
                .withConfig("trimValues", true);

        Step output = new Step()
                .withStepId(3)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("2"));

        StepList stepList = new StepList(Arrays.asList(csvInput, excelInput, output));
        new Scheduler(stepList).execute();
    }
}
