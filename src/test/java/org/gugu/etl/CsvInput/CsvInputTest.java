package org.gugu.etl.CsvInput;

import core.Scheduler;
import org.junit.jupiter.api.Test;
import runtask.Step;
import runtask.StepList;

import java.util.Arrays;
import java.util.Collections;

public class CsvInputTest {
    @Test
    public void testCsvInput() throws InterruptedException {
        Step input = new Step();
        input.withStepId(1)
                .withDes("读取csv文件")
                .withDomain("input")
                .withSubType("csv")
                .withConfig("filePath", "src/test/java/org/gugu/etl/CsvInput/gugugu.csv")
                .withConfig("delimiter", ",")
                .withConfig("quoteChar", "\"")
                .withConfig("hasHeader", true);

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
