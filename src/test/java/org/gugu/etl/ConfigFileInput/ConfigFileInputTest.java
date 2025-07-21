package org.gugu.etl.ConfigFileInput;

import core.Scheduler;
import org.junit.jupiter.api.Test;
import runtask.Step;
import runtask.StepList;
import java.util.Arrays;
import java.util.Collections;

public class ConfigFileInputTest {

    @Test
    public void testPropertiesInput() throws InterruptedException {
        Step input = new Step();
        input.withStepId(1)
                .withDes("读取properties文件")
                .withDomain("input")
                .withSubType("configfile")
                .withConfig("filePath", "/Users/yuhan/Desktop/Yuhan/ideaProject/ETL/src/test/java/org/gugu/etl/ConfigFileInput/yh.properties");

        Step output = new Step();
        output.withStepId(2)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(input, output));
        new Scheduler(stepList).execute();
    }

    @Test
    public void testIniInput() throws InterruptedException {
        Step input = new Step();
        input.withStepId(1)
                .withDes("读取ini文件")
                .withDomain("input")
                .withSubType("configfile")
                .withConfig("filePath", "/Users/yuhan/Desktop/Yuhan/ideaProject/ETL/src/test/java/org/gugu/etl/ConfigFileInput/yh.ini");

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