package org.gugu.etl.ConfigFileInput;

import core.Scheduler;
import org.junit.jupiter.api.Test;
import runtask.Step;
import runtask.StepList;
import tool.Log;

import java.util.Arrays;
import java.util.Collections;

public class ConfigFileInputTest {

    private static final String TEST_FILE_PATH = "/Users/yuhan/Desktop/Yuhan/ideaProject/ETL/src/test/java/org/gugu/etl/ConfigFileInput/config.csv";

    @Test
    public void testConfigFileInput() throws InterruptedException {
        Log.info("ConfigFileInputTest", "--- Running testConfigFileInput ---");

        Step configFileInputStep = new Step();
        configFileInputStep.withStepId(1)
                .withDes("从配置文件读取数据")
                .withDomain("input")
                .withSubType("configfile")
                .withConfig("filePath", TEST_FILE_PATH);

        Step consoleOutputStep = new Step();
        consoleOutputStep.withStepId(2)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(configFileInputStep, consoleOutputStep));
        new Scheduler(stepList).execute();

        Log.info("ConfigFileInputTest", "--- Finished testConfigFileInput ---");
    }
}