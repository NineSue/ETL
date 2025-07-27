package org.gugu.etl.JsonInput;

import core.Scheduler;
import org.junit.jupiter.api.Test;
import runtask.Step;
import runtask.StepList;
import tool.Log;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonInputTest {


    private static final String TEST_FILE_PATH = "src/test/java/org/gugu/etl/JsonInput/data.json";

    /**
     * 辅助方法：创建字段映射列表
     */
    private List<Map<String, String>> createFieldMappings() {
        List<Map<String, String>> mappings = new ArrayList<>();
        mappings.add(new HashMap<String, String>() {{ put("outputName", "id"); put("jsonPath", "id"); }});
        mappings.add(new HashMap<String, String>() {{ put("outputName", "name"); put("jsonPath", "name"); }});
        mappings.add(new HashMap<String, String>() {{ put("outputName", "age"); put("jsonPath", "details.age"); }});
        mappings.add(new HashMap<String, String>() {{ put("outputName", "city"); put("jsonPath", "details.city"); }});
        return mappings;
    }

   /**
     * 测试 JsonInput 插件的文件模式。
     * 从指定文件中读取纯 JSON 数据，并根据 JSONPath 提取字段。
     */

    @Test
    public void testJsonInputFileMode() throws InterruptedException {
        Log.info("JsonInputTest", "--- Running testJsonInputFileMode ---");

        Step jsonInputFileStep = new Step();
        jsonInputFileStep.withStepId(1)
                .withDes("从文件读取纯 JSON 数据")
                .withDomain("input")
                .withSubType("json")
                .withConfig("mode", "file")
                .withConfig("filePath", TEST_FILE_PATH)
                .withConfig("jsonRootPath", "") // 指定 JSON 数组的根路径
                .withConfig("fieldMappings", createFieldMappings()); // 字段映射

        Step consoleOutputStep = new Step();
        consoleOutputStep.withStepId(2)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(jsonInputFileStep, consoleOutputStep));
        new Scheduler(stepList).execute();

        Log.info("JsonInputTest", "--- Finished testJsonInputFileMode ---");
    }


    /*
     * 测试 JsonInput 插件的字符串模式。
     * 从 Step 配置的 sourceString 中直接读取纯 JSON 数据，并根据 JSONPath 提取字段。
     */
    @Test
    public void testJsonInputStringMode() throws InterruptedException {
        Log.info("JsonInputTest", "--- Running testJsonInputStringMode ---");

        String jsonString = "{\n" +
                "  \"items\": [\n" +
                "    {\"product_id\": \"A001\", \"name\": \"Laptop\", \"price\": 1200.00},\n" +
                "    {\"product_id\": \"B002\", \"name\": \"Mouse\", \"price\": 25.50}\n" +
                "  ],\n" +
                "  \"total_count\": 2\n" +
                "}";

        List<Map<String, String>> mappings = new ArrayList<>();
        mappings.add(new HashMap<String, String>() {{ put("outputName", "product_id"); put("jsonPath", "product_id"); }});
        mappings.add(new HashMap<String, String>() {{ put("outputName", "product_name"); put("jsonPath", "name"); }});
        mappings.add(new HashMap<String, String>() {{ put("outputName", "product_price"); put("jsonPath", "price"); }});

        Step jsonInputStringStep = new Step();
        jsonInputStringStep.withStepId(1)
                .withDes("从字符串读取纯 JSON 数据")
                .withDomain("input")
                .withSubType("json")
                .withConfig("mode", "string")
                .withConfig("sourceString", jsonString)
                .withConfig("jsonRootPath", "$.items") // 指定 JSON 数组的根路径
                .withConfig("fieldMappings", mappings); // 字段映射

        Step consoleOutputStep = new Step();
        consoleOutputStep.withStepId(2)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(jsonInputStringStep, consoleOutputStep));
        new Scheduler(stepList).execute();

        Log.info("JsonInputTest", "--- Finished testJsonInputStringMode ---");
    }

    /**
     * JsonInput 插件的 URL 模式的测试方法。
     */

    @Test
    public void testJsonInputUrlMode() throws InterruptedException {
        Log.info("JsonInputTest", "--- Running testJsonInputUrlMode ---");

        List<Map<String, String>> mappings = new ArrayList<>();
        mappings.add(new HashMap<String, String>() {{ put("outputName", "id"); put("jsonPath", "id"); }});
        mappings.add(new HashMap<String, String>() {{ put("outputName", "name"); put("jsonPath", "name"); }});
        mappings.add(new HashMap<String, String>() {{ put("outputName", "email"); put("jsonPath", "email"); }});

        Step jsonInputUrlStep = new Step();
        jsonInputUrlStep.withStepId(1)
                .withDes("从 URL 拉取 JSON 数据")
                .withDomain("input")
                .withSubType("json")
                .withConfig("mode", "url")
                .withConfig("url", "https://jsonplaceholder.typicode.com/users")
                .withConfig("jsonRootPath", "")
                .withConfig("fieldMappings", mappings);

        Step consoleOutputStep = new Step();
        consoleOutputStep.withStepId(2)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(jsonInputUrlStep, consoleOutputStep));
        new Scheduler(stepList).execute();

        Log.info("JsonInputTest", "--- Finished testJsonInputUrlMode ---");
    }
}
