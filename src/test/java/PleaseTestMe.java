import core.Scheduler;
import org.junit.jupiter.api.Test;
import plugin.ExcelOutput;
import runtask.Step;
import runtask.StepList;

import java.util.*;


public class PleaseTestMe{
    @Test
    public void test_csv_etl_flow() {
        Step input = new Step();
        input.withStepId(1)
                .withDes("读取csv文件")
                .withDomain("input")
                .withSubType("csv")
                .withConfig("filePath", "src/test/java/gugugu.csv")
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
    @Test
    public void test_http_etl_flow() {
        Step input = new Step();
        input.withStepId(1)
                .withDes("从接口读取数据")
                .withDomain("input")
                .withSubType("http")
                .withConfig("url", "http://localhost:3000/api/query/preview")
                .withConfig("method", "POST")
                .withConfig("body", "{\"connectionId\":3,\"sql\":\"SELECT id, name, age, city FROM users;\"}")
                .withConfig("headers", Collections.singletonMap("Content-Type", "application/json"));

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
    public void test_json_etl_flow() {
        Step input = new Step();
        input.withStepId(1)
                .withDes("读取包含json字段的文本")
                .withDomain("input")
                .withSubType("jsontext")
                .withConfig("sourceType", "file")
                .withConfig("filePath", "src/test/java/yh_json.txt");

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
    public void test_excel_output_generation() {
        // 创建 ExcelOutput 实例
        ExcelOutput excelOutput = new ExcelOutput();

        // 模拟输入数据
        List<Map<String, Object>> inputData = new ArrayList<>();
        Map<String, Object> row1 = new HashMap<>();
        row1.put("name", "张三");
        row1.put("age", 25);
        row1.put("email", "zhangsan@example.com");

        Map<String, Object> row2 = new HashMap<>();
        row2.put("name", "李四");
        row2.put("age", 30);
        row2.put("email", "lisi@example.com");

        inputData.add(row1);
        inputData.add(row2);

        // 设置输入数据
        excelOutput.setInputData(inputData);

        // 配置参数
        Map<String, Object> config = new HashMap<>();
        config.put("filename", "C:\\Users\\吴文喆\\Desktop\\新建文件夹\\output11.xlsx"); // 输出文件名
        config.put("sheetname", "Sheet1");     // 工作表名称
        config.put("append", false);           // 是否追加
        config.put("hasHeader", true);         // 是否写入表头

        // 字段配置
        List<Map<String, String>> fields = new ArrayList<>();
        Map<String, String> field1 = new HashMap<>();
        field1.put("fieldName", "name");
        field1.put("fieldAlias", "姓名");

        Map<String, String> field2 = new HashMap<>();
        field2.put("fieldName", "age");
        field2.put("fieldAlias", "年龄");

        Map<String, String> field3 = new HashMap<>();
        field3.put("fieldName", "email");
        field3.put("fieldAlias", "邮箱");

        fields.add(field1);
        fields.add(field2);
        fields.add(field3);

        config.put("fields", fields);

        // 调用 deal 方法处理
        try {
            excelOutput.deal(config);
            System.out.println("Excel 文件已成功生成！");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("生成 Excel 文件时出错！");
        }
    }
}



