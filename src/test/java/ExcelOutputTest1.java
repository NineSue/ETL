import core.Channel;
import core.flowdata.Row;
import plugin.output.ExcelOutput;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExcelOutputTest1 {
    public static void main(String[] args) {
        try {
            // 创建线程池作为Channel的参数
            ExecutorService pool = Executors.newFixedThreadPool(4); // 根据需要调整线程池大小
            String stepId = "testStep"; // 自定义步骤ID

            Map<String, Object> config = new HashMap<>();
            config.put("filename", "C:\\Users\\吴文喆\\Desktop\\新建文件夹\\outputwwz7.xlsx");
            config.put("sheetname", "Sheet1");
            config.put("append", false);
            config.put("hasHeader", true);

            List<Map<String, String>> fields = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                Map<String, String> field = new HashMap<>();
                field.put("fieldName", "Field" + i);
                fields.add(field);
            }
            config.put("fields", fields);

            ExcelOutput excelOutput = new ExcelOutput();
            excelOutput.init(config);

            // 使用带参数的Channel构造方法
            Channel input = new Channel(pool, stepId);

            List<Row> data = new ArrayList<>();
            for (int j = 0; j < 1000; j++) {
                Row row = new Row();
                for (int i = 0; i < 100; i++) {
                    row.add("Data" + i);
                }
                data.add(row);
            }
            for (Row row : data) {
                input.publish(row);
            }
            input.close();
            excelOutput.consume(input);
            System.out.println("数据已成功写入 Excel 文件。");

            // 关闭线程池
            pool.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
