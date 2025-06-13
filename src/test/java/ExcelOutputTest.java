
import core.Channel;
import core.flowdata.Row;
import plugin.ExcelOutput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExcelOutputTest {
    public static void main(String[] args) {
        try {
            // 配置参数
            Map<String, Object> config = new HashMap<>();
            config.put("filename", "C:\\Users\\吴文喆\\Desktop\\新建文件夹\\outputwwz.xlsx");
            config.put("sheetname", "Sheet1");
            config.put("append", false);
            config.put("hasHeader", true);

            // 定义字段
            List<Map<String, String>> fields = new ArrayList<>();
            Map<String, String> field1 = new HashMap<>();
            field1.put("fieldName", "Name");
            fields.add(field1);
            Map<String, String> field2 = new HashMap<>();
            field2.put("fieldName", "Age");
            fields.add(field2);
            config.put("fields", fields);

            // 创建 ExcelOutput 实例并初始化
            ExcelOutput excelOutput = new ExcelOutput();
            excelOutput.init(config);

            // 创建模拟的 Channel
            Channel<Row> input = new Channel<>();

            // 模拟数据
            List<Row> data = new ArrayList<>();
            Row row1 = new Row();
            row1.add("John");
            row1.add(25);
            data.add(row1);

            Row row2 = new Row();
            row2.add("Jane");
            row2.add(30);
            data.add(row2);

            // 将数据发送到 Channel
            for (Row row : data) {
                input.publish(row);
            }

            // 通知 Channel 数据发送完成
            input.close();

            // 处理数据
            excelOutput.consume(input);

            System.out.println("数据已成功写入 Excel 文件。");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}