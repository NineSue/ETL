import core.Channel;
import core.flowdata.Row;
import plugin.ExcelOutput;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExcelOutputTest1 {
    public static void main(String[] args) {
        try {
            Map<String, Object> config = new HashMap<>();
            config.put("filename", "C:\\Users\\吴文喆\\Desktop\\新建文件夹\\outputwwz4.xlsx");
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
            Channel<Row> input = new Channel<>();
            List<Row> data = new ArrayList<>();
            for (int j = 0; j < 10000; j++) {
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
