package org.gugu.etl;

import java.util.HashMap;
import java.util.Map;

import plugin.ExcelInput;

public class ExcelInputTest {
    public static void main(String[] args) {
        // 创建配置对象
        Map<String, Object> config = new HashMap<>();
        // 替换为实际的 Excel 文件路径
        config.put("filePath", "C:\\wwz.xlsx");
        // 可选，设置是否有表头
        config.put("hasHeader",false);

        // 创建 ExcelInput 实例
        ExcelInput excelInput = new ExcelInput();

        // 调用 deal 方法读取 Excel 文件
        Object table = excelInput.deal(config);

        // 检查结果
        if (table != null) {
            // 假设 RowSetTable 类有一个 printInfo 方法来打印表格信息
            try {
                java.lang.reflect.Method printInfoMethod = table.getClass().getMethod("printInfo");
                printInfoMethod.invoke(table);
            } catch (Exception e) {
                System.err.println("调用 RowSetTable 的 printInfo 方法时出错: " + e.getMessage());
            }
        } else {
            System.out.println("读取 Excel 文件失败。");
        }
    }
}