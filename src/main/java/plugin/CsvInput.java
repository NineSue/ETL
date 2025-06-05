package plugin;

import anno.Input;
import cn.hutool.core.io.FileUtil;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IInput;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

@Input(type = "csv")
public class CsvInput implements IInput {

    /**
     * @brief CSV 输入插件，用于读取 CSV 文件并转换为 RowSetTable。
     *
     * @param config 类型为 Map 的配置对象
     * {
     *   "filePath": "文件路径",          // 必选，CSV 文件路径
     *   "delimiter": "字段分隔符",       // 可选，默认逗号(,)
     *   "quoteChar": "引号字符",         // 可选，默认双引号(")
     *   "hasHeader": boolean             // 可选，默认 true，是否有表头
     * }
     *
     * @return RowSetTable 包含 CSV 数据的表格对象，失败时返回空对象
     */
    @Override
    public RowSetTable deal(Object config) {
        if (!(config instanceof Map<?, ?>)) {
            System.err.println("CSV配置不是Map类型！");
            return null;
        }

        // 读config
        Map<String, Object> confMaping = (Map<String, Object>) config;
        String filePath = (String) confMaping.get("filePath");
        String delimiterStr = (String) confMaping.getOrDefault("delimiter", ",");
        String quoteStr = (String) confMaping.getOrDefault("quoteChar", "\"");
        Boolean hasHeader = (Boolean) confMaping.getOrDefault("hasHeader", true);

        // 参数校验
        if (filePath == null || filePath.isEmpty()) {
            System.err.println("缺少文件路径！");
            return null;
        }

        if (delimiterStr == null || delimiterStr.length() != 1) {
            System.err.println("分隔符必须是一个字符");
            return null;
        }

        if (quoteStr == null || quoteStr.length() != 1) {
            System.err.println("引号字符必须是一个字符");
            return null;
        }

        char delimiter = delimiterStr.charAt(0);
        char quoteChar = quoteStr.charAt(0);

        File file = new File(filePath);
        if (!file.exists()) {
            System.err.println("文件不存在: " + filePath);
            return null;
        }

        List<String> lines = FileUtil.readLines(file, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            System.err.println("CSV文件为空");
            return null;
        }

        // 表头
        String[] header;
        List<String> dataLines;
        try {
            if (hasHeader) {
                header = parseCsvLine(lines.get(0), delimiter, quoteChar);
                dataLines = lines.subList(1, lines.size());
            } else {
                int columnCount = parseCsvLine(lines.get(0), delimiter, quoteChar).length;
                header = IntStream.range(0, columnCount)
                        .mapToObj(i -> "Column" + (i + 1))
                        .toArray(String[]::new);
                dataLines = lines;
            }
        } catch (IllegalArgumentException e) {
            System.err.println("解析表头失败：" + e.getMessage());
            return null;
        }

        RowSetTable table = new RowSetTable(Arrays.asList(header));

        // 数据行处理
        for (String line : dataLines) {
            try {
                String[] cols = parseCsvLine(line.trim(), delimiter, quoteChar);
                Row row = new Row();
                row.addAll(Arrays.asList(cols));
                table.addRow(row);
            } catch (IllegalArgumentException e) {
                System.err.println("解析数据行失败: " + line + "，错误：" + e.getMessage());
                continue;
            }
        }

        return table;
    }


     //解析一行 CSV 数据

    private String[] parseCsvLine(String line, char delimiter, char quoteChar) {
        if (line == null || line.isEmpty()) {
            return new String[0];
        }

        List<String> result = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == quoteChar) {
                if (i + 1 < line.length() && line.charAt(i + 1) == quoteChar) {
                    current.append(quoteChar);
                    i++; // 跳过下一个引号
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == delimiter && !inQuotes) {
                result.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }

        result.add(current.toString());

        if (inQuotes) {
            throw new IllegalArgumentException("CSV 行包含未闭合的引号: " + line);
        }

        return result.toArray(new String[0]);
    }
}
