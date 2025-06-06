//package plugin;
//
//import anno.Input;
//import cn.hutool.poi.excel.ExcelReader;
//import cn.hutool.poi.excel.ExcelUtil;
//import core.flowdata.Row;
//import core.flowdata.RowSetTable;
//import core.intf.IInput;
//import java.io.File;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
//@Input(type = "excel")
//public class ExcelInput implements IInput {
//
//    /**
//     * @brief Excel 输入插件，用于读取 Excel 文件并转换为 RowSetTable。
//     *
//     * @param config 类型为 Map 的配置对象
//     * {
//     *   "filePath": "文件路径",          // 必选，Excel 文件路径
//     *   "hasHeader": boolean            // 可选，默认 true，是否有表头
//     * }
//     *
//     * @return RowSetTable 包含 Excel 数据的表格对象，失败时返回空对象
//     */
//    @Override
//    public RowSetTable deal(Object config) {
//        if (!(config instanceof Map)) {
//            System.err.println("Excel配置不是Map类型！");
//            return null;
//        }
//        Map confMaping = (Map) config;
//        String filePath = (String) confMaping.get("filePath");
//        Boolean hasHeader = (Boolean) confMaping.getOrDefault("hasHeader", true);
//
//        if (filePath == null || filePath.isEmpty()) {
//            System.err.println("缺少文件路径！");
//            return null;
//        }
//        File file = new File(filePath);
//        if (!file.exists()) {
//            System.err.println("文件不存在: " + filePath);
//            return null;
//        }
//        ExcelReader reader = ExcelUtil.getReader(file);
//        if (reader == null) {
//            System.err.println("无法读取Excel文件: " + filePath);
//            return null;
//        }
//        List<String> header;
//        if (hasHeader) {
//            header = reader.readRow(0).stream().map(Object::toString).collect(Collectors.toList());
//        } else {
//            int columnCount = reader.readRow(0).size();
//            header = IntStream.range(0, columnCount)
//                    .mapToObj(i -> "Column" + (i + 1))
//                    .collect(Collectors.toList());
//        }
//        RowSetTable table = new RowSetTable(header);
//
//        List<List<Object>> rows;
//        if (hasHeader) {
//            rows = reader.read(1); // 从第2行开始读取数据
//        } else {
//            rows = reader.read(0); // 从第1行开始读取数据
//        }
//
//        for (List<Object> rowList : rows) {
//            Row row = new Row();
//            row.addAll(rowList.stream().map(Object::toString).collect(Collectors.toList()));
//            table.addRow(row);
//        }
//
//        return table;
//    }
//
//    @Override
//    public RowSetTable deal(Map<String, Object> config) {
//        return null;
//    }
//
//}



package plugin;

import anno.Input;
import cn.hutool.poi.excel.ExcelReader;
import cn.hutool.poi.excel.ExcelUtil;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IInput;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Input(type = "excel")
public class ExcelInput implements IInput {

    /**
     * @brief Excel 输入插件，用于读取 Excel 文件并转换为 RowSetTable。
     *
     * @param config 类型为 Map 的配置对象
     * {
     *   "filePath": "文件路径",          // 必选，Excel 文件路径
     *   "hasHeader": boolean,           // 可选，默认 true，是否有表头
     *   "sheetName": "工作表名称",       // 可选，指定工作表名称
     *   "sheetIndex": int,              // 可选，指定工作表索引
     *   "startRow": int,                // 可选，从哪一行开始读取
     *   "endRow": int,                  // 可选，读取到哪一行结束
     *   "columns": List<String>,        // 可选，指定读取的列（列名或列索引）
     *   "skipEmptyRows": boolean,       // 可选，默认 false，是否跳过空行
     *   "encoding": String              // 可选，默认 UTF-8，文件编码
     * }
     *
     * @return RowSetTable 包含 Excel 数据的表格对象，失败时返回空对象
     */
    @Override
    public RowSetTable deal(Object config) {
        if (!(config instanceof Map)) {
            System.err.println("Excel配置不是Map类型！");
            return null;
        }
        Map<String, Object> confMaping = (Map<String, Object>) config;
        String filePath = (String) confMaping.get("filePath");
        Boolean hasHeader = (Boolean) confMaping.getOrDefault("hasHeader", true);
        String sheetName = (String) confMaping.get("sheetName");
        Integer sheetIndex = (Integer) confMaping.get("sheetIndex");
        Integer startRow = (Integer) confMaping.getOrDefault("startRow", hasHeader ? 1 : 0);
        Integer endRow = (Integer) confMaping.get("endRow");
        List<String> columns = (List<String>) confMaping.get("columns");
        Boolean skipEmptyRows = (Boolean) confMaping.getOrDefault("skipEmptyRows", false);
        String encoding = (String) confMaping.getOrDefault("encoding", "UTF-8");

        if (filePath == null || filePath.isEmpty()) {
            System.err.println("缺少文件路径！");
            return null;
        }
        File file = new File(filePath);
        if (!file.exists()) {
            System.err.println("文件不存在: " + filePath);
            return null;
        }

        ExcelReader reader;
        if (sheetName != null) {
            reader = ExcelUtil.getReader(file, sheetName);
        } else if (sheetIndex != null) {
            reader = ExcelUtil.getReader(file, sheetIndex);
        } else {
            reader = ExcelUtil.getReader(file);
        }
        if (reader == null) {
            System.err.println("无法读取Excel文件: " + filePath);
            return null;
        }

        List<String> header;
        if (hasHeader) {
            header = reader.readRow(0).stream().map(Object::toString).collect(Collectors.toList());
        } else {
            int columnCount = reader.readRow(0).size();
            header = IntStream.range(0, columnCount)
                    .mapToObj(i -> "Column" + (i + 1))
                    .collect(Collectors.toList());
        }
        RowSetTable table = new RowSetTable(header);

        List<List<Object>> rows;
        if (endRow == null) {
            rows = reader.read(startRow);
        } else {
            rows = reader.read(startRow, endRow);
        }

        if (columns != null) {
            // 过滤指定的列
            List<Integer> columnIndexes = columns.stream()
                    .map(col -> header.indexOf(col))
                    .filter(idx -> idx >= 0)
                    .collect(Collectors.toList());
            rows = rows.stream()
                    .map(row -> {
                        List<Object> newRow = new java.util.ArrayList<>();
                        for (Integer idx : columnIndexes) {
                            newRow.add(row.get(idx));
                        }
                        return newRow;
                    })
                    .collect(Collectors.toList());
        }

        if (skipEmptyRows) {
            rows = rows.stream()
                    .filter(row -> row.stream().anyMatch(cell -> cell != null && !cell.toString().trim().isEmpty()))
                    .collect(Collectors.toList());
        }

        for (List<Object> rowList : rows) {
            Row row = new Row();
            row.addAll(rowList.stream().map(Object::toString).collect(Collectors.toList()));
            table.addRow(row);
        }

        return table;
    }

    @Override
    public RowSetTable deal(Map<String, Object> config) {
        return deal((Object) config);
    }
}
