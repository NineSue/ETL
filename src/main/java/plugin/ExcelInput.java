//package plugin;
//
//import anno.Input;
//import core.flowdata.RowSetTable;
//import core.intf.IInput;
//
//@Input(type = "excel")
//public class ExcelInput implements IInput {
//
//
//    @Override
//    public RowSetTable deal(Object data) {
//        return null;
//    }
////}
//package plugin;
//
//import anno.Input;
//import core.flowdata.Row;
//import core.flowdata.RowSetTable;
//import core.intf.IInput;
//import org.apache.poi.ss.usermodel.Cell;
//import org.apache.poi.ss.usermodel.DateUtil;
//import org.apache.poi.ss.usermodel.Sheet;
//import org.apache.poi.ss.usermodel.Workbook;
//import org.apache.poi.xssf.usermodel.XSSFWorkbook;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//@Input(type = "excel")
//public class ExcelInput implements IInput {
//
//    @Override
//    public RowSetTable deal(Object data) {
//        return null;
//    }
//
//    @Override
//    public RowSetTable deal(Map<String, Object> config) {
//        String filePath = (String) config.get("filePath");
//        String sheetName = (String) config.getOrDefault("sheetName", "Sheet1");
//        boolean hasHeader = (Boolean) config.getOrDefault("hasHeader", true);
//
//        List<Row> rows = new ArrayList<>();
//        List<String> headers = new ArrayList<>();
//
//        try (FileInputStream fis = new FileInputStream(new File(filePath));
//             Workbook workbook = new XSSFWorkbook(fis)) {
//
//            Sheet sheet = workbook.getSheet(sheetName);
//            if (sheet == null) {
//                throw new IllegalArgumentException("Sheet not found: " + sheetName);
//            }
//
//            int rowCount = sheet.getPhysicalNumberOfRows();
//            for (int i = 0; i < rowCount; i++) {
//                Row row = new Row();
//                org.apache.poi.ss.usermodel.Row excelRow = sheet.getRow(i);
//                if (excelRow == null) {
//                    continue;
//                }
//
//                int cellCount = excelRow.getPhysicalNumberOfCells();
//                for (int j = 0; j < cellCount; j++) {
//                    Cell cell = excelRow.getCell(j);
//                    if (cell == null) {
//                        row.addCell("");
//                        continue;
//                    }
//
//                    switch (cell.getCellType()) {
//                        case STRING:
//                            row.addCell(cell.getStringCellValue());
//                            break;
//                        case NUMERIC:
//                            if (DateUtil.isCellDateFormatted(cell)) {
//                                row.addCell(cell.getDateCellValue().toString());
//                            } else {
//                                row.addCell(String.valueOf(cell.getNumericCellValue()));
//                            }
//                            break;
//                        case BOOLEAN:
//                            row.addCell(String.valueOf(cell.getBooleanCellValue()));
//                            break;
//                        case FORMULA:
//                            row.addCell(cell.getCellFormula());
//                            break;
//                        default:
//                            row.addCell("");
//                    }
//                }
//
//                if (i == 0 && hasHeader) {
//                    headers.addAll(row.getCells());
//                } else {
//                    rows.add(row);
//                }
//            }
//
//        } catch (IOException e) {
//            throw new RuntimeException("Failed to read Excel file: " + filePath, e);
//        }
//        return new RowSetTable(new ArrayList<>(headers), rows);
//    }
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
     *   "hasHeader": boolean            // 可选，默认 true，是否有表头
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
        Map confMaping = (Map) config;
        String filePath = (String) confMaping.get("filePath");
        Boolean hasHeader = (Boolean) confMaping.getOrDefault("hasHeader", true);

        if (filePath == null || filePath.isEmpty()) {
            System.err.println("缺少文件路径！");
            return null;
        }
        File file = new File(filePath);
        if (!file.exists()) {
            System.err.println("文件不存在: " + filePath);
            return null;
        }
        ExcelReader reader = ExcelUtil.getReader(file);
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
        if (hasHeader) {
            rows = reader.read(1); // 从第2行开始读取数据
        } else {
            rows = reader.read(0); // 从第1行开始读取数据
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
        return null;
    }

}
