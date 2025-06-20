//package plugin;
//
//import anno.Input;
//import core.Channel;
//import core.flowdata.Row;
//import core.flowdata.RowSetTable;
//import core.intf.IInput;
//import org.apache.poi.ss.usermodel.*;
//import org.apache.poi.xssf.usermodel.XSSFWorkbook;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.util.*;
//
//@Input(type = "excel")
//public class ExcelInput implements IInput {
//    /**
//     * @brief Excel 输入插件，用于读取 Excel 文件并转换为 RowSetTable。
//     *
//     * @param config 类型为 Map 的配置对象
//     * {
//     *   "filePath": "文件路径",          // 必选，Excel 文件路径
//     *   "sheetName": "工作表名称",       // 可选，默认第一个工作表
//     *   "hasHeader": boolean,          // 可选，默认 true，是否有表头
//     *   "startRow": int,               // 可选，数据开始行号（0-based）
//     *   "nullToEmpty": boolean,        // 可选，默认 true，是否将null转为空字符串
//     *   "trimValues": boolean          // 可选，默认 true，是否去除字符串两端的空格
//     * }
//     *
//     * @return RowSetTable 包含 Excel 数据的表格对象，失败时返回空对象
//     */
//    @Override
//    public RowSetTable deal(Object config) {
//        // 参数校验
//        if (!(config instanceof Map)) {
//            System.err.println("Excel配置参数必须是Map类型");
//            return null;
//        }
//
//        Map<String, Object> confMap = (Map<String, Object>) config;
//
//        // 获取必要参数
//        String filePath = (String) confMap.get("filePath");
//        if (filePath == null || filePath.isEmpty()) {
//            System.err.println("必须指定文件路径(filePath)");
//            return null;
//        }
//
//        // 获取可选参数
//        String sheetName = (String) confMap.get("sheetName");
//        boolean hasHeader = (boolean) confMap.getOrDefault("hasHeader", true);
//        Integer startRow = (Integer) confMap.getOrDefault("startRow", hasHeader ? 1 : 0);
//        boolean nullToEmpty = (boolean) confMap.getOrDefault("nullToEmpty", true);
//        boolean trimValues = (boolean) confMap.getOrDefault("trimValues", true);
//
//        File file = new File(filePath);
//        if (!file.exists()) {
//            System.err.println("文件不存在: " + filePath);
//            return null;
//        }
//
//        try (FileInputStream fis = new FileInputStream(file);
//             Workbook workbook = new XSSFWorkbook(fis)) {
//
//            // 获取工作表
//            Sheet sheet = getSheet(workbook, sheetName);
//            if (sheet == null) {
//                return null;
//            }
//
//            // 处理表头
//            List<String> headers = new ArrayList<>();
//            if (hasHeader) {
//                org.apache.poi.ss.usermodel.Row headerRow = sheet.getRow(0);
//                if (headerRow != null) {
//                    for (int i = 0; i < headerRow.getLastCellNum(); i++) {
//                        Cell cell = headerRow.getCell(i);
//                        String headerName = getCellValueAsString(cell, workbook, nullToEmpty, trimValues);
//                        if (headerName == null || headerName.isEmpty()) {
//                            headerName = "Column" + (i + 1);
//                        }
//                        headers.add(headerName);
//                    }
//                }
//            } else {
//                // 如果没有表头，使用第一行确定列数并生成默认列名
//                org.apache.poi.ss.usermodel.Row firstRow = sheet.getRow(0);
//                int columnCount = firstRow != null ? firstRow.getLastCellNum() : 0;
//                for (int i = 0; i < columnCount; i++) {
//                    headers.add("Column" + (i + 1));
//                }
//            }
//
//            // 创建RowSetTable
//            RowSetTable table = new RowSetTable(headers);
//
//            // 处理数据行
//            for (int i = startRow; i <= sheet.getLastRowNum(); i++) {
//                org.apache.poi.ss.usermodel.Row excelRow = sheet.getRow(i);
//                if (excelRow == null) {
//                    continue; // 跳过空行
//                }
//
//                Row tableRow = new Row();
//                for (int j = 0; j < headers.size(); j++) {
//                    Cell cell = excelRow.getCell(j);
//                    String value = getCellValueAsString(cell, workbook, nullToEmpty, trimValues);
//                    tableRow.add(value);
//                }
//                table.addRow(tableRow);
//            }
//
//            return table;
//
//        } catch (IOException e) {
//            System.err.println("读取Excel文件失败: " + e.getMessage());
//            return null;
//        }
//    }
//
//    private Sheet getSheet(Workbook workbook, String sheetName) {
//        if (sheetName != null && !sheetName.isEmpty()) {
//            Sheet sheet = workbook.getSheet(sheetName);
//            if (sheet == null) {
//                System.err.println("工作表 '" + sheetName + "' 不存在");
//                return null;
//            }
//            return sheet;
//        }
//        return workbook.getSheetAt(0); // 默认第一个工作表
//    }
//
//    private String getCellValueAsString(Cell cell, Workbook workbook, boolean nullToEmpty, boolean trimValues) {
//        if (cell == null) {
//            return nullToEmpty ? "" : null;
//        }
//
//        String value;
//        switch (cell.getCellType()) {
//            case STRING:
//                value = cell.getStringCellValue();
//                break;
//            case NUMERIC:
//                if (DateUtil.isCellDateFormatted(cell)) {
//                    value = cell.getDateCellValue().toString();
//                } else {
//                    double num = cell.getNumericCellValue();
//                    // 如果是整数，去掉小数部分
//                    if (num == (long) num) {
//                        value = String.valueOf((long) num);
//                    } else {
//                        value = String.valueOf(num);
//                    }
//                }
//                break;
//            case BOOLEAN:
//                value = String.valueOf(cell.getBooleanCellValue());
//                break;
//            case FORMULA:
//                try {
//                    return getCellValueAsString(
//                            workbook.getCreationHelper().createFormulaEvaluator().evaluateInCell(cell),
//                            workbook, nullToEmpty, trimValues
//                    );
//                } catch (Exception e) {
//                    value = cell.getCellFormula();
//                }
//                break;
//            case BLANK:
//                value = nullToEmpty ? "" : null;
//                break;
//            default:
//                value = nullToEmpty ? "" : null;
//        }
//
//        return trimValues && value != null ? value.trim() : value;
//    }
//
//    @Override
//    public void init(Map<String, Object> cfg) {
//
//    }
//
//    @Override
//    public void start(Channel output) throws Exception {
//
//    }
//}