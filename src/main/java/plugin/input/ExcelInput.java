//package plugin.input;
//
//import anno.Input;
//import core.Channel;
//import core.flowdata.Row;
//import core.intf.IInput;
//
//import java.io.*;
//import java.util.*;
//
//import org.apache.poi.ss.usermodel.*;
//import org.apache.poi.xssf.usermodel.XSSFWorkbook;
//
///**
// * Excel 输入插件，用于读取 Excel 文件并通过 Channel 发送数据。
// */
//@Input(type="excel")
//public class ExcelInput implements IInput {
//    private String filePath;
//    private String sheetName;
//    private boolean hasHeader;
//    private int startRow;
//    private boolean nullToEmpty;
//    private boolean trimValues;
//
//    @Override
//    public void init(Map<String, Object> cfg) {
//        // 初始化配置参数
//        this.filePath = (String) cfg.get("filePath");
//        this.sheetName = (String) cfg.get("sheetName");
//        this.hasHeader = (boolean) cfg.getOrDefault("hasHeader", true);
//        this.startRow = (int) cfg.getOrDefault("startRow", hasHeader ? 1 : 0);
//        this.nullToEmpty = (boolean) cfg.getOrDefault("nullToEmpty", true);
//        this.trimValues = (boolean) cfg.getOrDefault("trimValues", true);
//
//        // 参数校验
//        if (filePath == null || filePath.isEmpty()) {
//            throw new IllegalArgumentException("必须指定文件路径(filePath)");
//        }
//        if (hasHeader && startRow ==0) {
//            System.err.println("警告：有头且开始行是0,自动修正");
//            this.startRow = 1;
//        }
//    }
//
//    @Override
//    public void start(Channel output) throws Exception {
//        try {
//            // 创建文件对象并验证存在性
//            File file = new File(filePath);
//            if (!file.exists()) {
//                throw new IllegalArgumentException("文件不存在: " + filePath);
//            }
//
//            // 打开工作簿和工作表
//            try (FileInputStream fis = new FileInputStream(file);
//                 Workbook workbook = new XSSFWorkbook(fis)) {
//
//                Sheet sheet = getSheet(workbook, sheetName);
//                if (sheet == null) {
//                    return;
//                }
//
//                // 处理表头
//                List<String> headers = new ArrayList<>();
//                processHeader(sheet, headers);
//
//                // 如果没有表头，生成默认列名
//                if (!hasHeader) {
//                    generateDefaultHeaders(sheet, headers);
//                }
//
//                Row headerRow=new Row();
//                for(String header:headers){
//                    headerRow.add(header);
//                }
//                output.publish(headerRow);
//
//                // 创建RowSetTable并处理数据行
//                for (int i = startRow; i <= sheet.getLastRowNum(); i++) {
//                    org.apache.poi.ss.usermodel.Row excelRow = sheet.getRow(i);
//                    if (excelRow != null) {
//                        Row tableRow = new Row();
//                        for (int j = 0; j < headers.size(); j++) {
//                            Cell cell = excelRow.getCell(j);
//                            String value = getCellValueAsString(cell, workbook, nullToEmpty, trimValues);
//                            tableRow.add(value);
//                        }
//                        output.publish(tableRow); // 通过 Channel 发送数据
//                    }
//                }
//            }
//
//            output.close(); // 数据发送完毕后关闭 Channel
//        } catch (IOException e) {
//            System.err.println("读取Excel文件失败: " + e.getMessage());
//            output.close();
//            throw e;
//        }
//    }
//
//    /**
//     * 获取工作表，根据名称或默认第一个
//     */
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
//    /**
//     * 处理表头行
//     */
//    private void processHeader(Sheet sheet, List<String> headers) {
//        if (hasHeader) {
//            org.apache.poi.ss.usermodel.Row headerRow = sheet.getRow(0);
//            if (headerRow != null) {
//                for (int i = 0; i < headerRow.getLastCellNum(); i++) {
//                    Cell cell = headerRow.getCell(i);
//                    String headerName = getCellValueAsString(cell, null, false, true);
//                    headers.add(headerName != null ? headerName : "Column" + (i + 1));
//                }
//            }
//        }
//    }
//
//    /**
//     * 生成默认表头（当没有表头时）
//     */
//    private void generateDefaultHeaders(Sheet sheet, List<String> headers) {
//        org.apache.poi.ss.usermodel.Row firstRow = sheet.getRow(0);
//        int columnCount = firstRow != null ? firstRow.getLastCellNum() : 0;
//        for (int i = 0; i < columnCount; i++) {
//            headers.add("Column" + (i + 1));
//        }
//    }
//
//    /**
//     * 将单元格转换为字符串值
//     */
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
//                    value = (num == (long) num) ? String.valueOf((long) num) : String.valueOf(num);
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
//}
