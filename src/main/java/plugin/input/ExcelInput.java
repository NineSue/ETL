package plugin.input; // Changed package name to reflect its type
import anno.Process; // Changed annotation type
import core.Channel;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IProcess; // Implemented IProcess interface
import tool.Log;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.util.*;

//Done
@Process(type = "excel") // Changed annotation
public class ExcelInput implements IProcess { // Implemented IProcess
    private String filePath;             // Static path (if configured, expects upstream to provide a trigger signal)
    private String fileNameField;        // Dynamic path field name
    private String sheetName;
    private boolean headerRow;
    private boolean includeFilenameInOutput;
    private String rowNumField;
    private boolean nullToEmpty;
    private boolean trimValues;

    // Removed redundant 'outputChannels' field as 'outputs' is passed directly to process and readExcelFile.

    @Override
    public void init(Map<String, Object> cfg) {
        this.filePath = (String) cfg.get("filePath");
        this.fileNameField = (String) cfg.get("fileNameField");
        this.sheetName = (String) cfg.getOrDefault("sheetName", "");
        this.headerRow = (Boolean) cfg.getOrDefault("headerRow", true);
        this.includeFilenameInOutput = (Boolean) cfg.getOrDefault("includeFilenameInOutput", false);
        this.rowNumField = (String) cfg.getOrDefault("rowNumField", null);
        this.nullToEmpty = (Boolean) cfg.getOrDefault("nullToEmpty", true);
        this.trimValues = (Boolean) cfg.getOrDefault("trimValues", true);

        // Ensure at least one path configuration method is specified
        if ((filePath == null || filePath.isEmpty()) && (fileNameField == null || fileNameField.isEmpty())) {
            throw new IllegalArgumentException("Must specify filePath or fileNameField");
        }
    }

    @Override
    public void process(Channel input, List<Channel> outputs) throws Exception { // Changed method signature
        if (fileNameField != null && !fileNameField.isEmpty()) {
            // Dynamic path mode: Consume file paths from the input channel
            Log.info("ExcelInput", "Dynamic path mode: Waiting for upstream to provide file paths...");
            input.onReceive(rowObj -> {
                if (!(rowObj instanceof Row)) {
                    Log.warn("ExcelInput", "Upstream data type is not Row, skipping.");
                    return;
                }
                Row row = (Row) rowObj;

                // Get header from the input channel
                RowSetTable inputHeader = input.getHeader();
                if (inputHeader == null) {
                    Log.error("ExcelInput", "Input channel's Header is not set, cannot get fileNameField.");
                    return;
                }

                int idx = inputHeader.getFieldIndex(fileNameField);
                if (idx == -1) {
                    Log.error("ExcelInput", "Field " + fileNameField + " does not exist in upstream Header.");
                    return;
                }

                Object val = row.get(idx);
                if (val == null) {
                    Log.warn("ExcelInput", "Field " + fileNameField + " is null, skipping.");
                    return;
                }
                String path = val.toString();
                try {
                    readExcelFile(path, outputs); // Pass the actual list of output channels to the read method
                } catch (Exception e) {
                    Log.error("ExcelInput", "Failed to read Excel: " + e.getMessage());
                }
            }, () -> {
                // When upstream channel closes, close all downstream output channels
                Log.info("ExcelInput", "Upstream channel closed, ExcelInput finished dynamic path reading.");
                for (Channel out : outputs) {
                    out.close();
                }
            });
        } else {
            // Static path mode: Read the file directly once
            // Processing a static file in IProcess means it doesn't depend on upstream data,
            // or upstream only provides a trigger signal. Here we read directly.
            Log.info("ExcelInput", "Static path mode: Reading file directly " + filePath);
            readExcelFile(filePath, outputs); // Pass the actual list of output channels to the read method
            // In static mode, close output channels immediately after reading is complete
            for (Channel out : outputs) {
                out.close();
            }
        }
    }

    // readExcelFile method needs to accept List<Channel>
    private void readExcelFile(String path, List<Channel> outputs) throws Exception {
        File file = new File(path);
        if (!file.exists()) {
            Log.error("ExcelInput", "File does not exist: " + path);
            return;
        }

        Log.info("ExcelInput", "Reading file: " + path);
        try (FileInputStream fis = new FileInputStream(file);
             Workbook workbook = new XSSFWorkbook(fis)) {

            Sheet sheet = getSheet(workbook);
            if (sheet == null) {
                Log.warn("ExcelInput", "Worksheet does not exist, file: " + path);
                return;
            }

            // Build header
            List<String> headers = headerRow ? extractHeader(sheet) : generateDefaultHeaders(sheet);
            if (rowNumField != null && !rowNumField.isEmpty()) {
                headers.add(rowNumField);
            }
            if (includeFilenameInOutput) {
                headers.add("filename");
            }
            RowSetTable headerTable = new RowSetTable(headers);

            // Set header for all output channels
            for (Channel out : outputs) {
                out.setHeader(headerTable);
            }
            Log.header("ExcelInput", String.join(", ", headers));

            int startRowIndex = headerRow ? 1 : 0;
            for (int i = startRowIndex; i <= sheet.getLastRowNum(); i++) {
                org.apache.poi.ss.usermodel.Row excelRow = sheet.getRow(i);
                if (excelRow == null) continue;

                Row outRow = new Row();
                // Determine the number of actual data columns based on headers, excluding special fields
                int dataColumnCount = headers.size();
                if (includeFilenameInOutput) {
                    dataColumnCount--;
                }
                if (rowNumField != null && !rowNumField.isEmpty()) {
                    dataColumnCount--;
                }

                for (int c = 0; c < dataColumnCount; c++) {
                    Cell cell = excelRow.getCell(c);
                    String val = getCellString(cell);
                    outRow.add(val);
                }

                // Add row number field if configured
                if (rowNumField != null && !rowNumField.isEmpty()) {
                    outRow.add(String.valueOf(i + 1));
                }
                // Add filename field if configured
                if (includeFilenameInOutput) {
                    outRow.add(file.getName());
                }

                // Publish to all output channels
                for (Channel out : outputs) {
                    out.publish(outRow);
                }
                Log.data("ExcelInput", outRow.toString());
            }
        }
    }

    // The following helper methods remain unchanged
    private Sheet getSheet(Workbook workbook) {
        if (sheetName != null && !sheetName.isEmpty()) {
            return workbook.getSheet(sheetName);
        }
        return workbook.getSheetAt(0);
    }

    private List<String> extractHeader(Sheet sheet) {
        List<String> headers = new ArrayList<>();
        org.apache.poi.ss.usermodel.Row headerRow = sheet.getRow(0);
        if (headerRow == null) return headers;
        for (int i = 0; i < headerRow.getLastCellNum(); i++) {
            Cell cell = headerRow.getCell(i);
            String val = getCellString(cell);
            headers.add(val == null || val.isEmpty() ? "Column" + (i + 1) : val);
        }
        return headers;
    }

    private List<String> generateDefaultHeaders(Sheet sheet) {
        int colCount = 0;
        org.apache.poi.ss.usermodel.Row firstRow = sheet.getRow(0);
        if (firstRow != null) colCount = firstRow.getLastCellNum();
        List<String> headers = new ArrayList<>();
        for (int i = 0; i < colCount; i++) {
            headers.add("Column" + (i + 1));
        }
        return headers;
    }

    private String getCellString(Cell cell) {
        if (cell == null) {
            return nullToEmpty ? "" : null;
        }
        switch (cell.getCellType()) {
            case STRING: return trim(cell.getStringCellValue());
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue().toString();
                } else {
                    double num = cell.getNumericCellValue();
                    if (num == (long) num) return String.valueOf((long) num);
                    else return String.valueOf(num);
                }
            case BOOLEAN: return String.valueOf(cell.getBooleanCellValue());
            case FORMULA:
                try {
                    FormulaEvaluator evaluator = cell.getSheet().getWorkbook().getCreationHelper().createFormulaEvaluator();
                    CellValue evaluatedValue = evaluator.evaluate(cell);
                    switch (evaluatedValue.getCellType()) {
                        case STRING: return trim(evaluatedValue.getStringValue());
                        case NUMERIC: return String.valueOf(evaluatedValue.getNumberValue());
                        case BOOLEAN: return String.valueOf(evaluatedValue.getBooleanValue());
                        default: return nullToEmpty ? "" : null; // Handle other formula results
                    }
                } catch (Exception e) {
                    // Log the error but return the formula string or empty string
                    Log.warn("ExcelInput", "Error evaluating formula: " + cell.getCellFormula() + " - " + e.getMessage());
                    return nullToEmpty ? "" : cell.getCellFormula();
                }
            case BLANK: return nullToEmpty ? "" : null;
            default: return nullToEmpty ? "" : null;
        }
    }

    private String trim(String s) {
        return trimValues && s != null ? s.trim() : s;
    }
}