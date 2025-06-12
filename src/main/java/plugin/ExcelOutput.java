package plugin;

import anno.Output;
import cn.hutool.poi.excel.ExcelUtil;
import cn.hutool.poi.excel.ExcelWriter;
import core.intf.IOutput;
import java.util.*;
@Output(type = "excelOutput")
public class ExcelOutput implements IOutput {

    private List<Map<String, Object>> inputData;

    public ExcelOutput() {
    }
    public void setInputData(List<Map<String, Object>> inputData) {
        this.inputData = inputData;
    }
    @Override
    public void deal(Object config) {
        if (!(config instanceof Map)) {
            throw new IllegalArgumentException("配置必须是一个 Map 对象");
        }
        Map<String, Object> configMap = (Map<String, Object>) config;
        String filename = (String) configMap.get("filename");
        String sheetname = (String) configMap.get("sheetname");
        boolean append = convertToBoolean(configMap.get("append"), false);
        boolean hasHeader = convertToBoolean(configMap.get("hasHeader"), true);
        List<Map<String, String>> fields = (List<Map<String, String>>) configMap.get("fields");
        if (filename == null || sheetname == null || fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("缺少必要的配置参数：filename、sheetname 或 fields");
        }
        if (inputData == null || inputData.isEmpty()) {
            throw new IllegalStateException("没有可用的输入数据");
        }
        ExcelWriter writer;
        if (append) {
            writer = ExcelUtil.getWriter(filename, sheetname);
        } else {
            writer = ExcelUtil.getWriter(filename);
            writer.setSheet(sheetname);
        }

        try {
            if (hasHeader) {
                List<String> headerList = new ArrayList<>();
                for (Map<String, String> field : fields) {
                    headerList.add(field.get("fieldName"));
                }
                writer.writeHeadRow(headerList);
            }
            for (Map<String, Object> rowData : inputData) {
                List<Object> rowValues = new ArrayList<>();
                for (Map<String, String> field : fields) {
                    String fieldName = field.get("fieldName");
                    rowValues.add(rowData.get(fieldName));
                }
                writer.writeRow(rowValues);
            }
        } finally {
            writer.close();
        }
    }
    private boolean convertToBoolean(Object value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        return defaultValue;
    }
}