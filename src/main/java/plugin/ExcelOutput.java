package plugin;

import anno.Output;
import cn.hutool.poi.excel.ExcelUtil;
import cn.hutool.poi.excel.ExcelWriter;
import core.Channel;
import core.flowdata.Row;
import core.intf.IOutput;
import java.util.*;

@Output(type = "excelOutput")
public class ExcelOutput implements IOutput {
    private Map<String, Object> config;
    private ExcelWriter writer;
    private String filename;
    private String sheetname;
    private boolean append;
    private boolean hasHeader;
    private List<Map<String, String>> fields;
    private boolean headerWritten = false;
    private List<String> fieldNames; // 存储字段名顺序

    @Override
    public void init(Map<String, Object> cfg) {
        this.config = cfg;
        // 初始化配置参数
        this.filename = (String) config.get("filename");
        this.sheetname = (String) config.get("sheetname");
        this.append = convertToBoolean(config.get("append"), false);
        this.hasHeader = convertToBoolean(config.get("hasHeader"), true);
        this.fields = (List<Map<String, String>>) config.get("fields");

        // 验证必要配置
        if (filename == null || sheetname == null || fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("缺少必要的配置参数：filename、sheetname 或 fields");
        }

        // 提取字段名顺序
        this.fieldNames = new ArrayList<>();
        for (Map<String, String> field : fields) {
            fieldNames.add(field.get("fieldName"));
        }

        // 初始化ExcelWriter
        if (append) {
            writer = ExcelUtil.getWriter(filename, sheetname);
        } else {
            writer = ExcelUtil.getWriter(filename);
            writer.setSheet(sheetname);
        }
    }

    @Override
    public void consume(Channel<Row> input) throws Exception {
        try {
            // 订阅Channel中的数据
            input.subscribe(row -> {
                synchronized (this) {
                    try {
                        // 如果需要表头且尚未写入
                        if (hasHeader && !headerWritten) {
                            writer.writeHeadRow(fieldNames);
                            headerWritten = true;
                        }

                        // 直接写入行数据，因为Row是ArrayList<Object>
                        // 假设字段顺序与Row中的数据顺序一致
                        writer.writeRow(row);
                    } catch (Exception e) {
                        throw new RuntimeException("处理Excel数据时出错", e);
                    }
                }
            });

            // 等待所有数据处理完成
            input.onReceive(null, () -> {
                synchronized (this) {
                    if (writer != null) {
                        writer.close();
                        writer = null;
                    }
                }
            });
        } catch (Exception e) {
            if (writer != null) {
                writer.close();
            }
            throw e;
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