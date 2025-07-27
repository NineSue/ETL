package plugin;

import anno.Output;
import cn.hutool.poi.excel.ExcelUtil;
import cn.hutool.poi.excel.ExcelWriter;
import core.Channel;
import core.flowdata.Row;
import core.intf.IOutput;
import java.util.*;
import java.util.concurrent.*;

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
    private List<String> fieldNames;
    private ExecutorService executorService;
    private List<Future<?>> futures;
    private final int THREAD_POOL_SIZE = 4;

    @Override
    public void init(Map<String, Object> cfg) {
        this.config = cfg;
        this.filename = (String) config.get("filename");
        this.sheetname = (String) config.get("sheetname");
        this.append = convertToBoolean(config.get("append"), false);
        this.hasHeader = convertToBoolean(config.get("hasHeader"), true);
        this.fields = (List<Map<String, String>>) config.get("fields");

        if (filename == null || sheetname == null || fields == null || fields.isEmpty()) {
            throw new IllegalArgumentException("缺少必要的配置参数：filename、sheetname 或 fields");
        }
        this.fieldNames = new ArrayList<>();
        for (Map<String, String> field : fields) {
            fieldNames.add(field.get("fieldName"));
        }
        this.executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        this.futures = new ArrayList<>();
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
            input.subscribe(row -> {
                synchronized (this) {
                    try {
                        if (hasHeader && !headerWritten) {
                            writer.writeHeadRow(fieldNames);
                            headerWritten = true;
                        }
                        Future<?> future = executorService.submit(() -> {
                            try {
                                writer.writeRow(row);
                            } catch (Exception e) {
                                throw new RuntimeException("处理Excel数据时出错", e);
                            }
                        });
                        futures.add(future);
                    } catch (Exception e) {
                        throw new RuntimeException("处理Excel数据时出错", e);
                    }
                }
            });
            input.onReceive(null, () -> {
                synchronized (this) {
                    for (Future<?> future : futures) {
                        try {
                            future.get();
                        } catch (InterruptedException | ExecutionException e) {
                            throw new RuntimeException("线程执行出错", e);
                        }
                    }
                    if (writer != null) {
                        writer.close();
                        writer = null;
                    }
                    executorService.shutdown();
                }
            });
        } catch (Exception e) {
            if (writer != null) {
                writer.close();
            }
            executorService.shutdownNow();
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
