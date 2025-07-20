package plugin.input;

import anno.Input;
import cn.hutool.core.io.FileUtil;
import core.Channel;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IInput;
import tool.Log;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Input(type = "configfile")
public class ConfigFileInput implements IInput {

    private String filePath;

    @Override
    public void init(Map<String, Object> cfg) {
        this.filePath = (String) cfg.get("filePath");

        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("Missing file path");
        }

        Log.info("ConfigFileInput", "Init with path: " + filePath);
    }

    @Override
    public void start(List<Channel> outputs) throws Exception {
        if (outputs == null || outputs.isEmpty()) {
            Log.error("ConfigFileInput", "No output channels provided. Data will not be published.");
            return;
        }

        try {
            List<String> lines = FileUtil.readLines(filePath, StandardCharsets.UTF_8);
            if (lines.isEmpty()) {
                Log.warn("ConfigFileInput", "Config file is empty.");
                return;
            }

            // 假设第一行为表头
            String headerLine = lines.get(0);
            String[] headers = headerLine.split(",");
            List<String> headerList = new ArrayList<>();
            for (String header : headers) {
                headerList.add(header.trim());
            }
            RowSetTable headerTable = new RowSetTable(headerList);

            // 设置表头到所有输出通道
            for (Channel output : outputs) {
                output.setHeader(headerTable);
            }
            Log.header("ConfigFileInput", String.join(", ", headerList));

            // 处理数据行
            for (int i = 1; i < lines.size(); i++) {
                String line = lines.get(i).trim();
                if (line.isEmpty()) continue;

                String[] values = line.split(",");
                Row row = new Row();
                for (String value : values) {
                    row.add(value.trim());
                }

                // 发布数据到所有输出通道
                for (Channel output : outputs) {
                    output.publish(row);
                }
                Log.data("ConfigFileInput", row.toString());
            }
        } catch (Exception e) {
            Log.error("ConfigFileInput", "Error reading config file: " + e.getMessage());
            throw e;
        } finally {
            // 关闭所有输出通道以表示流结束
            for (Channel output : outputs) {
                output.close();
            }
            Log.info("ConfigFileInput", "All output channels closed");
        }
    }
}