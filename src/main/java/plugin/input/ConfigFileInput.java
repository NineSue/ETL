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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
            if (filePath.endsWith(".properties")) {
                processPropertiesFile(outputs);
            } else if (filePath.endsWith(".ini")) {
                processIniFile(outputs);
            } else {
                Log.error("ConfigFileInput", "Unsupported file type. Only .properties and .ini files are supported.");
                return;
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

    private void processPropertiesFile(List<Channel> outputs) throws Exception {
        Properties properties = new Properties();
        properties.load(FileUtil.getInputStream(filePath));

        List<String> headers = new ArrayList<>();
        headers.add("key");
        headers.add("value");
        RowSetTable headerTable = new RowSetTable(headers);

        // 设置表头到所有输出通道
        for (Channel output : outputs) {
            output.setHeader(headerTable);
        }
        Log.header("ConfigFileInput", String.join(", ", headers));

        // 处理数据行
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            Row row = new Row();
            row.add(entry.getKey().toString());
            row.add(entry.getValue().toString());

            // 发布数据到所有输出通道
            for (Channel output : outputs) {
                output.publish(row);
            }
            Log.data("ConfigFileInput", row.toString());
        }
    }

    private void processIniFile(List<Channel> outputs) throws Exception {
        List<String> lines = FileUtil.readLines(filePath, StandardCharsets.UTF_8);
        Map<String, Map<String, String>> iniData = parseIni(lines);

        List<String> headers = new ArrayList<>();
        headers.add("section");
        headers.add("key");
        headers.add("value");
        RowSetTable headerTable = new RowSetTable(headers);

        // 设置表头到所有输出通道
        for (Channel output : outputs) {
            output.setHeader(headerTable);
        }
        Log.header("ConfigFileInput", String.join(", ", headers));

        // 处理数据行
        for (Map.Entry<String, Map<String, String>> sectionEntry : iniData.entrySet()) {
            String section = sectionEntry.getKey();
            Map<String, String> sectionData = sectionEntry.getValue();
            for (Map.Entry<String, String> entry : sectionData.entrySet()) {
                Row row = new Row();
                row.add(section);
                row.add(entry.getKey());
                row.add(entry.getValue());

                // 发布数据到所有输出通道
                for (Channel output : outputs) {
                    output.publish(row);
                }
                Log.data("ConfigFileInput", row.toString());
            }
        }
    }

    private Map<String, Map<String, String>> parseIni(List<String> lines) {
        Map<String, Map<String, String>> iniData = new HashMap<>();
        String currentSection = null;
        Pattern sectionPattern = Pattern.compile("\\[(.*)\\]");
        Pattern keyValuePattern = Pattern.compile("(.*)=(.*)");

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty() || line.startsWith(";")) {
                continue;
            }

            Matcher sectionMatcher = sectionPattern.matcher(line);
            if (sectionMatcher.matches()) {
                currentSection = sectionMatcher.group(1);
                iniData.putIfAbsent(currentSection, new HashMap<>());
            } else {
                Matcher keyValueMatcher = keyValuePattern.matcher(line);
                if (keyValueMatcher.matches()) {
                    String key = keyValueMatcher.group(1).trim();
                    String value = keyValueMatcher.group(2).trim();
                    if (currentSection != null) {
                        iniData.get(currentSection).put(key, value);
                    }
                }
            }
        }
        return iniData;
    }
}