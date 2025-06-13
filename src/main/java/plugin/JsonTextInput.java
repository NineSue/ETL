package plugin;

import anno.Input;
import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IInput;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @brief JSON 文本输入插件，支持 file / field / url 三种模式
 *
 * @param config 配置示例：
 * {
 *   "filePath": "src/test/java/yh_json.txt",   // mode=file时必填文件路径
 *   "delimiter": ",",                          // 可选：字段分隔符，默认 ,
 *   "mode": "file",                            // 可选：file / field / url，默认 file
 *   "hasHeader": true,                         // 可选，默认 true，是否跳过表头
 *   "jsonField": "json_data",                  // mode = field/url 时必填：哪一列是 JSON 或 URL
 *   "fields": ["age", "city"]                  // 可选：指定只提取哪些 JSON 字段
 * }
 */
@Input(type = "jsontext")
public class JsonTextInput implements IInput {

    @Override
    public RowSetTable deal(Object config) {
        if (!(config instanceof Map)) {
            System.err.println("配置不是 Map 类型！");
            return null;
        }

        Map<String, Object> conf = (Map<String, Object>) config;
        String mode = (String) conf.getOrDefault("mode", "file");

        switch (mode.toLowerCase()) {
            case "file":
                return parseFileMode(conf);
            case "field":
                return parseFieldMode(conf);
            case "url":
                return parseUrlJson();
            default:
                System.err.println("不支持的 mode：" + mode);
                return null;
        }
    }

    private RowSetTable parseFileMode(Map<String, Object> conf) {
        String filePath = (String) conf.get("filePath");
        if (filePath == null || filePath.isEmpty()) {
            System.err.println("filePath 不能为空！");
            return null;
        }

        String delimiterStr = (String) conf.getOrDefault("delimiter", ",");
        Boolean hasHeader = (Boolean) conf.getOrDefault("hasHeader", true);
        List<String> fields = (List<String>) conf.getOrDefault("fields", new ArrayList<>());

        List<String> lines = FileUtil.readLines(new File(filePath), StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            System.err.println("文件为空！");
            return null;
        }

        List<String> dataLines = new ArrayList<>(lines);
        if (hasHeader && dataLines.size() > 1) {
            dataLines = dataLines.subList(1, dataLines.size());
        }

        return parseWholeLineJson(dataLines, delimiterStr, fields);
    }

    private RowSetTable parseFieldMode(Map<String, Object> conf) {
        String sourceField = (String) conf.get("sourceField");
        if (sourceField == null || sourceField.isEmpty()) {
            System.err.println("sourceField 不能为空！");
            return null;
        }

        String delimiterStr = (String) conf.getOrDefault("delimiter", ",");
        String jsonField = (String) conf.get("jsonField");
        List<String> fields = (List<String>) conf.getOrDefault("fields", new ArrayList<>());

        List<String> fieldLines = Arrays.asList(sourceField.split("\n"));
        return parseJsonField(fieldLines, delimiterStr, jsonField, fields, true);
    }

    private RowSetTable parseWholeLineJson(List<String> lines, String delimiterStr, List<String> fields) {
        Set<String> headerSet = new LinkedHashSet<>();
        List<JSONObject> jsonObjs = new ArrayList<>();

        Pattern pattern = Pattern.compile("^(\\d+),([^,]+),(.*})$");

        for (String line : lines) {
            Matcher matcher = pattern.matcher(line);
            if (!matcher.matches()) {
                System.err.println("数据格式错误，行：" + line);
                continue;
            }

            String jsonPart = matcher.group(3).trim();
            try {
                JSONObject json = JSONUtil.parseObj(jsonPart);
                if (fields.isEmpty()) {
                    headerSet.addAll(json.keySet());
                } else {
                    headerSet.addAll(fields);
                }
                jsonObjs.add(json);
            } catch (Exception e) {
                System.err.println("JSON 解析失败：" + line);
                e.printStackTrace();
            }
        }

        RowSetTable table = new RowSetTable(new ArrayList<>(headerSet));
        for (JSONObject json : jsonObjs) {
            Row row = new Row();
            for (String key : headerSet) {
                row.add(json.getStr(key, ""));
            }
            table.addRow(row);
        }
        return table;
    }

    private RowSetTable parseJsonField(List<String> lines, String delimiterStr, String jsonField, List<String> fields, boolean hasHeader) {
        if (lines == null || lines.isEmpty()) {
            System.err.println("输入数据为空！");
            return null;
        }

        String[] headerLine = lines.get(0).split(delimiterStr, -1);
        int jsonFieldIndex = Arrays.asList(headerLine).indexOf(jsonField);
        if (jsonFieldIndex == -1) {
            System.err.println("找不到字段：" + jsonField);
            return null;
        }

        List<String> dataLines = hasHeader ? lines.subList(1, lines.size()) : lines;

        Set<String> jsonKeys = new LinkedHashSet<>();
        List<String[]> validRows = new ArrayList<>();

        Pattern jsonPattern = Pattern.compile("^(\\d+),([^,]+),(.*})$");

        for (String line : dataLines) {
            Matcher matcher = jsonPattern.matcher(line);
            if (!matcher.matches()) {
                System.err.println("数据格式错误，行：" + line);
                continue;
            }

            String[] parts = new String[3];
            parts[0] = matcher.group(1).trim();
            parts[1] = matcher.group(2).trim();
            parts[2] = matcher.group(3).trim();

            try {
                JSONObject json = JSONUtil.parseObj(parts[2]);
                if (fields.isEmpty()) {
                    jsonKeys.addAll(json.keySet());
                } else {
                    jsonKeys.addAll(fields);
                }
                validRows.add(parts);
            } catch (Exception e) {
                System.err.println("JSON 字段解析失败：" + parts[2]);
            }
        }

        List<String> finalHeader = new ArrayList<>();
        for (int i = 0; i < headerLine.length; i++) {
            if (i != jsonFieldIndex) {
                finalHeader.add(headerLine[i]);
            }
        }
        finalHeader.addAll(jsonKeys);

        RowSetTable table = new RowSetTable(finalHeader);
        for (String[] parts : validRows) {
            Row row = new Row();
            for (int i = 0; i < parts.length; i++) {
                if (i == jsonFieldIndex) continue;
                row.add(parts[i]);
            }
            JSONObject json = JSONUtil.parseObj(parts[jsonFieldIndex]);
            for (String key : jsonKeys) {
                row.add(json.getStr(key, ""));
            }
            table.addRow(row);
        }
        return table;
    }

    private RowSetTable parseUrlJson() {
        System.err.println("【提醒】mode=url 模式暂未实现，已预留结构。");
        return new RowSetTable(Collections.singletonList("URL模式尚未实现"));
    }
}