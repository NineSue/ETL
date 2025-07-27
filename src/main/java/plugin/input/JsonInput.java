package plugin.input;

import anno.Input;
import cn.hutool.core.io.FileUtil;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import core.Channel;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IInput;
import tool.Log;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Input(type = "json")
public class JsonInput implements IInput {

    private String mode;
    private String filePath;
    private String url;
    private String sourceString;
    private String jsonRootPath;
    private List<Map<String, String>> fieldMappings;

    @Override
    public void init(Map<String, Object> cfg) {
        this.mode = (String) cfg.getOrDefault("mode", "file");
        this.filePath = (String) cfg.get("filePath");
        this.url = (String) cfg.get("url");
        this.sourceString = (String) cfg.get("sourceString");
        this.jsonRootPath = (String) cfg.get("jsonRootPath");
        this.fieldMappings = (List<Map<String, String>>) cfg.get("fieldMappings");

        if (fieldMappings == null || fieldMappings.isEmpty()) {
            throw new IllegalArgumentException("Missing required configuration: 'fieldMappings'.");
        }

        Log.info("JsonInput", "Initialized: mode=" + mode +
                ", filePath=" + (filePath != null ? filePath : "null") +
                ", url=" + (url != null ? url : "null") +
                ", sourceString=" + (sourceString != null ? "..." : "null") +
                ", jsonRootPath=" + (jsonRootPath != null ? jsonRootPath : "null") +
                ", fieldMappings=" + fieldMappings.size() + " entries");
    }

    @Override
    public void start(List<Channel> outputs) throws Exception {
        if (outputs == null || outputs.isEmpty()) {
            Log.error("JsonInput", "No output channels provided. Data will not be published.");
            return;
        }
        try {
            processDataAndPublish(outputs);
        } catch (Exception e) {
            Log.error("JsonInput", "Error during data processing: " + e.getMessage());
            throw e;
        } finally {
            for (Channel out : outputs) {
                out.close();
            }
            Log.success("JsonInput", "Finished processing and publishing data.");
        }
    }

    private void processDataAndPublish(List<Channel> outputs) throws Exception {
        String rawJsonContent;

        // 读取 JSON 字符串
        switch (mode.toLowerCase()) {
            case "file":
                if (filePath == null || filePath.isEmpty()) {
                    throw new IllegalArgumentException("File mode: 'filePath' cannot be empty.");
                }
                File file = new File(filePath);
                if (!file.exists()) {
                    throw new IllegalArgumentException("File not found: " + filePath);
                }
                rawJsonContent = FileUtil.readString(file, StandardCharsets.UTF_8);
                Log.info("JsonInput", "Read content from file: " + filePath);
                break;
            case "url":
                if (url == null || url.isEmpty()) {
                    throw new IllegalArgumentException("URL mode: 'url' cannot be empty.");
                }
                HttpResponse response = HttpRequest.get(url).execute();
                if (response.getStatus() != 200) {
                    throw new RuntimeException("HTTP request failed: " + response.getStatus() + ", body: " + response.body());
                }
                rawJsonContent = response.body();
                Log.info("JsonInput", "Fetched content from URL: " + url);
                break;
            case "string":
                if (sourceString == null || sourceString.isEmpty()) {
                    throw new IllegalArgumentException("String mode: 'sourceString' cannot be empty.");
                }
                rawJsonContent = sourceString;
                Log.info("JsonInput", "Using raw JSON string.");
                break;
            default:
                throw new IllegalArgumentException("Unsupported mode: " + mode);
        }

        if (rawJsonContent.trim().isEmpty()) {
            Log.warn("JsonInput", "Raw JSON is empty.");
            return;
        }

        Object parsedJson = JSONUtil.parse(rawJsonContent);
        List<JSONObject> jsonObjectsToProcess = new ArrayList<>();

        // 修复逻辑：支持任意根类型 + JSONPath
        Object rootElement = parsedJson;
        if (jsonRootPath != null && !jsonRootPath.isEmpty()) {
            rootElement = JSONUtil.getByPath((JSON) parsedJson, jsonRootPath);
        }

        if (rootElement instanceof JSONArray) {
            for (Object item : (JSONArray) rootElement) {
                if (item instanceof JSONObject) {
                    jsonObjectsToProcess.add((JSONObject) item);
                } else {
                    Log.warn("JsonInput", "Skip non-JSONObject item: " + item);
                }
            }
        } else if (rootElement instanceof JSONObject) {
            jsonObjectsToProcess.add((JSONObject) rootElement);
        } else {
            throw new RuntimeException("Resolved root is neither JSON array nor object.");
        }

        if (jsonObjectsToProcess.isEmpty()) {
            Log.warn("JsonInput", "No valid JSON objects to process.");
            return;
        }

        // 设置 header
        List<String> headers = new ArrayList<>();
        for (Map<String, String> map : fieldMappings) {
            String name = map.get("outputName");
            if (name == null || name.isEmpty()) throw new IllegalArgumentException("Missing outputName.");
            headers.add(name);
        }

        RowSetTable header = new RowSetTable(headers);
        for (Channel out : outputs) {
            out.setHeader(header);
        }
        Log.header("JsonInput", "Output header set: " + header.getField());

        // 逐条发出数据
        for (JSONObject json : jsonObjectsToProcess) {
            Row row = new Row();
            for (Map<String, String> map : fieldMappings) {
                String path = map.get("jsonPath");
                Object val = JSONUtil.getByPath(json, path);
                row.add(val != null ? val.toString() : "");
            }
            for (Channel out : outputs) {
                out.publish(row);
                Log.data("JsonInput", "Published row: " + row);
            }
        }
    }
}
