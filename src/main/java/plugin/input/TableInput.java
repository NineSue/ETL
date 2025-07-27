package plugin.input;

import anno.Input;
import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import core.Channel;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IInput;
import tool.Log;
import java.nio.charset.StandardCharsets;
import java.util.*;

//TODO 已从旧版迭代为新版，待测试
@Input(type = "table")
public class TableInput implements IInput {

    private String url;
    private String method;
    private String body;
    private Map<String, String> headers;

    @Override
    public void init(Map<String, Object> cfg) {
        this.url = (String) cfg.get("url");
        this.method = ((String) cfg.getOrDefault("method", "POST")).toUpperCase();
        this.body = (String) cfg.getOrDefault("body", "");
        this.headers = (Map<String, String>) cfg.getOrDefault("headers", new HashMap<>());

        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("Missing URL parameter.");
        }
        if (!"GET".equals(method) && !"POST".equals(method)) {
            throw new IllegalArgumentException("Unsupported HTTP method: " + method + ". Only GET/POST are supported.");
        }

        Log.info("TableInput", "Initialized. URL: " + url + ", Method: " + method);
        if (!body.isEmpty()) {
            Log.debug("TableInput", "Request Body (first 100 chars): " + body.substring(0, Math.min(body.length(), 100)) + (body.length() > 100 ? "..." : ""));
        }
    }

    @Override
    public void start(List<Channel> outputs) throws Exception {
        if (outputs == null || outputs.isEmpty()) {
            Log.error("TableInput", "No output channels provided. Data will not be published.");
            return;
        }

        Log.info("TableInput", "Starting HTTP request.");
        long startTime = System.currentTimeMillis();

        HttpRequest request;
        if ("POST".equals(method)) {
            request = HttpRequest.post(url).body(body, String.valueOf(StandardCharsets.UTF_8));
        } else { // GET
            request = HttpRequest.get(url);
        }
        request.headerMap(headers, true);

        HttpResponse response;
        try {
            response = request.execute();
        } catch (Exception e) {
            Log.error("TableInput", "HTTP request failed: " + e.getMessage());
            throw new RuntimeException("HTTP请求失败", e);
        }

        if (response.getStatus() != 200) {
            Log.error("TableInput", "HTTP request returned non-200 status: " + response.getStatus() + ". Body: " + response.body());
            throw new RuntimeException("HTTP请求返回非200状态码: " + response.getStatus());
        }

        String responseBody = response.body();
        JSONObject json;
        try {
            json = JSONUtil.parseObj(responseBody);
        } catch (Exception e) {
            Log.error("TableInput", "Response is not valid JSON: " + e.getMessage());
            throw new RuntimeException("返回不是有效JSON", e);
        }

        if (!json.containsKey("data")) {
            Log.error("TableInput", "Returned JSON does not contain 'data' field.");
            throw new RuntimeException("返回JSON不包含 'data' 字段");
        }

        JSONObject data = json.getJSONObject("data");
        JSONArray columns = data.getJSONArray("columns");
        JSONArray rows = data.getJSONArray("rows");

        if (columns == null || rows == null) {
            Log.error("TableInput", "Returned data does not contain 'columns' or 'rows' arrays.");
            throw new RuntimeException("返回数据不包含 columns 或 rows");
        }

        List<String> headersList = new ArrayList<>();
        for (Object col : columns) {
            headersList.add(col != null ? col.toString() : "");
        }

        RowSetTable table = new RowSetTable(headersList);
        Log.header("TableInput", String.join(", ", headersList));

        for (Object rowObj : rows) {
            if (rowObj instanceof JSONObject) {
                JSONObject rowJson = (JSONObject) rowObj;
                Row row = new Row();
                for (String header : headersList) {
                    Object value = rowJson.getObj(header);
                    row.add(value != null ? value.toString() : "");
                }
                table.addRow(row);
                Log.data("TableInput", row.toString());
            } else {
                Log.warn("TableInput", "Skipping non-JSONObject row: " + rowObj);
            }
        }

        // Publish data to all output channels
        for (Channel out : outputs) {
            // 修正：使用 new RowSetTable(table.getField()) 来获取表头模式
            out.setHeader(new RowSetTable(table.getField()));
            // 修正：使用 table.getRowList() 来获取所有行
            for (Row row : table.getRowList()) {
                out.publish(row);
            }
        }

        long totalTime = System.currentTimeMillis() - startTime;
        // 使用 table.getRowList().size() 来获取行数
        long rowsPerSecond = table.getRowList().size() * 1000 / Math.max(totalTime, 1);

        Log.success("TableInput", "HTTP request completed.");
        //使用 table.getRowList().size() 来获取行数
        Log.success("TableInput", "Total rows: " + table.getRowList().size());
        Log.success("TableInput", "Time: " + totalTime + "ms");
        Log.success("TableInput", "Speed: " + rowsPerSecond + " rows/sec");

        // Close all output channels to signal end of stream
        for (Channel out : outputs) {
            out.close();
        }
        Log.info("TableInput", "All output channels closed.");
    }
}
