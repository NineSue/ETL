package plugin.output;

import anno.Output;
import core.Channel;
import core.flowdata.RowSetTable;
import core.intf.IOutput;
import tool.Log;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

//TODO 已从旧版迭代为新版，待测试
@Output(type = "table")
public class TableOutput implements IOutput {

    private String connectionId;
    private String tableName;

    // IOutput 接口的 init 方法，用于初始化插件配置
    @Override
    public void init(Map<String, Object> cfg) {
        this.connectionId = (String) cfg.get("connectionId");
        this.tableName = (String) cfg.get("tableName");
        Log.info("TableOutput", "Initialized with connectionId: " + connectionId + ", tableName: " + tableName);
    }

    // 接收上游数据
    @Override
    public void consume(Channel input) {
        Log.info("TableOutput", "Starting to consume data from input channel.");

        // 订阅输入通道，当有数据到达时进行处理
        input.onReceive(rowSetTableObj -> {
            if (!(rowSetTableObj instanceof RowSetTable)) {
                Log.warn("TableOutput", "Received non-RowSetTable object from channel, skipping.");
                return;
            }
            RowSetTable table = (RowSetTable) rowSetTableObj;

            // 参数校验
            if (connectionId == null || connectionId.isEmpty()) {
                Log.error("TableOutput", "Missing required configuration parameter: 'connectionId'. Cannot send data.");
                return;
            }
            if (tableName == null || tableName.isEmpty()) {
                Log.error("TableOutput", "Missing required configuration parameter: 'tableName'. Cannot send data.");
                return;
            }
            if (table == null) {
                Log.error("TableOutput", "Input RowSetTable is null. Cannot send data.");
                return;
            }

            // 生成 SQL 插入语句
            String[] sqlStatements = table.getInsertSQL(tableName);

            // 逐条发送 SQL 语句
            for (String sql : sqlStatements) {
                // 构造请求数据
                String requestData = "{\"connectionId\": \"" + connectionId + "\", \"sql\": \"" + escapeJson(sql) + "\"}";
                String requestUrl = "http://localhost:3000/api/output/execute";

                Log.info("TableOutput", "Sending request to URL: " + requestUrl);
                Log.debug("TableOutput", "Request Body: " + requestData); // 使用 debug 级别，避免过多日志

                // 发送 HTTP POST 请求到 Node.js 后端
                try {
                    URL url = new URL(requestUrl);
                    HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                    connection.setRequestMethod("POST");
                    connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                    connection.setRequestProperty("Accept", "application/json");
                    connection.setDoOutput(true);

                    try (OutputStream os = connection.getOutputStream()) {
                        byte[] inputBytes = requestData.getBytes(StandardCharsets.UTF_8);
                        os.write(inputBytes, 0, inputBytes.length);
                    }

                    int responseCode = connection.getResponseCode();
                    Log.info("TableOutput", "HTTP Response Code: " + responseCode);

                    // 读取响应体
                    StringBuilder response = new StringBuilder();
                    try (BufferedReader in = new BufferedReader(new InputStreamReader(
                            responseCode >= 200 && responseCode < 300 ? connection.getInputStream() : connection.getErrorStream()
                    ))) {
                        String inputLine;
                        while ((inputLine = in.readLine()) != null) {
                            response.append(inputLine);
                        }
                    }
                    Log.info("TableOutput", "HTTP Response Body: " + response.toString());

                    if (responseCode == HttpURLConnection.HTTP_OK) {
                        Log.success("TableOutput", "Data insertion successful for SQL statement.");
                    } else {
                        Log.error("TableOutput", "Data insertion failed, HTTP response code: " + responseCode + ". Response: " + response.toString());
                    }
                } catch (IOException e) {
                    Log.error("TableOutput", "Failed to send request: " + e.getMessage());
                }
            }
        }, () -> {
            // 当上游通道关闭时，此回调会被触发
            Log.info("TableOutput", "Input channel closed, TableOutput finished processing all available data.");
        });
    }

    // 转义 JSON 字符串，保持不变
    private String escapeJson(String input) {
        if (input == null) {
            return "";
        }
        return input.replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}