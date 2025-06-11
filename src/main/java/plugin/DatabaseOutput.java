package plugin;

import anno.Output;
import core.flowdata.RowSetTable;
import core.intf.IOutput;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Output(type = "database")
public class DatabaseOutput implements IOutput {

    /**
     * @brief 数据库输出插件，用于将数据以 SQL 插入语句的形式发送到指定的数据库连接。
     *
     * @param config 类型为 Map 的配置对象
     * {
     *   "connectionId": "连接 ID",          // 必选，数据库连接标识
     *   "tableName": "表名",                // 必选，目标数据库表名
     *   "table": RowSetTable                 // 必选，包含数据的 RowSetTable 对象
     * }
     *
     * @return 无返回值，通过控制台输出 HTTP 响应结果
     */
    @Override
    public void deal(Object config) {
        // 读取配置
        if (!(config instanceof Map<?, ?>)) {
            System.err.println("配置参数不是 Map 类型！");
            return;
        }

        Map<String, Object> confMap = (Map<String, Object>) config;
        String connectionId = (String) confMap.get("connectionId");
        String tableName = (String) confMap.get("tableName");
        RowSetTable table = (RowSetTable) confMap.get("_input");

        // 参数校验
        if (connectionId == null || connectionId.isEmpty() ||
                tableName == null || tableName.isEmpty() ||
                table == null) {
            System.err.println("缺少必要的配置参数！");
            return;
        }

        // 生成 SQL 插入语句
        String[] sqlStatements = table.getInsertSQL(tableName);

        // 逐条发送 SQL 语句
        for (String sql : sqlStatements) {
            // 构造请求数据
            String requestData = "{\"connectionId\": \"" + connectionId + "\", \"sql\": \"" + escapeJson(sql) + "\"}";

            // 打印请求数据
            System.out.println("请求 URL: " + "http://localhost:3000/api/output/execute");
            System.out.println("请求体: " + requestData);

            // 发送 HTTP POST 请求到 Node.js 后端
            try {
                URL url = new URL("http://localhost:3000/api/output/execute");
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
                connection.setRequestProperty("Accept", "application/json");
                connection.setDoOutput(true);

                try (OutputStream os = connection.getOutputStream()) {
                    byte[] input = requestData.getBytes(StandardCharsets.UTF_8);
                    os.write(input, 0, input.length);
                }

                int responseCode = connection.getResponseCode();
                System.out.println("HTTP 响应码: " + responseCode);

                // 读取响应体
                StringBuilder response = new StringBuilder();
                try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                    String inputLine;
                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                }

                System.out.println("HTTP 响应体: " + response.toString());

                if (responseCode == HttpURLConnection.HTTP_OK) {
                    System.out.println("数据插入成功！");
                } else {
                    System.err.println("数据插入失败，HTTP响应码：" + responseCode);
                }
            } catch (IOException e) {
                System.err.println("发送请求失败：" + e.getMessage());
            }
        }
    }

    // 转义 JSON 字符串
    private String escapeJson(String input) {
        return input.replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}