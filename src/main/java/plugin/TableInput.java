//package plugin;
//
//import anno.Input;
//import cn.hutool.http.HttpRequest;
//import cn.hutool.http.HttpResponse;
//import cn.hutool.http.HttpUtil;
//import cn.hutool.json.JSONArray;
//import cn.hutool.json.JSONObject;
//import cn.hutool.json.JSONUtil;
//import core.flowdata.Row;
//import core.flowdata.RowSetTable;
//import core.intf.IInput;
//
//import java.nio.charset.StandardCharsets;
//import java.util.*;
//
///**
// * 表输入插件（通过 HTTP 接口获取表格数据）
// */
//@Input(type = "http") //
//public class TableInput implements IInput {
//
//    @Override
//    public RowSetTable deal(Object config) {
//        if (!(config instanceof Map<?, ?>)) {
//            System.err.println("HTTP配置不是Map类型！");
//            return null;
//        }
//
//        Map<String, Object> conf = (Map<String, Object>) config;
//        String url = (String) conf.get("url");
//        String method = ((String) conf.getOrDefault("method", "POST")).toUpperCase();
//        String body = (String) conf.getOrDefault("body", "");
//        Map<String, String> headers = (Map<String, String>) conf.getOrDefault("headers", new HashMap<>());
//
//        if (url == null || url.isEmpty()) {
//            System.err.println("缺少url参数！");
//            return null;
//        }
//
//        HttpRequest request = method.equals("POST")
//                ? HttpRequest.post(url).body(body, String.valueOf(StandardCharsets.UTF_8))
//                : HttpRequest.get(url);
//
//        request.headerMap(headers, true);
//
//        HttpResponse response;
//        try {
//            response = request.execute();
//        } catch (Exception e) {
//            System.err.println("HTTP请求失败：" + e.getMessage());
//            return null;
//        }
//
//        if (response.getStatus() != 200) {
//            System.err.println("HTTP请求返回状态码：" + response.getStatus());
//            return null;
//        }
//
//        String responseBody = response.body();
//        JSONObject json;
//        try {
//            json = JSONUtil.parseObj(responseBody);
//        } catch (Exception e) {
//            System.err.println("返回不是有效JSON：" + e.getMessage());
//            return null;
//        }
//
//        if (!json.containsKey("data")) {
//            System.err.println("返回JSON不包含 'data' 字段！");
//            return null;
//        }
//
//        JSONObject data = json.getJSONObject("data");
//        JSONArray columns = data.getJSONArray("columns");
//        JSONArray rows = data.getJSONArray("rows");
//
//        if (columns == null || rows == null) {
//            System.err.println("返回数据不包含 columns 或 rows");
//            return null;
//        }
//
//        List<String> headersList = new ArrayList<>();
//        for (Object col : columns) {
//            headersList.add(col.toString());
//        }
//
//        RowSetTable table = new RowSetTable(headersList);
//
//        for (Object rowObj : rows) {
//            if (rowObj instanceof JSONObject) {
//                JSONObject rowJson = (JSONObject) rowObj;
//                Row row = new Row();
//                for (String header : headersList) {
//                    Object value = rowJson.getObj(header);
//                    row.add(value != null ? value.toString() : "");
//                }
//                table.addRow(row);
//            }
//        }
//
//        return table;
//    }
//}
