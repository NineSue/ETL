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

/**
 * @brief JSON文本输入插件：读取一行一行的文本数据，每行格式如：
 * id,name,{"age":30,"city":"New York"}
 *
 * @param config 配置示例：
 * {
 *   "sourceType": "file",         // 必选: file / field / url，目前仅支持file
 *   "filePath": "src/xxx.txt"     // 若 sourceType 为 file
 *   // "sourceField": ""          // 若 sourceType 为 field
 *   // "url": ""                  // 若 sourceType 为 url
 * }
 */
@Input(type = "jsontext")
public class JsonTextInput implements IInput {

    @Override
    public RowSetTable deal(Object config) {
        if (!(config instanceof Map<?, ?>)) {
            System.err.println("配置格式错误，应为 Map");
            return null;
        }

        Map<String, Object> confMap = (Map<String, Object>) config;
        String sourceType = (String) confMap.get("sourceType");

        if (sourceType == null || sourceType.isEmpty()) {
            System.err.println("缺少 sourceType 配置");
            return null;
        }

        List<String> lines = new ArrayList<>();
        switch (sourceType) {
            case "file":
                String filePath = (String) confMap.get("filePath");
                if (filePath == null || filePath.isEmpty()) {
                    System.err.println("sourceType 为 file 时，filePath 不能为空");
                    return null;
                }
                File file = new File(filePath);
                if (!file.exists()) {
                    System.err.println("文件不存在: " + filePath);
                    return null;
                }
                lines = FileUtil.readLines(file, StandardCharsets.UTF_8);
                break;

            // 留出位置后续支持其他 sourceType
            case "field":
            case "url":
                System.err.println("暂未支持 sourceType: " + sourceType);
                return null;

            default:
                System.err.println("未知的 sourceType: " + sourceType);
                return null;
        }

        if (lines.isEmpty()) {
            System.err.println("数据源为空");
            return null;
        }

        // 表头固定为 id, name, age, city
        List<String> headers = Arrays.asList("id", "name", "age", "city");
        RowSetTable table = new RowSetTable(headers);

        for (String line : lines) {
            String[] parts = line.split(",", 3);
            if (parts.length != 3) {
                System.err.println("数据行格式错误: " + line);
                continue;
            }

            String id = parts[0].trim();
            String name = parts[1].trim();
            String jsonStr = parts[2].trim();

            JSONObject jsonObject;
            try {
                jsonObject = JSONUtil.parseObj(jsonStr);
            } catch (Exception e) {
                System.err.println("JSON 解析失败：" + jsonStr);
                continue;
            }

            Row row = new Row();
            row.add(id);
            row.add(name);
            row.add(String.valueOf(jsonObject.get("age")));
            row.add(String.valueOf(jsonObject.get("city")));
            table.addRow(row);
        }

        return table;
    }
}
