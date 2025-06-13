package plugin.input;

import anno.Input;
import cn.hutool.core.io.FileUtil;
import core.Channel;
import core.flowdata.Row;
import core.intf.IInput;
import lombok.var;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Input(type = "csv") // 与Factory扫描的注解类型一致
public class CsvInput implements IInput {
    private Map<String, Object> config; // 保存初始化配置
    private char delimiter = ',';       // 默认分隔符
    private char quoteChar = '"';       // 默认引号字符
    private boolean hasHeader = true;   // 默认有表头
    private String filePath;

    @Override
    public void init(Map<String, Object> cfg) {
        // 初始化配置并校验
        this.config = cfg;
        this.filePath = (String) cfg.get("filePath");
        String delimiterStr = (String) cfg.getOrDefault("delimiter", ",");
        String quoteStr = (String) cfg.getOrDefault("quoteChar", "\"");
        this.hasHeader = (Boolean) cfg.getOrDefault("hasHeader", true);

        // 参数校验
        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("缺少文件路径！");
        }
        if (delimiterStr.length() != 1) {
            throw new IllegalArgumentException("分隔符必须是一个字符");
        }
        if (quoteStr.length() != 1) {
            throw new IllegalArgumentException("引号字符必须是一个字符");
        }
        this.delimiter = delimiterStr.charAt(0);
        this.quoteChar = quoteStr.charAt(0);
    }

    @Override
    public void start(Channel<Row> output) throws Exception {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IllegalArgumentException("文件不存在: " + filePath);
        }

        // 读取文件行
        var lines = FileUtil.readLines(file, StandardCharsets.UTF_8);
        if (lines.isEmpty()) {
            throw new IllegalArgumentException("CSV文件为空");
        }

        // 处理表头（跳过表头行）
        int dataStart = hasHeader ? 1 : 0;

        // 逐行解析并发布到通道
        for (int i = dataStart; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty()) continue;

            Row row = parseCsvLine(line);
            output.publish(row); // 通过通道发布数据
        }
        output.close(); // 数据发送完毕后关闭通道
    }

    // 解析单行CSV数据（简化逻辑，保留核心功能）
    private Row parseCsvLine(String line) {
        Row row = new Row();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == quoteChar) {
                if (i + 1 < line.length() && line.charAt(i + 1) == quoteChar) {
                    current.append(quoteChar);
                    i++; // 跳过下一个引号
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == delimiter && !inQuotes) {
                row.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        row.add(current.toString()); // 添加最后一个字段

        if (inQuotes) {
            throw new IllegalArgumentException("CSV行包含未闭合的引号: " + line);
        }
//        System.out.println(row);
        return row;
    }
}
