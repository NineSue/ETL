package plugin.process;

import anno.Process;
import core.Channel;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IProcess;
import tool.Log;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Process(type = "switch")
public class SwitchCase implements IProcess {

    private String switchField;
    private boolean useContainsComparison;
    private String caseValueType;
    private String caseValueMask;
    private String caseDecimalSymbol;
    private String caseGroupingSymbol;
    private Map<String, String> rawCaseMap;
    private String defaultTarget;

    private Map<Object, String> parsedCaseMap;
    private Map<String, Channel> targetChannels = new HashMap<>();
    private Map<String, Object> pluginConfig;

    private volatile boolean headerSet = false;  // 延迟设置 Header 标志

    @Override
    public void init(Map<String, Object> cfg) {
        this.pluginConfig = cfg;
        this.switchField = (String) cfg.get("switchField");
        this.useContainsComparison = (Boolean) cfg.getOrDefault("useContainsComparison", false);
        this.caseValueType = (String) cfg.get("caseValueType");
        this.caseValueMask = (String) cfg.getOrDefault("caseValueMask", "");
        this.caseDecimalSymbol = (String) cfg.getOrDefault("caseDecimalSymbol", ".");
        this.caseGroupingSymbol = (String) cfg.getOrDefault("caseGroupingSymbol", ",");

        this.rawCaseMap = (Map<String, String>) cfg.get("caseMap");
        this.defaultTarget = (String) cfg.get("defaultTarget");

        this.parsedCaseMap = new HashMap<>();
        if (rawCaseMap != null) {
            for (Map.Entry<String, String> entry : rawCaseMap.entrySet()) {
                try {
                    Object parsedKey;
                    if (useContainsComparison) {
                        parsedKey = entry.getKey(); // 使用原始字符串
                        if (!"String".equalsIgnoreCase(caseValueType)) {
                            Log.warn("SwitchCase", "useContainsComparison 启用，但 caseValueType 不是 String，将强制按 String 处理");
                        }
                    } else {
                        parsedKey = parseValue(entry.getKey(), caseValueType, caseValueMask, caseDecimalSymbol, caseGroupingSymbol);
                    }
                    parsedCaseMap.put(parsedKey, entry.getValue());
                } catch (ParseException | IllegalArgumentException e) {
                    Log.error("SwitchCase", "解析 caseMap key '" + entry.getKey() + "' 失败: " + e.getMessage());
                }
            }
        }

        Log.info("SwitchCase", "Init with switchField: " + switchField +
                ", useContainsComparison: " + useContainsComparison +
                ", caseValueType: " + caseValueType +
                ", parsedCaseMap: " + parsedCaseMap +
                ", defaultTarget: " + defaultTarget);
    }

    @Override
    public void process(Channel input, List<Channel> outputs) throws Exception {
        prepareTargetChannels(outputs);

        // 不立即设置 Header，改为延迟，等收到首条数据时设置

        input.onReceive(rowObj -> {
            if (!(rowObj instanceof Row)) {
                Log.warn("SwitchCase", "上游数据类型非 Row，跳过");
                return;
            }

            // 延迟设置 Header，只执行一次
            if (!headerSet) {
                RowSetTable header = input.getHeader();
                if (header == null) {
                    Log.error("SwitchCase", "上游通道 Header 为空，无法设置下游 Header");
                } else {
                    for (Channel output : targetChannels.values()) {
                        output.setHeader(header);
                    }
                    Log.header("SwitchCase", String.join(", ", header.getField()));
                }
                headerSet = true;
            }

            Row row = (Row) rowObj;
            RowSetTable hdr = input.getHeader();
            int switchFieldIndex = hdr != null ? hdr.getFieldIndex(switchField) : -1;

            if (switchFieldIndex == -1) {
                Log.error("SwitchCase", "字段 '" + switchField + "' 不存在于上游 Header 中，使用默认路由");
                publishToTarget(row, defaultTarget);
                return;
            }

            Object rawSwitchValue = row.get(switchFieldIndex);
            if (rawSwitchValue == null) {
                Log.debug("SwitchCase", "字段值为空，使用默认路由");
                publishToTarget(row, defaultTarget);
                return;
            }

            Object parsedSwitchValue;
            try {
                parsedSwitchValue = parseValue(rawSwitchValue, caseValueType, caseValueMask, caseDecimalSymbol, caseGroupingSymbol);
            } catch (ParseException | IllegalArgumentException e) {
                Log.error("SwitchCase", "字段值转换失败: " + rawSwitchValue + " -> " + e.getMessage());
                publishToTarget(row, defaultTarget);
                return;
            }

            String targetStepId = null;
            if (useContainsComparison && parsedSwitchValue instanceof String) {
                String switchString = (String) parsedSwitchValue;
                for (Map.Entry<Object, String> entry : parsedCaseMap.entrySet()) {
                    if (entry.getKey() instanceof String && switchString.contains((String) entry.getKey())) {
                        targetStepId = entry.getValue();
                        break;
                    }
                }
            } else {
                targetStepId = parsedCaseMap.get(parsedSwitchValue);
            }

            publishToTarget(row, targetStepId != null ? targetStepId : defaultTarget);
        }, () -> {
            Log.info("SwitchCase", "上游通道关闭，SwitchCase 结束处理");
            targetChannels.values().forEach(Channel::close);
        });
    }

    private Object parseValue(Object rawValue, String type, String mask, String decimalSymbol, String groupingSymbol)
            throws ParseException {
        if (rawValue == null) return null;

        String stringValue = rawValue.toString();
        switch (type.toLowerCase()) {
            case "string":
                return stringValue;
            case "integer":
                if (!groupingSymbol.isEmpty()) stringValue = stringValue.replace(groupingSymbol, "");
                return Integer.parseInt(stringValue);
            case "bignumber":
            case "number":
            case "double":
            case "float":
                if (!decimalSymbol.equals(".")) stringValue = stringValue.replace(decimalSymbol, ".");
                if (!groupingSymbol.isEmpty()) stringValue = stringValue.replace(groupingSymbol, "");
                return new BigDecimal(stringValue);
            case "boolean":
                switch (stringValue.toLowerCase()) {
                    case "true": case "y": case "1": return true;
                    case "false": case "n": case "0": return false;
                    default: throw new IllegalArgumentException("无法解析布尔值: " + stringValue);
                }
            case "date":
                if (mask == null || mask.isEmpty())
                    throw new IllegalArgumentException("date 类型需要配置 caseValueMask");
                SimpleDateFormat sdf = new SimpleDateFormat(mask);
                sdf.setLenient(false);
                return sdf.parse(stringValue);
            default:
                Log.warn("SwitchCase", "未知类型 '" + type + "'，默认作为 String");
                return stringValue;
        }
    }

    private void prepareTargetChannels(List<Channel> outputs) {
        if (!targetChannels.isEmpty()) return;

        Map<String, Channel> stepIdToChannel = new HashMap<>();
        for (Channel channel : outputs) {
            String stepId = getStepIdFromChannel(channel);
            if (stepId != null) {
                stepIdToChannel.put(stepId, channel);
            } else {
                Log.warn("SwitchCase", "Channel 缺少 StepId: " + channel.getStepId());
            }
        }

        Set<String> allTargets = new HashSet<>();
        if (rawCaseMap != null) allTargets.addAll(rawCaseMap.values());
        if (defaultTarget != null) allTargets.add(defaultTarget);

        for (String targetStepId : allTargets) {
            Channel ch = stepIdToChannel.get(targetStepId);
            if (ch != null) {
                targetChannels.put(targetStepId, ch);
            } else {
                Log.error("SwitchCase", "未找到目标通道: " + targetStepId);
            }
        }
    }

    private void publishToTarget(Row row, String targetStepId) {
        Channel ch = targetChannels.get(targetStepId);
        if (ch != null) {
            ch.publish(row);
            Log.data("SwitchCase", "Row 路由至 " + targetStepId + ": " + row);
        } else {
            Log.error("SwitchCase", "目标通道不存在: " + targetStepId);
        }
    }

    private String getStepIdFromChannel(Channel channel) {
        return channel.getStepId();
    }

    @Override
    public Set<String> declareOutputTargets() {
        if (pluginConfig == null) {
            Log.error("SwitchCase", "插件未初始化，无法获取 declareOutputTargets");
            return Collections.emptySet();
        }
        Set<String> targets = new HashSet<>();
        Map<String, String> caseMap = (Map<String, String>) pluginConfig.get("caseMap");
        String def = (String) pluginConfig.get("defaultTarget");

        if (caseMap != null) targets.addAll(caseMap.values());
        if (def != null) targets.add(def);
        return targets;
    }
}
