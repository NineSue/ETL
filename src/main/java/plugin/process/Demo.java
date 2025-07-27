package plugin.process;

import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IProcess;
import core.Channel;
import anno.Process;
import tool.Log;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Process(type = "demo")
public class Demo implements IProcess {
    private String ageColumnName = "age";
    private int ageColumnIndex = -1;
    private volatile boolean headerProcessed = false;

    @Override
    public void init(Map<String, Object> cfg) {
        if (cfg.containsKey("ageColumn")) {
            this.ageColumnName = (String) cfg.get("ageColumn");
        }
        Log.info("Demo", "Initialized with age column: " + ageColumnName);
    }

    @Override
    public void process(Channel input, List<Channel> outputs) throws Exception {
        Log.info("Demo", "Processing data...");
        if (outputs == null || outputs.size() < 2) {
            Log.error("Demo", "Requires at least 2 output channels.");
            throw new IllegalStateException("Demo需要至少2个输出通道");
        }

        input.onReceive(data -> {
            if (!headerProcessed) {
                RowSetTable table = input.getHeader();
                if (table == null) {
                    Log.warn("Demo", "No header received yet.");
                    return;
                }
                this.ageColumnIndex = table.getField().indexOf(ageColumnName);
                if (ageColumnIndex == -1) {
                    Log.error("Demo", "Age field '" + ageColumnName + "' not found in header.");
                    throw new IllegalStateException("未找到 age 字段: " + ageColumnName);
                }
                for (Channel out : outputs) {
                    out.setHeader(table);
                }
                headerProcessed = true;
                Log.info("Demo", "Header processed, ageIndex=" + ageColumnIndex);
            }

            Row row = (Row) data;
            try {
                if (ageColumnIndex >= 0 && ageColumnIndex < row.size()) {
                    Object ageValue = row.get(ageColumnIndex);
                    if (ageValue == null) {
                        Log.warn("Demo", "Age value is null for row: " + row.toString() + ". Skipping.");
                        return;
                    }
                    int age = Integer.parseInt(ageValue.toString().trim());
                    if (age > 28) {
                        outputs.get(0).publish(row.copy());
                        Log.data("Demo", "Routed to output 0 (age > 28): " + row.toString());
                    } else {
                        outputs.get(1).publish(row.copy());
                        Log.data("Demo", "Routed to output 1 (age <= 28): " + row.toString());
                    }
                } else {
                    Log.warn("Demo", "Age index " + ageColumnIndex + " out of bounds for row size " + row.size() + ". Skipping.");
                }
            } catch (NumberFormatException e) {
                Log.error("Demo", "Error parsing age '" + row.get(ageColumnIndex) + "': " + e.getMessage() + ". Skipping.");
            } catch (Exception e) {
                Log.error("Demo", "Processing exception: " + e.getMessage());
            }
        }, () -> {
            Log.info("Demo", "Upstream finished, closing outputs.");
            for (Channel out : outputs) {
                out.close();
            }
        });
    }

    @Override
    public Set<String> declareOutputTargets() {
        return Collections.emptySet();
    }
}