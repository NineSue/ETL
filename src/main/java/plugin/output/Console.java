package plugin.output;

import anno.Output;
import core.Channel;
import core.flowdata.Row;
import core.intf.IOutput;

import java.util.Map;

@Output(type = "console")
public class Console implements IOutput {
    private Map<String, Object> config;

    @Override
    public void init(Map<String, Object> cfg) {
        this.config = cfg;
    }

    @Override
    public void consume(Channel<Row> input) throws Exception {
        // 订阅通道数据并输出
        input.subscribe(row -> {
            System.out.println(row);
        });
    }
}
