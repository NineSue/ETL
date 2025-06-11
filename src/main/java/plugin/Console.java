package plugin;

import anno.Output;
import core.flowdata.RowSetTable;
import core.intf.IOutput;
import java.util.Map;

@Output(type = "console")
public class Console implements IOutput {
    @Override
    public void deal(Object input) {
//        System.out.println("控制台输出插件接收到数据：");
        if (input instanceof Map) {
            RowSetTable rst = (RowSetTable) ((Map<?, ?>) input).get("_input");
            System.out.println(rst.toString());
        } else {
            System.err.println("无效的输入类型");
        }
    }
}

