package plugin;

import anno.Output;
import core.intf.IOutput;

@Output(type = "console")
public class Console implements IOutput {
    @Override
    public void deal(Object config) {
//        System.out.println("控制台输出插件接收到数据：");
        System.out.println();
        System.out.println(config.toString());
    }
}
