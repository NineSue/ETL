package core.intf;
import core.Channel;
import core.flowdata.Row;

import java.util.Map;

public interface IOutput {
    void init(Map<String,Object> cfg);
    void consume(Channel input) throws Exception;
}