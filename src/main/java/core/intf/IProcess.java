package core.intf;

import core.Channel;
import core.flowdata.Row;

import java.util.Map;

public interface IProcess {
    void init(Map<String,Object> cfg);
    void process(Channel<Row> input,Channel<Row> output) throws Exception;
}