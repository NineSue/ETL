package core.intf;

import core.Channel;
import java.util.List;
import java.util.Map;

public interface IInput {
    void init(Map<String,Object> cfg);
    void start(List<Channel> output) throws Exception;
}
