package core.intf;
import core.Channel;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
public interface IProcess {
    void init(Map<String, Object> cfg);

    void process(Channel input, List<Channel> outputs) throws Exception;

    /**
     * 明确声明此步骤显式的下游步骤ID，用于补充管道拓扑。
     * Scheduler 初始化后调用，非必需，默认空集。
     *
     * @return 该步骤显式输出目标步骤ID集合，默认无
     */
    default Set<String> declareOutputTargets() {
        return Collections.emptySet();
    }
}
