import core.Scheduler;
import org.junit.jupiter.api.Test;
import runtask.Step;
import runtask.StepList;
import java.util.Arrays;
import java.util.Collections;



public class PleaseTestMe{
    @Test
    public void test_csv_etl_flow() {
        Step input = new Step();
        input.withStepId(1)
                .withDes("读取csv文件")
                .withDomain("input")
                .withSubType("csv")
                .withConfig("filePath", "src/test/java/gugugu.csv")
                .withConfig("delimiter", ",")
                .withConfig("quoteChar", "\"")
                .withConfig("hasHeader", true);

        Step output = new Step();
        output.withStepId(2)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(input, output));

        new Scheduler(stepList).execute();
    }
    @Test
    public void test_http_etl_flow() {
        Step input = new Step();
        input.withStepId(1)
                .withDes("从接口读取数据")
                .withDomain("input")
                .withSubType("http")
                .withConfig("url", "http://localhost:3000/api/query/preview")
                .withConfig("method", "POST")
                .withConfig("body", "{\"connectionId\":3,\"sql\":\"SELECT id, name, age, city FROM users;\"}")
                .withConfig("headers", Collections.singletonMap("Content-Type", "application/json"));

        Step output = new Step();
        output.withStepId(2)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(input, output));
        new Scheduler(stepList).execute();
    }

    @Test
    public void test_json_etl_flow() {
        Step input = new Step();
        input.withStepId(1)
                .withDes("读取包含json字段的文本")
                .withDomain("input")
                .withSubType("jsontext")
                .withConfig("sourceType", "file")
                .withConfig("filePath", "src/test/java/yh_json.txt");

        Step output = new Step();
        output.withStepId(2)
                .withDes("输出到控制台")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("1"));

        StepList stepList = new StepList(Arrays.asList(input, output));

        new Scheduler(stepList).execute();
    }
}
