import core.Scheduler;
import org.junit.jupiter.api.Test;
import runtask.Step;
import runtask.StepList;
import java.util.Arrays;
import java.util.Collections;

public class DatabaseTest {
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
                .withDes("输出到数据库")
                .withDomain("output")
                .withSubType("database")
                .withParentStepId(Collections.singletonList("1"))
                .withConfig("connectionId", "1") // 数据库连接ID
                .withConfig("tableName", "people");  // 数据库表名

        StepList stepList = new StepList(Arrays.asList(input, output));

        new Scheduler(stepList).execute();
    }
}