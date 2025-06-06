import core.Scheduler;
import org.junit.jupiter.api.Test;
import runtask.Step;
import runtask.StepList;
import java.util.Arrays;
import java.util.Collections;

public class ExcelTest {
    @Test
    public void test_excel_etl_flow() {
        // 创建读取 Excel 文件的 Step
        Step input = new Step();
        input.withStepId(1)
                .withDes("读取 Excel 文件")
                .withDomain("input")
                .withSubType("excel")
                .withConfig("filePath", "C:\\wwz.xlsx")
                .withConfig("hasHeader", true)
                .withConfig("sheetName", "Sheet1")
                .withConfig("startRow", 1)
                .withConfig("endRow", 10)
                .withConfig("columns", Arrays.asList("Column1", "Column3"))
                .withConfig("skipEmptyRows", true); // 跳过空行
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
