package org.gugu.etl.SwitchCase;

import core.Scheduler;
import runtask.Step;
import runtask.StepList;
import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SwitchCaseTest {

    /**
     * 测试SwitchCase插件根据字段值进行数据路由
     * - 输入CSV包含 'category' 字段
     * - SwitchCase根据 'category' 字段的值路由数据：
     *   - 'Electronics' 路由到 Output_Electronics (Step 3)
     *   - 'Books' 路由到 Output_Books (Step 4)
     *   - 其他值路由到 Output_Default (Step 5)
     */
    @Test
    public void test_switchCaseRouting() throws InterruptedException {
        // 1. CSV 输入步骤：提供测试数据
        Step csvInput = new Step()
                .withStepId(1)
                .withDes("CSV输入：测试数据")
                .withDomain("input")
                .withSubType("csv")
                .withConfig("filePath", "src/test/java/org/gugu/etl/SwitchCase/testData.csv") // 假设存在此文件
                .withConfig("hasHeader", true);

        // 2. SwitchCase 处理步骤：配置路由逻辑
        Map<String, String> caseMap = new HashMap<>();
        caseMap.put("Electronics", "3"); // 如果 category 是 'Electronics'，路由到 Step 3
        caseMap.put("Books", "4");       // 如果 category 是 'Books'，路由到 Step 4

        Step switchCaseProcess = new Step()
                .withStepId(2)
                .withDes("SwitchCase路由")
                .withDomain("process")
                .withSubType("switch") // 对应 SwitchCase 插件的 @Process(type = "switch")
                .withParentStepId(Collections.singletonList("1")) // 上游是 CSV 输入
                .withConfig("switchField", "category") // 根据 'category' 字段进行判断
                .withConfig("caseValueType", "String") // 比较值的类型是字符串
                .withConfig("useContainsComparison", false) // 精确匹配
                .withConfig("caseMap", caseMap) // 路由规则
                .withConfig("defaultTarget", "5"); // 默认路由到 Step 5

        // 3. 输出步骤 1：用于接收 'Electronics' 类型的数据
        Step outputElectronics = new Step()
                .withStepId(3)
                .withDes("输出到控制台：Electronics")
                .withDomain("output")
                .withSubType("console")
                // 注意：这里仍然需要声明父步骤，即使 SwitchCase 内部声明了目标。
                // Scheduler 会根据 SwitchCase 的 declareOutputTargets 确保连接。
                .withParentStepId(Collections.singletonList("2"));

        // 4. 输出步骤 2：用于接收 'Books' 类型的数据
        Step outputBooks = new Step()
                .withStepId(4)
                .withDes("输出到控制台：Books")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("2"));

        // 5. 输出步骤 3：用于接收默认（不匹配任何case）的数据
        Step outputDefault = new Step()
                .withStepId(5)
                .withDes("输出到控制台：Default")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("2"));

        // 构建步骤列表并执行调度器
        StepList stepList = new StepList(Arrays.asList(csvInput, switchCaseProcess, outputElectronics, outputBooks, outputDefault));
        new Scheduler(stepList).execute();
    }

    /**
     * 测试SwitchCase插件使用 'contains' 比较模式
     * - 输入CSV包含 'description' 字段
     * - SwitchCase根据 'description' 字段是否包含特定子字符串路由数据：
     *   - 包含 'laptop' 路由到 Output_Laptop (Step 3)
     *   - 包含 'book' 路由到 Output_Book (Step 4)
     *   - 其他路由到 Output_Default (Step 5)
     */
    @Test
    public void test_switchCaseContainsRouting() throws InterruptedException {
        // 1. CSV 输入步骤：提供测试数据
        Step csvInput = new Step()
                .withStepId(1)
                .withDes("CSV输入：测试数据 (Contains)")
                .withDomain("input")
                .withSubType("csv")
                .withConfig("filePath", "src/test/java/org/gugu/etl/SwitchCase/testDataContains.csvok") // 假设存在此文件
                .withConfig("hasHeader", true);

        // 2. SwitchCase 处理步骤：配置路由逻辑
        Map<String, String> caseMap = new HashMap<>();
        caseMap.put("laptop", "3"); // 如果 description 包含 'laptop'，路由到 Step 3
        caseMap.put("book", "4");   // 如果 description 包含 'book'，路由到 Step 4

        Step switchCaseProcess = new Step()
                .withStepId(2)
                .withDes("SwitchCase路由 (Contains)")
                .withDomain("process")
                .withSubType("switch")
                .withParentStepId(Collections.singletonList("1"))
                .withConfig("switchField", "description") // 根据 'description' 字段进行判断
                .withConfig("caseValueType", "String") // 比较值的类型是字符串
                .withConfig("useContainsComparison", true) // 使用包含比较
                .withConfig("caseMap", caseMap)
                .withConfig("defaultTarget", "5");

        // 3. 输出步骤 1：用于接收包含 'laptop' 的数据
        Step outputLaptop = new Step()
                .withStepId(3)
                .withDes("输出到控制台：Laptop")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("2"));

        // 4. 输出步骤 2：用于接收包含 'book' 的数据
        Step outputBook = new Step()
                .withStepId(4)
                .withDes("输出到控制台：Book")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("2"));

        // 5. 输出步骤 3：用于接收默认（不匹配任何case）的数据
        Step outputDefault = new Step()
                .withStepId(5)
                .withDes("输出到控制台：Default (Contains)")
                .withDomain("output")
                .withSubType("console")
                .withParentStepId(Collections.singletonList("2"));

        // 构建步骤列表并执行调度器
        StepList stepList = new StepList(Arrays.asList(csvInput, switchCaseProcess, outputLaptop, outputBook, outputDefault));
        new Scheduler(stepList).execute();
    }
}