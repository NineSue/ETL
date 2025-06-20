//import cn.hutool.core.io.FileUtil;
//import cn.hutool.json.JSONUtil;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import core.Scheduler;
//import org.gugu.etl.StartUp;
//import org.junit.jupiter.api.Test;
//import runtask.Step;
//import runtask.StepList;
//
//import java.io.File;
//import java.nio.charset.StandardCharsets;
//import java.util.Arrays;
//import java.util.Collections;
//
//
//
//public class PleaseTestMe {
//
//    @Test
//    public void test_csv_etl_flow() {
//        Step input = new Step();
//        input.withStepId(1)
//                .withDes("读取csv文件")
//                .withDomain("input")
//                .withSubType("csv")
//                .withConfig("filePath", "src/test/java/gugugu.csv")
//                .withConfig("delimiter", ",")
//                .withConfig("quoteChar", "\"")
//                .withConfig("hasHeader", true);
//
//        Step output = new Step();
//        output.withStepId(2)
//                .withDes("输出到控制台")
//                .withDomain("output")
//                .withSubType("console")
//                .withParentStepId(Collections.singletonList("1"));
//
//        StepList stepList = new StepList(Arrays.asList(input, output));
//
//        new Scheduler(stepList).execute();
//    }
//
//
//
//    @Test
//    public void test_http_etl_flow() {
//        Step input = new Step();
//        input.withStepId(1)
//                .withDes("从接口读取数据")
//                .withDomain("input")
//                .withSubType("http")
//                .withConfig("url", "http://localhost:3000/api/query/preview")
//                .withConfig("method", "POST")
//                .withConfig("body", "{\"connectionId\":3,\"sql\":\"SELECT id, name, age, city FROM users;\"}")
//                .withConfig("headers", Collections.singletonMap("Content-Type", "application/json"));
//
//        Step output = new Step();
//        output.withStepId(2)
//                .withDes("输出到控制台")
//                .withDomain("output")
//                .withSubType("console")
//                .withParentStepId(Collections.singletonList("1"));
//
//        StepList stepList = new StepList(Arrays.asList(input, output));
//        new Scheduler(stepList).execute();
//    }
//
//    @Test
//    public void test_json_etl_flow() {
//
//        // file 模式：从文件中读取数据
//        Step inputFile = new Step();
//        inputFile.withStepId(1)
//                .withDes("从文件读取包含 json 字段的文本")
//                .withDomain("input")
//                .withSubType("jsontext")
//                .withConfig("mode", "file")
//                .withConfig("filePath", "src/test/java/yh_json.txt");
//
//        // field 模式：从字段直接读取数据
//        Step inputField = new Step();
//        inputField.withStepId(1)
//                .withDes("从字段读取包含 json 字段的文本")
//                .withDomain("input")
//                .withSubType("jsontext")
//                .withConfig("mode", "field")
//                .withConfig("sourceField", "id,name,json_data\n" +
//                        "1,Alice,{\"age\":30,\"city\":\"New York\"}\n" +
//                        "2,Bob,{\"age\":25,\"city\":\"Los Angeles\"}\n" +
//                        "3,Charlie,{\"age\":35,\"city\":\"Chicago\"}")
//                .withConfig("jsonField","json_data")
//                .withConfig("delimiter", ",");
//
//        // url 模式（未实现）
//        Step inputUrl = new Step();
//        inputUrl.withStepId(1)
//                .withDes("从 URL 拉取数据")
//                .withDomain("input")
//                .withSubType("jsontext")
//                .withConfig("mode", "url")
//                .withConfig("url", "");
//
//
//        Step input = new Step();
//        input.withStepId(1)
//                .withDes("读取包含json字段的文本")
//                .withDomain("input")
//                .withSubType("jsontext")
//                .withConfig("sourceType", "file")
//                .withConfig("filePath", "src/test/java/yh_json.txt");
//
//
//        Step output = new Step();
//        output.withStepId(2)
//                .withDes("输出到控制台")
//                .withDomain("output")
//                .withSubType("console")
//                .withParentStepId(Collections.singletonList("1"));
//
//
//        //file 模式
//        StepList stepList = new StepList(Arrays.asList(inputFile, output));
//        // field 模式
//        //StepList stepList = new StepList(Arrays.asList(inputField, output));
//        // 未来支持 url 模式
//        //StepList stepList = new StepList(Arrays.asList(inputUrl, output));
//        new Scheduler(stepList).execute();
//    }
//
//    @Test
//    public void testStepList() {
//        Step input = new Step();
//        input.withStepId(1)
//             .withDes("读取csv文件")
//             .withDomain("input")
//             .withSubType("csv")
//             .withConfig("filePath", "src/test/java/gugugu.csv")
//             .withConfig("delimiter", ",")
//             .withConfig("quoteChar", "\"")
//             .withConfig("hasHeader", true);
//
//        Step output = new Step();
//        output.withStepId(2)
//              .withDes("输出到控制台")
//              .withDomain("output")
//              .withSubType("console")
//              .withParentStepId(Collections.singletonList("1"));
//
//        Step output_1 = new Step();
//        output_1.withStepId(2)
//                .withDes("yYy")
//                .withDomain("output")
//                .withSubType("xxx")
//                .withParentStepId(Collections.singletonList("1"));
//        StepList sl = new StepList();
//        sl.addStep(input);
//        sl.addStep(output);
//        sl.updateStep(output_1);
//        sl.rmStep(input.getStepId()
//                       .toString());
//        System.out.println(sl);
//    }
//
//    @Test
//    public void test_JSONToBean() throws JsonProcessingException {
//        File data = new File("C:\\Users\\白乃常\\.etl\\process_files\\a001.json");
//        StringBuffer sb = new StringBuffer();
//        FileUtil.readLines(data, StandardCharsets.UTF_8)
//                .forEach(sb::append);
//        String json = sb.toString();
//        ObjectMapper mapper = new ObjectMapper();
//        StepList response = mapper.readValue(json, StepList.class);
//        System.out.println(response);
//    }
//}
//
//
