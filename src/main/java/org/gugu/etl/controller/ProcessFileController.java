package org.gugu.etl.controller;
import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONUtil;
import org.gugu.etl.StartUp;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Controller
public class ProcessFileController {
    @GetMapping("/getAll")
    @ResponseBody
    public String getAll() {
        HashMap<Object, Object> result = new HashMap<>();
        File[] files = FileUtil.ls(StartUp.dataDir);
        List<String> filenames = new ArrayList<>();
        for (File file : files) {
            filenames.add(file.getName());
        }
        result.put("status", 200);
        result.put("message", "ok");
        result.put("files", JSONUtil.toJsonStr(filenames));
        result.put("filepath",StartUp.dataDir);
        return JSONUtil.toJsonStr(result);
    }
    
    @GetMapping("/cat_file")
    @ResponseBody
    public String catFile(@RequestParam(required = true) String fileName) {
        HashMap<Object, Object> result = new HashMap<>();
        File data = new File(StartUp.dataDir + StartUp.fileSeparator + fileName);
        System.out.println("查看"+ fileName +"文件");
        if (! FileUtil.exist(data)) {
            result.put("status", 400);
            result.put("message", "file is not exist");
            return JSONUtil.toJsonStr(result);
        }
        StringBuffer sb = new StringBuffer();
        FileUtil.readLines(data, StandardCharsets.UTF_8)
                .forEach(sb::append);
        return sb.toString();
    }
    @PostMapping("/create")
    @ResponseBody // 添加此注解，确保返回值直接写入响应体
    public String create(@RequestParam String fileName){
        HashMap<Object, Object> result = new HashMap<>();
        File data = new File(StartUp.dataDir + StartUp.fileSeparator + fileName);
        if (FileUtil.exist(data)) {
            result.put("status", 400);
            result.put("message", "file is exist");
            return JSONUtil.toJsonStr(result);
        }
        FileUtil.touch(data);
        result.put("status",200);
        result.put("message","ok");
        result.put("filepath",data.getAbsoluteFile());
        System.out.println("创建了流程文件: " + data.getAbsolutePath());
        return JSONUtil.toJsonStr(result);
    }
}
