package org.gugu.etl.controller;

import cn.hutool.core.io.FileUtil;
import cn.hutool.json.JSONUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.gugu.etl.StartUp;
import org.springframework.web.bind.annotation.ResponseBody;
import runtask.Step;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import runtask.StepList;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

@Controller
public class StepController {
    
    private static void flushFile(StepList sl, String fileName) {
        FileUtil.writeBytes(sl.toString()
                              .getBytes(StandardCharsets.UTF_8), StartUp.getPathString(fileName));
    }
    
    @PostMapping("/add_step")
    @ResponseBody
    public String addStep(@RequestBody Step step, @RequestParam String fileName) {
        HashMap<Object, Object> result = new HashMap<>();
        StepList sl = getStepList(fileName);
        Set<Integer> stepIds = new HashSet<>();
        for (Step s : sl.getData()) {
            stepIds.add(s.getStepId());
        }
        if (stepIds.contains(step.getStepId())){
            result.put("status", "400");
            result.put("message", "step id is exist");
            return JSONUtil.toJsonStr(result);
        }
        sl.addStep(step);
        flushFile(sl, fileName);
        result.put("status", "200");
        result.put("message", "ok");
        return JSONUtil.toJsonStr(result);
    }
    
    @PostMapping("/rm_step")
    @ResponseBody
    public String rmStep(@RequestParam String fileName, @RequestParam String stepId) {
        StepList sl = getStepList(fileName);
        boolean b = sl.rmStep(stepId);
        HashMap<Object, Object> result = new HashMap<>();
        if (b) {
            flushFile(sl, fileName);
            result.put("status", "200");
            result.put("message", "ok");
        } else {
            result.put("status", "400");
            result.put("message", "step remove fail");
        }
        return JSONUtil.toJsonStr(result);
    }
    
    @PostMapping("/update_step")
    @ResponseBody
    public String updateStep(@RequestBody Step step, @RequestParam String fileName) {
        HashMap<Object, Object> result = new HashMap<>();
        StepList sl = getStepList(fileName);
        boolean b = sl.updateStep(step);
        if (b) {
            flushFile(sl, fileName);
            result.put("status", "200");
            result.put("message", "ok");
        } else {
            result.put("status", "400");
            result.put("message", "step remove fail");
        }
        return JSONUtil.toJsonStr(result);
    }
    
    private static StepList getStepList(String fileName) {
        File data = new File(StartUp.getPathString(fileName));
        StringBuffer sb = new StringBuffer();
        FileUtil.readLines(data, StandardCharsets.UTF_8)
                .forEach(sb::append);
        String json = sb.toString();
        if (json.isEmpty()) {
            return new StepList();
        }
        ObjectMapper mapper = new ObjectMapper();
        try {
            return mapper.readValue(json, StepList.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
