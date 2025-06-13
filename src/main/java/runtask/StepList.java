package runtask;

import cn.hutool.json.JSONConfig;
import cn.hutool.json.JSONUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;

public class StepList {
    List<Step> data = new ArrayList<>();
    
    
    public void addStep(Step step){
        if (this.data.contains(step)){
            return;
        }
        this.data.add(step);
    }
    
    public boolean rmStep(String stepId) {
        Step step = new Step();
        step.stepId = Integer.parseInt(stepId);
        return data.remove(step);
    }
    
    @Override
    public String toString() {
        HashMap<Object,Object> map = new HashMap<>();
        map.put("data",data);
        return JSONUtil.toJsonStr(map);
    }
    
    public boolean updateStep(Step step){
        boolean removed = this.data.remove(step);
        if (!removed) {
            return false;
        }
        this.data.add(step);
        return true;
    }
    
    public List<Step> getData() {
        return data;
    }
    
    public void setData(List<Step> data) {
        this.data = data;
    }
    
    public StepList(List<Step> data) {
        this.data = data;
    }
    
    public StepList() {
    }
}
