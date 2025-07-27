package runtask;

import core.flowdata.RowSetTable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Step {
    protected Integer stepId;
    protected String description;
    protected String subType;//子类型
    protected HashMap<String, Object> config = new HashMap<>();//具体配置
    protected String domain;//主类型
    protected List<String> parentStepId = new ArrayList<>();//上游ID
    protected RowSetTable subData;
    
    public void setStepId(Integer stepId) {
        this.stepId = stepId;
    }
    
    public String getDescription() {
        return description;
    }
    
    public void setDescription(String description) {
        this.description = description;
    }
    
    public void setSubType(String subType) {
        this.subType = subType;
    }
    
    public void setConfig(HashMap<String, Object> config) {
        this.config = config;
    }
    
    public void setDomain(String domain) {
        this.domain = domain;
    }
    
    public void setParentStepId(List<String> parentStepId) {
        this.parentStepId = parentStepId;
    }
    
    public Step(Integer stepId, String description, String subType, String domain, List<String> parentStepId, List<String> childStepId, HashMap<String, Object> config) {
        this.stepId = stepId;
        this.description = description;
        this.subType = subType;
        this.domain = domain;
        this.parentStepId = parentStepId;
        this.config = config;
    }
    
    public RowSetTable getSubData() {
        return subData;
    }
    
    public void setSubData(RowSetTable subData) {
        this.subData = subData;
    }
    
    public Step() {
    }
    
    public Integer getStepId() {
        return stepId;
    }
    
    public Step withStepId(Integer stepId) {
        this.stepId = stepId;
        return this;
    }
    
    public String getDes() {
        return description;
    }
    
    public Step withDes(String description) {
        this.description = description;
        return this;
    }
    
    public String getSubType() {
        return subType;
    }
    
    public Step withSubType(String type) {
        this.subType = type;
        return this;
    }
    
    public String getDomain() {
        return domain;
    }
    
    public Step withDomain(String domain) {
        this.domain = domain;
        return this;
    }
    
    public List<String> getParentStepId() {
        return parentStepId;
    }
    
    public Step withParentStepId(List<String> parentStepId) {
        this.parentStepId = parentStepId;
        return this;
    }
    
    public HashMap<String, Object> getConfig() {
        return config;
    }
    
    public Step withConfig(String key, Object value) {
        this.config.put(key, value);
        return this;
    }
    
    public Step withConfig(HashMap<String, Object> config) {
        this.config = config != null ? config : new HashMap<>();
        return this;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        
        Step other = (Step) obj;
        
        return stepId != null && stepId.equals(other.stepId);
    }
    
}
