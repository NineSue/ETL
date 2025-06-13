package core;

import anno.Input;
import anno.Output;
import cn.hutool.core.util.ClassUtil;

import java.lang.annotation.Annotation;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Factory {
    private static final String PKG = "plugin";
    private final Map<String, Class<?>> reg = new ConcurrentHashMap<>();
    private final Map<String, Object> pluginCache = new ConcurrentHashMap<>();
    //线程安全的HashMap，确保不会出现俩个线程判断无缓存同时创建一个插件实例的情况(●´ω｀●)
    //底层是分段锁的实现，细分一下情况，有补充请私me
    // 查缓存 -> 有缓存返回
    // 无缓存 -> 加锁 -> 再查缓存 -> 有缓存返回
    // 无缓存 -> 创建实例 -> 放缓存 -> 解锁 -> 返回实例

    public Factory() {
        init();
    }

    /*
        遍历所有标注插件的注解类，
        提取注解里的 type 值，
        建立 “子类型” 与 “插件类” 的映射且注册进工厂
    */
    @SuppressWarnings("unchecked")
    private void init() {
        for(Class<? extends Annotation> a:new Class[]{Input.class, Process.class, Output.class}){
            ClassUtil.scanPackageByAnnotation(PKG,a).forEach(cls->{
                try{
                    String type= (String) a.getMethod("type").invoke(cls.getAnnotation(a));
                    reg.put(type,cls);
                }catch (Exception e){
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getPlugin(String type,Class iface) {
        Object inst = pluginCache.computeIfAbsent(type, t -> {
            try {
                return reg.get(type).getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        return (T) inst;
    }
}
