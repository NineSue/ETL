package core;
import anno.Input;
import anno.Output;
import anno.Process;
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
    // 线程安全的HashMap，确保不会出现两个线程同时创建插件实例
    // 底层是分段锁实现

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
        for (Class<? extends Annotation> a : new Class[]{Input.class, Process.class, Output.class}) {
            ClassUtil.scanPackageByAnnotation(PKG, a).forEach(cls -> {
                try {
                    String type = (String) a.getMethod("type").invoke(cls.getAnnotation(a));
                    reg.put(type, cls);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T getPlugin(String type, Class<T> iface) {
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
