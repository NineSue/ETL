package core;

import anno.Input;
import anno.Output;
import cn.hutool.core.util.ClassUtil;


import java.lang.annotation.Annotation;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Factory {
    private static final String PKG = "plugin";
    private final Map<String, Class<?>> reg = new HashMap<>();
    private final Map<String, Object> pluginCache = new ConcurrentHashMap<>();
    //线程安全的HashMap，确保不会出现俩个线程判断无缓存同时创建一个插件实例的情况(●´ω｀●)
    //底层是分段锁的实现，细分一下情况，有补充请私晓苏理
    // 查缓存 -> 有缓存返回
    // 无缓存 -> 加锁 -> 再查缓存 -> 有缓存返回
    // 无缓存 -> 创建实例 -> 放缓存 -> 解锁 -> 返回实例

    public Factory() {
        init();
    }
    @SuppressWarnings("unchecked")
    private void init() {
        Class<? extends Annotation>[] annos = new Class[]{
                Input.class,
                Output.class,
                Process.class
        };
        for (Class<? extends Annotation> anno : annos) {
            scan(anno);
        }
    }

    private void scan(Class<? extends Annotation> anno) {
        Set<Class<?>> classes = ClassUtil.scanPackageByAnnotation(PKG, anno);
        for (Class<?> cls : classes) {
            Annotation tag = cls.getAnnotation(anno);
            try {
                String type = (String) anno.getMethod("type").invoke(tag);
                reg.put(type, cls);
            } catch (Exception e) {
                System.out.println("错,获取注解类方法有误");
            }
        }
    }

    public Object runPlugin(String type, Object... data) {
        Class<?> cls = reg.get(type);
        if (cls == null) {
            System.out.println("错，未找到插件类型: " + type);
            return null;
        }

        Object plugin = pluginCache.computeIfAbsent(type, t -> {
            try {
                return cls.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                System.out.println("错，反射方法有误"+cls.getName());
                return null;
            }
        });

        if (plugin == null) return null;

        try {
            return plugin.getClass().getMethod("deal", Object.class).invoke(plugin, data);
        } catch (Exception e) {
            System.out.println("错，反射方法有误"+cls.getName());
            return null;
        }
    }

}
