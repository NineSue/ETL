package core;
import anno.MetaAnno;
import org.reflections.Reflections;

import java.lang.annotation.Annotation;
import java.util.HashSet;
import java.util.Set;

public class Checker {

    public static void run(String... seekPackage) {
        Reflections reflections = new Reflections(seekPackage);

        Set<Class<?>> all = reflections.getTypesAnnotatedWith(MetaAnno.class);
        Set<Class<? extends Annotation>> metaAnnotatedAnnotations = new HashSet<>();

        for (Class<?> clazz : all) {
            if (Annotation.class.isAssignableFrom(clazz)) {
                @SuppressWarnings("unchecked")
                Class<? extends Annotation> annoClass = (Class<? extends Annotation>) clazz;
                metaAnnotatedAnnotations.add(annoClass);
            }
        }

        boolean hasError = false; // 错误标志

        for (Class<? extends Annotation> annoClass : metaAnnotatedAnnotations) {
            MetaAnno metaAnno = annoClass.getAnnotation(MetaAnno.class);
            String[] requiredInterfaces = metaAnno.value();

            Set<Class<?>> targetClasses = reflections.getTypesAnnotatedWith(annoClass);

            for (Class<?> target : targetClasses) {
                boolean matched = implementsAny(target, requiredInterfaces);

                if (!matched) {
                    hasError = true; // 出现错误
                    String errorMsg = metaAnno.errorTemplate()
                            .replace("%annotation%", annoClass.getSimpleName())
                            .replace("%class%", target.getName());
                    System.err.println(errorMsg+" (┙>∧<)┙へ┻┻");
                }
            }
        }

        if (hasError) {
            System.exit(1); // 有错误时退出程序
        } else {
            final String ANSI_RESET = "\u001B[0m";
            final String ANSI_GREEN = "\u001B[32m";
            System.out.println(ANSI_GREEN + "完美！所有插件实现都符合接口规范！ദ്ദി˶ｰ̀֊ｰ́ )" + ANSI_RESET);
        }
    }

    private static boolean implementsAny(Class<?> clazz, String[] interfaceNames) {
        for (Class<?> iface : clazz.getInterfaces()) {
            for (String name : interfaceNames) {
                if (iface.getName().equals(name)) return true;
            }
        }

        Class<?> superClass = clazz.getSuperclass();
        if (superClass != null && superClass != Object.class) {
            return implementsAny(superClass, interfaceNames);
        }

        return false;
    }
}
