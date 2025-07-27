package anno;
import java.lang.annotation.*;

@Target(ElementType.ANNOTATION_TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface MetaAnno {
    String[] value();
    String successTemplate() default "校验通过，类 %class% 合规";
    String errorTemplate() default "校验失败，类 %class% 未实现要求接口";
}
