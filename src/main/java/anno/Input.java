package anno;
import java.lang.annotation.*;

@MetaAnno(
        value = {"core.intf.IInput"}
)
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Input {
    String type();
}
