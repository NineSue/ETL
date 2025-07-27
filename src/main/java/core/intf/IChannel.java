package core.intf;
import core.flowdata.RowSetTable;

import java.util.function.Consumer;

public interface IChannel<T>{
    void publish(T row) throws InterruptedException;
    void subscribe(Consumer<T> consumer);
    void close();
    boolean isClosed();
    default void onReceive(Consumer<T> handler,Runnable onComplete){
        throw new UnsupportedOperationException("Option");
    }
    RowSetTable getHeader();
    void setHeader(RowSetTable table);
}
