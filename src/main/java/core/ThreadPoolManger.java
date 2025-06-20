package core;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ThreadPoolManger {
    private static final ExecutorService pool = Executors.newCachedThreadPool();
    private ThreadPoolManger(){}
    
    public static ExecutorService getThreadPool() {
        return pool;
    }
}
