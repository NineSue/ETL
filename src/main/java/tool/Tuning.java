package tool;


public class Tuning {

    static final int CPU = Runtime.getRuntime().availableProcessors();
    static final long MEM_MB = Runtime.getRuntime().maxMemory() / 1024 / 1024;

    public static int threadPoolSize() {
        return Integer.getInteger("etl.threadPoolSize", CPU * 2);
    }

    public static int queueSize() {
        return Integer.getInteger("etl.queueSize", threadPoolSize() * 100);
    }

    public static int objectPoolSize() {
        int defaultSize = threadPoolSize() * 4;
        if (MEM_MB < 2048) {
            defaultSize = Math.max(16, defaultSize / 2);
        }
        return Integer.getInteger("etl.objectPoolSize", defaultSize);
    }

    public static boolean enableLog() {
        return Boolean.parseBoolean(System.getProperty("etl.enableLog", "true"));
    }

    public static int logLevel() {
        return Integer.getInteger("etl.logLevel", 3);
    }

    public static boolean enableKaomoji() {
        return Boolean.parseBoolean(System.getProperty("etl.enableKaomoji", "true"));
    }

    public static void init() {
        Log.LOG_LEVEL = enableLog() ? logLevel() : 0;
        Log.ENABLE_KAOMOJI = enableKaomoji();
    }

    public static void print() {
        Log.info("Tuning", "System tuning initialized");
        Log.info("Tuning", "CPU cores: " + CPU);
        Log.info("Tuning", "Max memory: " + MEM_MB + " MB");
        Log.info("Tuning", "Thread pool size: " + threadPoolSize());
        Log.info("Tuning", "Queue capacity: " + queueSize());
        Log.info("Tuning", "Object pool size: " + objectPoolSize());
        Log.info("Tuning", "Log enabled: " + enableLog());
        Log.info("Tuning", "Log level: " + logLevel());
        Log.info("Tuning", "Kaomoji enabled: " + enableKaomoji());
    }
}
