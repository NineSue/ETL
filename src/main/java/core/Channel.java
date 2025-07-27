package core;

import core.flowdata.RowSetTable;
import core.intf.IChannel;
import tool.Log;
import tool.Tuning;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;


public class Channel implements IChannel<Object> {

    private RowSetTable header = null; // 仅存 header

    private final BlockingQueue<Object> queue = new LinkedBlockingQueue<>(Tuning.queueSize());
    // 阻塞队列实现背压，上游 put() 可能阻塞

    private volatile boolean closed = false; // 发布与消费依赖该标志，防止并发写入

    private final ExecutorService pool; // 注入线程池，避免每个通道创建线程
    private final String channelId;     // 日志标识
    private final String stepId;        // 所属步骤 ID，用于调试与插件映射

    public Channel(ExecutorService pool, String stepId) {
        this.pool = pool;
        this.stepId = stepId;
        this.channelId = Integer.toHexString(this.hashCode()).substring(0, 6);
        Log.info("Channel-" + channelId + " (Step: " + stepId + ")", "Created.");
    }

    public String getStepId() {
        return stepId;
    }

    @Override
    public RowSetTable getHeader() {
        return header;
    }

    @Override
    public void setHeader(RowSetTable header) {
        this.header = header;
        Log.info("Channel-" + channelId + " (Step: " + stepId + ")", "Set header: " + header);
    }

    @Override
    public void publish(Object row) {
        if (closed) {
            Log.warn("Channel-" + channelId + " (Step: " + stepId + ")", "Closed, drop data");
            return;
        }
        try {
            queue.put(row); // 有界队列，自动阻塞，背压点
            int size = queue.size();
            if (size % 5000 == 0) {
                Log.info("Channel-" + channelId + " (Step: " + stepId + ")", "Queue size: " + size);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // 标记中断
            throw new RuntimeException("Data publish interrupted", e);
        }
    }

    public Object poll(long timeoutMs) throws InterruptedException {
        return queue.poll(timeoutMs, TimeUnit.MILLISECONDS); // 支持带超时的消费
    }

    public boolean isEmpty() {
        return queue.isEmpty(); // 提供队列剩余判断
    }

    @Override
    public void onReceive(Consumer<Object> handler, Runnable onDone) {
        // 启动单线程消费循环，由线程池管理生命周期
        pool.submit(() -> runConsumerLoop(handler, onDone));
    }

    private void runConsumerLoop(Consumer<Object> handler, Runnable onDone) {
        AtomicInteger consumed = new AtomicInteger(0);
        try {
            while (!closed || !queue.isEmpty()) {
                Object item = queue.poll(1, TimeUnit.SECONDS); // 定期 poll 支持 graceful close
                if (item != null) {
                    handler.accept(item);
                    int c = consumed.incrementAndGet();
                    if (c % 5000 == 0) {
                        Log.info("Channel-" + channelId + " (Step: " + stepId + ")", "Consumed: " + c);
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (onDone != null) onDone.run(); // 通知消费结束
            Log.success("Channel-" + channelId + " (Step: " + stepId + ")", "Consumer done, total: " + consumed.get());
        }
    }

    @Override
    public void subscribe(Consumer<Object> consumer) {
        // 简化调用：忽略 onDone
        this.onReceive(consumer, null);
    }

    @Override
    public synchronized void close() {
        // 关闭通道，阻止新数据进入，通知消费者可退出
        if (!closed) {
            closed = true;
            Log.info("Channel-" + channelId + " (Step: " + stepId + ")", "Closed, remaining: " + queue.size());
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }
}
