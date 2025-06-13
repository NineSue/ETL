package core;

import core.intf.IChannel;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/*
    生产者线程（publish） → 数据入队 → 唤醒消费者线程
                                     ↓
    消费者线程（subscribe）→ 从队列取出数据 → 调用Consumer处理
                                     ↑
    关闭通道（close） → 标记closed=true → 唤醒所有等待线程 → 关闭线程池
*/
public class Channel<T> implements IChannel<T> {
    private final Queue<T> queue=new LinkedList<>();
    //LinkedList筹备了两套操作，链表与队列
    private final ExecutorService executor= Executors.newSingleThreadExecutor();
    //单线程线程池 -> 1,1,0，milliseconds,LinkedBlockingQueue
    private Consumer<T> consumer;
    private volatile boolean closed=false;

    @Override
    public synchronized void publish(T row) throws InterruptedException{
        if(closed) throw new IllegalStateException("通道已关闭");
        queue.offer(row);
        notifyAll();
    }

    @Override
    public synchronized void subscribe(Consumer<T> consumer){
        this.consumer=consumer;
        executor.submit(()-> {
            try {
                while (!closed || !queue.isEmpty()) {
                    T row;
                    synchronized (this) {
                        while (queue.isEmpty() && closed) wait();
                        row = queue.poll();
                    }
                    if (row != null && consumer != null)
                        consumer.accept(row);
                }
            } catch (InterruptedException e) {
            }
        });
    }

    @Override
    public synchronized void close(){
        closed=true;
        notifyAll();
//        executor.shutdown();
    }

    @Override
    public boolean isClosed(){
        return closed;
    }

    @Override
    public  synchronized void onReceive(Consumer<T> handler,Runnable onComplete){
        this.consumer=handler;
        executor.submit(()->{
           try{
               while (!closed||!queue.isEmpty()){
                   T row;
                   synchronized (this){
                       while(queue.isEmpty()&&!closed) wait();
                       row=queue.poll();
                   }
                   if(row!=null&&consumer!=null)
                       consumer.accept(row);
               }
           }catch (InterruptedException e){}
           finally {
                if(onComplete!=null)
                    onComplete.run();
           }
        });
    }
}
