package core;

import core.flowdata.Row;
import core.intf.IInput;
import core.intf.IOutput;
import core.intf.IProcess;
import runtask.Step;
import runtask.StepList;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


public class Scheduler {
    private static final String INPUT = "input";
    private static final String PROCESS = "process";
    private static final String OUTPUT = "output";

    private final Factory fact=new Factory();
    private final Map<Integer, Step> steps=new HashMap<>();
    /*
        CAS(Compare-And-Swap)=乐观锁+原子操作
        CAS是CPU原生支持的无锁原子操作，依赖于汇编指令 lock cmpxchg，不会被线程打断
        非阻塞，高性能，无需加锁，但存在CPU空转，例如多线程反复失败
        java通过unsafe的JNI调用本地的C方法
        CAS只能判断期望值，无法判断中间是否改过，故存在ABA问题，解决方法的话是通过引入AtomicStampedReference ，在比较的时候比较期望值与版本号
     */
    private final Map<Integer, AtomicInteger> remain=new ConcurrentHashMap<>();//剩余步骤
    private final Map<Integer,List<Integer>> chilren=new HashMap<>();
    private final BlockingQueue<Integer> ready=new LinkedBlockingQueue<>();
    private final Set<Integer> done=ConcurrentHashMap.newKeySet();
    private final CountDownLatch gate;
    private final Map<Integer,Channel<Row>> inCh=new ConcurrentHashMap<>();
    private final Map<Integer,Channel<Row>> outCh=new ConcurrentHashMap<>();
    private final ExecutorService pool=Executors.newCachedThreadPool();
    //可缓存线程池 ->  0, Integer.MAX_VALUE,60L, SECONDS,SynchronousQueue

    /*
        1
        通过遍历步骤列表建立:
        ID->步骤,
        ID->双向管道,
        ID->上游步骤数。

        2
        总步骤完成计数器gate，完成则递减，后续归0放行

        3
        去看步骤列表谁是没有上游节点的，放入就绪队列，等待调度

        4
        建立上游与下游映射，遍历每个步骤的上游步骤列表，将当前步骤ID添加到对应上游步骤的下游列表中



    */
    public Scheduler(StepList list){
        System.out.println("开始执行-.-");
        //1
        list.getData().forEach(s->{
            int id=s.getStepId();
            steps.put(id,s);
            inCh.put(id,new Channel<>());
            outCh.put(id,new Channel<>());
            remain.put(id,new AtomicInteger(s.getParentStepId().size()));
        });
        //2
        gate=new CountDownLatch(steps.size());
        //3
        steps.keySet().forEach(id->{
            if(remain.get(id).get()==0)
                ready.add(id);
        });
        //4
        steps.values().forEach(s->{
            List<String> parentIds=s.getParentStepId();
            int curStepId=s.getStepId();

            for(String p:parentIds){
                int parentId=Integer.parseInt(p);
                if(!chilren.containsKey(parentId)){
                    chilren.put(parentId,new ArrayList<>());
                }

            List<Integer>childList=chilren.get(parentId);
            childList.add(curStepId);
            }
        });
    }

    private void run(Step s){
        int id=s.getStepId();
        if(!remain.containsKey(id)||
                !remain.get(id).compareAndSet(remain.get(id).get(),remain.get(id).get())){}
        Factory f=fact;
        try{
            System.out.println("开始执行 step: " + id);
        switch (s.getDomain()){
            case INPUT:{
                IInput in=f.getPlugin(s.getSubType(),IInput.class);
                in.init(s.getConfig());
                Channel<Row> out=outCh.get(id);
                in.start(out);
                out.close();
                break;
            }
            case PROCESS:{
                IProcess p=f.getPlugin(s.getSubType(),IProcess.class);
                p.init(s.getConfig());
                Channel<Row> in=inCh.get(id);
                Channel<Row> out=outCh.get(id);
                AtomicInteger cnt=new AtomicInteger(s.getParentStepId().size());

//                s.getParentStepId().forEach(pId->{
//                        outCh.get(Integer.parseInt(pId)).subscribe(r->{
//                                try{
//                                    in.publish(r);
//                                } catch (InterruptedException e){}
//                            });
//                        }
//                    );
                for(String pIdStr:s.getParentStepId()){
                    int pId=Integer.parseInt(pIdStr);
                    outCh.get(pId).onReceive(r-> {
                        try {
                            in.publish(r);
                        } catch (InterruptedException e) {
                        }
                    }, ()->{
                        if(cnt.decrementAndGet()==0)
                            in.close();
                    });
                }

                p.process(in,out);
                out.close();
                break;
            }
            case OUTPUT:{
                IOutput o=f.getPlugin(s.getSubType(),IOutput.class);
                o.init(s.getConfig());
                Channel<Row> in=inCh.get(id);
//                s.getParentStepId().forEach(pId->
//                        outCh.get(Integer.parseInt(pId)).subscribe(r ->{
//                            try{
//                                in.publish(r);
//                            }catch (InterruptedException e){}
//                        }));
//                in.close();
                AtomicInteger cnt=new AtomicInteger(s.getParentStepId().size());
                for(String pIdStr:s.getParentStepId()){
                    int pId=Integer.parseInt(pIdStr);
                    outCh.get(pId).onReceive(r-> {
                        try {
                            in.publish(r);
                        } catch (InterruptedException e) {
                        }
                    }, ()->{
                        if(cnt.decrementAndGet()==0)
                            in.close();
                    });
                }

                o.consume(in);
                break;
            }
        }
        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            chilren.getOrDefault(id,Collections.emptyList())
                    .forEach(c->{
                        if(remain.get(c).decrementAndGet()==0)
                            ready.add(c);
                    });
            gate.countDown();
        }
        System.out.println("当前剩余 gate: " + gate.getCount());
    }

//    public void execute() throws InterruptedException {
//        while (!ready.isEmpty()) {
//            Integer stepId = ready.poll();
//            if (stepId != null && steps.containsKey(stepId)) {
//                Step step = steps.get(stepId);
//                pool.submit(() -> run(step));
//            }
//        }
//        gate.await();
//        pool.shutdown();
//    }
    public void execute() throws InterruptedException {
        while (gate.getCount() > 0) {
            Integer stepId = ready.poll(1, TimeUnit.SECONDS); // 等待新 step
            if (stepId != null && steps.containsKey(stepId)) {
                Step step = steps.get(stepId);
                pool.submit(() -> run(step));
            }
        }
        gate.await();
        pool.shutdown();
    }
    static {
        Checker.run("plugin", "anno");
    }
}