package core;

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

    private final Factory fact = new Factory();
    private final Map<Integer, Step> steps = new HashMap<>();
    private final Map<Integer, AtomicInteger> remain = new ConcurrentHashMap<>();
    private final Map<Integer, List<Integer>> children = new HashMap<>();
    private final BlockingQueue<Integer> ready = new LinkedBlockingQueue<>();
    private final CountDownLatch gate;
    private final Map<Integer, Channel> inCh = new ConcurrentHashMap<>();
    private final Map<Integer, List<Channel>> outCh = new ConcurrentHashMap<>();
    private final ExecutorService pool = ThreadPoolManger.getThreadPool(); // 🔥 公共线程池
    private final List<Integer> inputSteps = new ArrayList<>();
    private final List<Integer> processSteps = new ArrayList<>();
    private final List<Integer> outputSteps = new ArrayList<>();

    public Scheduler(StepList list) {
        System.out.println("开始执行-.-");
        list.getData().forEach(s -> {
            int id = s.getStepId();
            steps.put(id, s);
            Channel ch = new Channel(pool); // 🔥 注入线程池
            inCh.put(id, ch);
            outCh.put(id, new ArrayList<>());
            remain.put(id, new AtomicInteger(s.getParentStepId().size()));
        });

        gate = new CountDownLatch(steps.size());

        steps.keySet().forEach(id -> {
            if (remain.get(id).get() == 0)
                ready.add(id);
        });

        steps.values().forEach(s -> {
            int curId = s.getStepId();
            for (String pid : s.getParentStepId()) {
                int parentId = Integer.parseInt(pid);
                children.computeIfAbsent(parentId, k -> new ArrayList<>()).add(curId);
                outCh.computeIfAbsent(parentId, k -> new ArrayList<>()).add(inCh.get(curId));
            }
        });

        list.getData().forEach(s -> {
            int id = s.getStepId();
            switch (s.getDomain()) {
                case INPUT: inputSteps.add(id); break;
                case PROCESS: processSteps.add(id); break;
                case OUTPUT: outputSteps.add(id); break;
            }
        });
    }

    private void run(Step s) {
        int id = s.getStepId();
        if (!remain.containsKey(id)) return;

        try {
            System.out.println("开始执行 step: " + id);
            switch (s.getDomain()) {
                case INPUT: {
                    List<Channel> outs = outCh.get(id);
                    if (outs == null || outs.isEmpty())
                        throw new IllegalStateException("Input step无下游");

                    IInput in = fact.getPlugin(s.getSubType(), IInput.class);
                    in.init(s.getConfig());
                    in.start(outs.get(0));
                    break;
                }
                case PROCESS: {
                    IProcess p = fact.getPlugin(s.getSubType(), IProcess.class);
                    p.init(s.getConfig());
                    Channel in = inCh.get(id);
                    List<Channel> outs = outCh.get(id);
                    p.process(in, outs);
                    break;
                }
                case OUTPUT: {
                    IOutput o = fact.getPlugin(s.getSubType(), IOutput.class);
                    o.init(s.getConfig());
                    Channel in = inCh.get(id);
                    o.consume(in);
                    break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            children.getOrDefault(id, Collections.emptyList())
                    .forEach(c -> {
                        if (remain.get(c).decrementAndGet() == 0)
                            ready.add(c);
                    });
            gate.countDown();
        }
        System.out.println("当前剩余 gate: " + gate.getCount());
    }

    public void execute() throws InterruptedException {
        long start = System.currentTimeMillis();
        while (gate.getCount() > 0) {
            Integer stepId = ready.poll(1, TimeUnit.SECONDS);
            if (stepId != null && steps.containsKey(stepId)) {
                pool.submit(() -> run(steps.get(stepId)));
            }
        }
        gate.await();
        long end = System.currentTimeMillis();
        System.err.println("🎉 执行完成，总耗时: " + (end - start) + "ms");
        pool.shutdown();
    }

    static {
        Checker.run("plugin", "anno");
    }
}