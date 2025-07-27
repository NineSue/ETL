package core;
import core.intf.IInput;
import core.intf.IOutput;
import core.intf.IProcess;
import runtask.Step;
import runtask.StepList;
import tool.Log;
import tool.Tuning;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static tool.Tuning.init;

public class Scheduler {
    static {
        init(); // 初始化日志与参数配置
        Checker.run("plugin", "anno"); // 检查插件类与注解是否匹配
    }

    private static final String INPUT = "input";
    private static final String PROCESS = "process";
    private static final String OUTPUT = "output";

    private final Factory fact = new Factory(); // 插件工厂
    private final Map<Integer, Step> steps = new HashMap<>(); // 所有步骤
    private final Map<Integer, AtomicInteger> remain = new ConcurrentHashMap<>(); // 每个步骤还需等待的上游数
    private final Map<Integer, List<Integer>> children = new HashMap<>(); // 每个步骤的下游
    private final Map<Integer, Channel> inCh = new ConcurrentHashMap<>(); // 每个步骤的输入通道
    private final Map<Integer, List<Channel>> outCh = new ConcurrentHashMap<>(); // 每个步骤的输出通道
    private final ExecutorService pool = Executors.newFixedThreadPool(Tuning.threadPoolSize()); // 全局线程池

    private final List<Integer> inputSteps = new ArrayList<>();
    private final List<Integer> processSteps = new ArrayList<>();
    private final List<Integer> outputSteps = new ArrayList<>();

    private final Map<Integer, CompletableFuture<Void>> stepFutures = new ConcurrentHashMap<>();
    private final Map<Integer, CountDownLatch> stepReadyLatches = new ConcurrentHashMap<>();

    public Scheduler(StepList list) {
        Tuning.print();
        Log.info("Scheduler", "Start scheduler setup");

        // 注册步骤，构建通道、等待计数器、步骤图
        list.getData().forEach(s -> {
            int id = s.getStepId();
            steps.put(id, s);
            inCh.put(id, new Channel(pool, String.valueOf(id))); // 通道中注入线程池与 stepId
            outCh.put(id, new ArrayList<>());
            remain.put(id, new AtomicInteger(s.getParentStepId().size())); // 初始上游依赖数
            stepReadyLatches.put(id, new CountDownLatch(1)); // 等待下游准备
        });

        // 建立父子依赖图与输出通道连接
        steps.values().forEach(s -> {
            int curId = s.getStepId();
            for (String pid : s.getParentStepId()) {
                int parentId = Integer.parseInt(pid);
                children.computeIfAbsent(parentId, k -> new ArrayList<>()).add(curId);
                outCh.computeIfAbsent(parentId, k -> new ArrayList<>()).add(inCh.get(curId));
            }
        });

        // 分类步骤类型（输入/处理/输出）
        list.getData().forEach(s -> {
            int id = s.getStepId();
            switch (s.getDomain()) {
                case INPUT: inputSteps.add(id); break;
                case PROCESS: processSteps.add(id); break;
                case OUTPUT: outputSteps.add(id); break;
            }
        });

        Log.success("Scheduler", "Setup done " +
                inputSteps.size() + " inputs " +
                processSteps.size() + " processes " +
                outputSteps.size() + " outputs");
    }

    // 启动输出步骤（被输入步骤依赖，需要优先准备）
    private CompletableFuture<Void> runOutputStepAsync(Step s) {
        int id = s.getStepId();
        return CompletableFuture.runAsync(() -> {
            try {
                IOutput o = fact.getPlugin(s.getSubType(), IOutput.class);
                o.init(s.getConfig());
                stepReadyLatches.get(id).countDown(); // 通知下游已准备
                o.consume(inCh.get(id));
            } catch (Exception e) {
                throw new RuntimeException("Output step " + id + " failed", e);
            }
        }, pool);
    }

    // 启动输入步骤（需要等待下游就绪）
    private CompletableFuture<Void> runInputStepAsync(Step s) {
        int id = s.getStepId();
        return CompletableFuture.runAsync(() -> {
            try {
                for (Integer downstreamId : children.getOrDefault(id, Collections.emptyList())) {
                    stepReadyLatches.get(downstreamId).await(30, TimeUnit.SECONDS);
                }
                IInput in = fact.getPlugin(s.getSubType(), IInput.class);
                in.init(s.getConfig());
                in.start(outCh.get(id));
            } catch (Exception e) {
                throw new RuntimeException("Input step " + id + " failed", e);
            }
        }, pool);
    }

    // 启动处理步骤
    private CompletableFuture<Void> runProcessStepAsync(Step s) {
        int id = s.getStepId();
        return CompletableFuture.runAsync(() -> {
            try {
                IProcess p = fact.getPlugin(s.getSubType(), IProcess.class);
                p.init(s.getConfig());
                stepReadyLatches.get(id).countDown(); // 通知下游已准备
                p.process(inCh.get(id), outCh.get(id));
            } catch (Exception e) {
                throw new RuntimeException("Process step " + id + " failed", e);
            }
        }, pool);
    }

    public void execute() throws InterruptedException {
        Log.info("Scheduler", "Start pipeline");

        // 启动顺序：先输出，再处理，最后输入（输入可能阻塞等待下游准备）
        for (Integer id : outputSteps) stepFutures.put(id, runOutputStepAsync(steps.get(id)));
        for (Integer id : processSteps) stepFutures.put(id, runProcessStepAsync(steps.get(id)));
        for (Integer id : inputSteps) stepFutures.put(id, runInputStepAsync(steps.get(id)));

        try {
            CompletableFuture.allOf(stepFutures.values().toArray(new CompletableFuture[0]))
                    .get(30, TimeUnit.MINUTES);
            Log.success("Scheduler", "All steps done");
        } catch (TimeoutException e) {
            Log.warn("Scheduler", "Time out");
        } catch (ExecutionException e) {
            Log.error("Scheduler", "Run failed: " + e.getCause().getMessage());
        } finally {
            pool.shutdown();
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow();
                Log.warn("Scheduler", "Force close thread pool");
            } else {
                Log.success("Scheduler", "Thread pool closed");
            }
        }
    }
}
