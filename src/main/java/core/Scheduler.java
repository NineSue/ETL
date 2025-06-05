package core;

import core.flowdata.Row;
import core.flowdata.RowSetTable;
import runtask.Step;
import runtask.StepList;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Scheduler {
    private static final String INPUT = "input";
    private static final String PROCESS = "process";
    private static final String OUTPUT = "output";

    private final Factory factory = new Factory();
    private final Map<Integer, Step> stepsById = new HashMap<>();
    private final Map<Integer, List<Integer>> childrenMap = new HashMap<>();
    private final Map<Integer, AtomicInteger> remainingParents = new ConcurrentHashMap<>();
    private final BlockingQueue<Integer> readyQueue = new LinkedBlockingQueue<>();
    private final Map<Integer, String> stepStatus = new ConcurrentHashMap<>();
    private final Map<String, RowSetTable> inputCache = new ConcurrentHashMap<>();
    private final Map<Integer, SharedRowSetTable> sharedDataMap = new ConcurrentHashMap<>();
    private final Set<Integer> executedSteps = ConcurrentHashMap.newKeySet();

    private final ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private volatile boolean isRunning = true;
    private final CountDownLatch doneLatch;

    public Scheduler(StepList stepList) {
        logInfo("开始执行⌯>ᴗo⌯ .ᐟ.ᐟ");
        for (Step step : stepList.getData()) {
            stepsById.put(step.getStepId(), step);
        }
        this.doneLatch = new CountDownLatch(stepsById.size());
        initDAG();
    }

    private void initDAG() {
        for (Step step : stepsById.values()) {
            int id = step.getStepId();
            List<String> parents = step.getParentStepId();
            remainingParents.put(id, new AtomicInteger(parents.size()));
            for (String pidStr : parents) {
                int pid = Integer.parseInt(pidStr);
                childrenMap.computeIfAbsent(pid, k -> new ArrayList<>()).add(id);
            }
        }

        for (Map.Entry<Integer, AtomicInteger> entry : remainingParents.entrySet()) {
            if (entry.getValue().get() == 0) {
                readyQueue.add(entry.getKey());
            }
        }
    }

    public void execute() {
        try {
            logInfo("工作流执行开始，总步骤数：" + stepsById.size());
            while (isRunning && (!readyQueue.isEmpty() || doneLatch.getCount() > 0)) {
                Integer stepId = readyQueue.poll(1, TimeUnit.SECONDS);
                if (stepId == null) continue;
                Step step = stepsById.get(stepId);
                threadPool.submit(() -> runAndNotify(step));
            }
            doneLatch.await();
            logInfo("所有步骤执行完成");
        } catch (InterruptedException e) {
            logError("工作流执行中断：" + e.getMessage());
            Thread.currentThread().interrupt();
        } finally {
            threadPool.shutdownNow();
            logInfo("线程池关闭(๑•̀ㅁ•́ฅ✧)");
        }
    }

    private void runAndNotify(Step step) {
        int stepId = step.getStepId();

        if (!executedSteps.add(stepId)) {
            logWarn("步骤 [" + stepId + "] 已执行，跳过重复执行");
            return;
        }

        try {
            RowSetTable input = getInput(step);
            String domain = step.getDomain();
            String type = step.getSubType();
            Object result = null;

            switch (domain) {
                case INPUT:
                    String filePath = String.valueOf(step.getConfig().get("filePath"));
                    logInfo("输入步骤 [" + stepId + "]: 读取 " + filePath);
                    result = inputCache.computeIfAbsent(filePath,
                            k -> (RowSetTable) factory.runPlugin(type, step.getConfig()));
                    logSuccess("读取完成，记录数: " + ((RowSetTable) result).getRowList().size());
                    break;
                case PROCESS:
                    logInfo("处理步骤 [" + stepId + "]: " + type);
                    result = factory.runPlugin(type, input);
                    logSuccess("处理完成");
                    break;
                case OUTPUT:
                    logInfo("输出步骤 [" + stepId + "]: " + type);
                    factory.runPlugin(type, input);
                    result = input;
                    logSuccess("输出完成");
                    break;
                default:
                    logError("未知步骤类型 [" + domain + "]，步骤 [" + stepId + "] 执行失败");
                    stepStatus.put(stepId, "failed");
                    return;
            }

            if (result instanceof RowSetTable) {
                sharedDataMap.put(stepId, new SharedRowSetTable((RowSetTable) result));
                stepStatus.put(stepId, "success");
            } else {
                logError("步骤 [" + stepId + "] 返回结果类型不正确");
                stepStatus.put(stepId, "failed");
            }

        } catch (Exception e) {
            logError("步骤 [" + stepId + "] 执行失败：" + e.getMessage());
            stepStatus.put(stepId, "failed");
        } finally {
            notifyChildren(stepId);
            doneLatch.countDown();
        }
    }

    private void notifyChildren(int stepId) {
        if (!"success".equals(stepStatus.get(stepId))) {
            return;
        }

        List<Integer> children = childrenMap.getOrDefault(stepId, Collections.emptyList());
        for (int child : children) {
            AtomicInteger counter = remainingParents.get(child);
            if (counter.decrementAndGet() == 0) {
                List<String> parentIds = stepsById.get(child).getParentStepId();
                boolean allParentsSuccess = parentIds.stream().allMatch(pidStr ->
                        "success".equals(stepStatus.getOrDefault(Integer.parseInt(pidStr), "failed"))
                );
                if (allParentsSuccess) {
                    readyQueue.add(child);
                } else {
                    logWarn("子步骤 [" + child + "] 有失败的父步骤，标记为 skipped");
                    stepStatus.put(child, "skipped");
                    doneLatch.countDown();
                }
            }
        }
    }

    private RowSetTable getInput(Step step) {
        if (INPUT.equals(step.getDomain())) {
            return new RowSetTable(Collections.emptyList());
        }

        List<String> parents = step.getParentStepId();
        if (parents.isEmpty()) {
            return new RowSetTable(Collections.emptyList());
        }

        RowSetTable merged = null;
        for (String pidStr : parents) {
            int pid = Integer.parseInt(pidStr);
            SharedRowSetTable shared = sharedDataMap.get(pid);
            if (shared == null) continue;

            if (merged == null) {
                merged = new RowSetTable(shared.getField());
            }
            merged.getRowList().addAll(shared.getRowList());
        }

        return merged != null ? merged : new RowSetTable(Collections.emptyList());
    }

    public static class SharedRowSetTable {
        private final List<Row> rowList;
        private final List<String> field;

        public SharedRowSetTable(RowSetTable origin) {
            this.rowList = Collections.unmodifiableList(origin.getRowList());
            this.field = Collections.unmodifiableList(origin.getField());
        }

        public List<Row> getRowList() {
            return rowList;
        }

        public List<String> getField() {
            return field;
        }
    }

    // 日志颜色常量
    private static final String COLOR_RESET = "\u001B[0m";
    private static final String COLOR_BLUE = "\u001B[34m";
    private static final String COLOR_YELLOW = "\u001B[33m";
    private static final String COLOR_GREEN = "\u001B[32m";
    private static final String COLOR_RED = "\u001B[31m";

    private void logInfo(String msg) {
        System.out.println(COLOR_BLUE + "[INFO ] " + msg + COLOR_RESET);
    }

    private void logSuccess(String msg) {
        System.out.println(COLOR_GREEN + "[SUCCESS] " + msg + COLOR_RESET);
    }

    private void logWarn(String msg) {
        System.out.println(COLOR_YELLOW + "[WARN ] " + msg + COLOR_RESET);
    }

    private void logError(String msg) {
        System.err.println(COLOR_RED + "[ERROR] " + msg + COLOR_RESET);
    }
}
