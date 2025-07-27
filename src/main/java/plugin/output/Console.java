package plugin.output;
import anno.Output;
import core.Channel;
import core.flowdata.RowSetTable;
import core.intf.IOutput;
import tool.Log;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

@Output(type = "console")
public class Console implements IOutput {
    @Override
    public void init(Map<String, Object> config) {
        Log.info("ConsoleOutput", "Init completed");
    }

    @Override
    public void consume(Channel input) throws Exception {
        String channelId = getShortChannelId(input);
        Log.info("ConsoleOutput", "Ready to consume from channel: " + channelId);
        AtomicLong consumedCount = new AtomicLong(0);
        Object lastProcessedData = null;
        long startTime = System.currentTimeMillis();
        long lastReportTime = startTime;
        boolean firstDataReceived = false;

        while (!input.isClosed() || !input.isEmpty()) {
            try {
                Object data = input.poll(2000); // Poll with a timeout
                if (data != null) {
                    if (!firstDataReceived) {
                        Log.info("ConsoleOutput", "First data received, start consuming");
                        firstDataReceived = true;
                        RowSetTable header = input.getHeader();
                        if (header != null) {
                            Log.header("ConsoleOutput", String.join(", ", header.getField()));
                        } else {
                            Log.warn("ConsoleOutput", "No header info available on channel " + channelId);
                        }
                    }
                    lastProcessedData = data;
                    consumedCount.incrementAndGet();
                    Log.data("ConsoleOutput", data.toString());

                    long currentTime = System.currentTimeMillis();
                    if (consumedCount.get() % 5000 == 0 || (currentTime - lastReportTime) > 5000) {
                        long elapsed = currentTime - startTime;
                        long speed = consumedCount.get() * 1000 / Math.max(elapsed, 1);
                        Log.info("ConsoleOutput", "Consumed: " + consumedCount.get() + " records (Speed: " + speed + " records/sec)");
                        lastReportTime = currentTime;
                    }
                } else {
                    // If data is null, check if channel is closed and empty to break loop
                    if (input.isClosed() && input.isEmpty()) {
                        break;
                    }
                    // If not closed or not empty, continue polling
                    if (!input.isClosed()) continue;
                }
            } catch (InterruptedException e) {
                Log.warn("ConsoleOutput", "Consume interrupted");
                Thread.currentThread().interrupt(); // Restore interrupt status
                break;
            }
        }

        long totalTime = System.currentTimeMillis() - startTime;
        long totalCount = consumedCount.get();
        long avgSpeed = totalTime > 0 ? (totalCount * 1000 / totalTime) : 0;

        Log.success("ConsoleOutput", "Consume completed");
        Log.success("ConsoleOutput", "Total records: " + totalCount);
        Log.success("ConsoleOutput", "Total time: " + totalTime + "ms");
        Log.success("ConsoleOutput", "Average speed: " + avgSpeed + " records/sec");

        if (lastProcessedData != null) {
            Log.info("ConsoleOutput", "Last processed data: " + lastProcessedData);
        }
    }

    // Helper method to get a short channel ID for logging, duplicated from Channel.java for convenience
    private String getShortChannelId(Channel channel) {
        return Integer.toHexString(channel.hashCode()).substring(0, Math.min(6, Integer.toHexString(channel.hashCode()).length()));
    }
}