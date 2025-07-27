package plugin.input;
import anno.Input;
import core.Channel;
import core.flowdata.Row;
import core.flowdata.RowSetTable;
import core.intf.IInput;
import tool.Log;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Input(type = "csv")
public class CsvInput implements IInput {
    private Map<String, Object> config;
    private char delimiter = ',';
    private char quoteChar = '"';
    private boolean hasHeader = true;
    private String filePath;

    @Override
    public void init(Map<String, Object> cfg) {
        this.config = cfg;
        this.filePath = (String) cfg.get("filePath");
        String delimiterStr = (String) cfg.getOrDefault("delimiter", ",");
        String quoteStr = (String) cfg.getOrDefault("quoteChar", "\"");
        this.hasHeader = (Boolean) cfg.getOrDefault("hasHeader", true);

        if (filePath == null || filePath.isEmpty()) {
            throw new IllegalArgumentException("Missing file path");
        }
        if (delimiterStr.length() != 1) {
            throw new IllegalArgumentException("Delimiter must be a single character");
        }
        if (quoteStr.length() != 1) {
            throw new IllegalArgumentException("Quote character must be a single character");
        }

        this.delimiter = delimiterStr.charAt(0);
        this.quoteChar = quoteStr.charAt(0);
        Log.info("CsvInput", "Init with path: " + filePath);
    }

    @Override
    public void start(List<Channel> outputs) throws Exception {
        File file = new File(filePath);
        if (!file.exists()) {
            throw new IllegalArgumentException("File not found: " + filePath);
        }

        Log.info("CsvInput", "Start reading: " + filePath);
        boolean headerProcessed = !hasHeader;
        long lineCount = 0;
        long startTime = System.currentTimeMillis();

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                lineCount++;

                if (!headerProcessed) {
                    Row headRow = parseCsvLine(line);
                    // Assuming RowChangeTable() is a valid method that converts a Row to a RowSetTable (header schema)
                    RowSetTable table = headRow.RowChangeTable();
                    // Set Header to all output channels
                    for (Channel output : outputs) {
                        output.setHeader(table);
                    }
                    Log.header("CsvInput", String.join(", ", table.getField()));
                    headerProcessed = true;
                    continue;
                }

                Row row = parseCsvLine(line);
                // Publish data to all output channels
                for (Channel output : outputs) {
                    output.publish(row);
                }
                Log.data("CsvInput", row.toString());

                if (lineCount % 10000 == 0) {
                    long elapsed = System.currentTimeMillis() - startTime;
                    Log.info("CsvInput", "Processed " + lineCount + " lines");
                }
            }
        } catch (IOException e) {
            Thread.currentThread().interrupt(); // Ensure interrupt status is preserved
            Log.error("CsvInput", "Read error: " + e.getMessage());
            throw new Exception("Error reading CSV file", e);
        } finally {
            // Ensure channels are closed in all cases
            long totalTime = System.currentTimeMillis() - startTime;
            long linesPerSecond = lineCount * 1000 / Math.max(totalTime, 1);
            Log.success("CsvInput", "Read completed");
            Log.success("CsvInput", "Total lines: " + lineCount);
            Log.success("CsvInput", "Time: " + totalTime + "ms");
            Log.success("CsvInput", "Speed: " + linesPerSecond + " lines/sec");

            // Close all output channels to signal end of stream
            for (Channel output : outputs) {
                output.close();
            }
            Log.info("CsvInput", "All output channels closed");
        }
    }

    private Row parseCsvLine(String line) {
        Row row = new Row();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == quoteChar) {
                if (i + 1 < line.length() && line.charAt(i + 1) == quoteChar) {
                    current.append(quoteChar);
                    i++;
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == delimiter && !inQuotes) {
                row.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        row.add(current.toString());

        if (inQuotes) {
            throw new IllegalArgumentException("Unclosed quotes in CSV line");
        }
        return row;
    }
}