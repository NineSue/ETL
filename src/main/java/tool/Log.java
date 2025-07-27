package tool;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Log {
    public static int LOG_LEVEL = 1; // 0=none, 1=data only, 2=info/warn/error only, 3=all
    public static boolean ENABLE_KAOMOJI = true;

    private static final String RESET          = "\u001B[0m";
    private static final String BLACK          = "\u001B[30m";
    private static final String BLUE           = "\u001B[34m";   // Tag color
    private static final String DARK_GRAY      = "\u001B[90m";   // Row data color, dark gray, not harsh
    private static final String YELLOW         = "\u001B[33m";
    private static final String GREEN          = "\u001B[32m";
    private static final String RED            = "\u001B[31m";

    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // data and header logs are only output when LOG_LEVEL == 1 or 3
    public static void data(String tag, String content) {
        if (LOG_LEVEL == 1 || LOG_LEVEL == 3) {
            String formattedContent = removeBrackets(content);
            logData(tag, formattedContent);
        }
    }

    public static void header(String tag, String content) {
        if (LOG_LEVEL == 1 || LOG_LEVEL == 3) {
            String formattedContent = removeBrackets(content);
            logHeader(tag, formattedContent);
        }
    }

    // info/success/warn/error are only output when LOG_LEVEL >= 2 (i.e., 2 and 3)
    public static void info(String tag, String msg) {
        if (LOG_LEVEL >= 2) log("INFO", tag, beautify(msg), BLUE, null);
    }

    public static void success(String tag, String msg) {
        if (LOG_LEVEL >= 2) {
            String tail = shouldAddKaomoji(tag, msg) ? "ദ്ദി˶ｰ̀֊ｰ́ )" : null;
            log("OK", tag, beautify(msg), GREEN, tail);
        }
    }

    public static void warn(String tag, String msg) {
        if (LOG_LEVEL >= 2) log("WARN", tag, beautify(msg), YELLOW, null);
    }

    public static void error(String tag, String msg) {
        if (LOG_LEVEL >= 2) {
            String tail = shouldAddKaomoji(tag, msg) ? "(┙>∧<)┙へ┻┻" : null;
            log("ERROR", tag, beautify(msg), RED, tail);
        }
    }

    // debug/trace are only output when LOG_LEVEL == 3
    public static void debug(String tag, String msg) {
        if (LOG_LEVEL == 3) log("DEBUG", tag, beautify(msg), null, null);
    }

    public static void trace(String tag, String msg) {
        if (LOG_LEVEL == 3) log("TRACE", tag, beautify(msg), null, null);
    }

    private static void log(String level, String tag, String msg, String color, String tail) {
        if (LOG_LEVEL == 0) return;
        String time = LocalDateTime.now().format(TIME_FMT);
        String thread = Thread.currentThread().getName();
        String base = String.format("[%s] [%-5s] [%s] [%s] %s", time, level, tag, thread, msg);
        if (tail != null) base += " " + tail;
        if (color != null) {
            System.out.println(color + base + RESET);
        } else {
            System.out.println(base);
        }
    }

    private static boolean shouldAddKaomoji(String tag, String msg) {
        if (!ENABLE_KAOMOJI) return false;
        String lower = msg.toLowerCase();
        return lower.contains("validation passed") ||
                lower.contains("all steps done") ||
                lower.contains("pipeline done");
    }

    private static String beautify(String msg) {
        // Placeholder for future message beautification logic
        return msg;
    }

    private static void logData(String tag, String content) {
        if (LOG_LEVEL == 0) return;
        String time = LocalDateTime.now().format(TIME_FMT);
        String thread = Thread.currentThread().getName();
        String prefix = String.format("[%s] ", time) +
                BLUE + "[DATA ]" + RESET + " " +
                "[" + tag + "]" + " [" + thread + "]";
        String[] lines = content.split("\n");
        System.out.println(prefix);
        for (String line : lines) {
            System.out.println("\t" + DARK_GRAY + line + RESET);
            System.out.println("\t" + DARK_GRAY + "───────────────────────────────" + RESET); // Separator line
        }
    }

    private static void logHeader(String tag, String content) {
        if (LOG_LEVEL == 0) return;
        String time = LocalDateTime.now().format(TIME_FMT);
        String thread = Thread.currentThread().getName();
        String prefix = String.format("[%s] ", time) +
                BLUE + "[HEAD ]" + RESET + " " +
                "[" + tag + "]" + " [" + thread + "]";
        String[] lines = content.split("\n");
        System.out.println(prefix);
        for (String line : lines) {
            System.out.println("\t" + DARK_GRAY + line + RESET);
            System.out.println("\t" + DARK_GRAY + "───────────────────────────────" + RESET);
        }
    }

    private static String removeBrackets(String s) {
        if (s == null) return "";
        s = s.trim();
        if (s.startsWith("[") && s.endsWith("]")) {
            s = s.substring(1, s.length() - 1).trim();
        }
        return s;
    }
}