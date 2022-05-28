package jdtx.repl.main.api.log;

import org.apache.log4j.*;
import org.apache.log4j.spi.*;

import java.time.*;
import java.time.format.*;

public class JdtxLogAppender extends AppenderSkeleton {

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String url;

    private static String value;

    public static synchronized void setLogValue(String logValue) {
        value = logValue;
    }

    public static synchronized String getLogValue() {
        return value;
    }

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("YYYY.MM.dd HH:mm:ss,SSS");

    protected void append(LoggingEvent event) {
        String s = LocalDateTime.now().format(FORMATTER) +
                " : Thread:[" + Thread.currentThread().getName() + "] : " + event.getMessage();
        //System.out.println("Jdtx-Log-Appender: " + s);
        setLogValue(s);
    }

    public void close() {

    }

    public boolean requiresLayout() {
        return false;
    }

}
