package jdtx.repl.main.log;

import org.apache.log4j.*;
import org.apache.log4j.spi.*;

public class JdtxLogAppender extends AppenderSkeleton {

    public String getConversionPattern() {
        return conversionPattern;
    }

    public void setConversionPattern(String conversionPattern) {
        this.conversionPattern = conversionPattern;
    }

    public String conversionPattern;

    protected void append(LoggingEvent event) {
        PatternLayout layout = new PatternLayout(conversionPattern);
        //
        String s = layout.format(event);
        //
        JdtxLogStorage.setLogValue(s);
    }

    public void close() {

    }

    public boolean requiresLayout() {
        return false;
    }

}
