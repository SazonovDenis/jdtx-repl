package jdtx.repl.main.log;

import com.jdtx.state.StateItemStackNamed;
import org.apache.log4j.*;
import org.apache.log4j.spi.*;
import org.joda.time.DateTime;

public class JdtxLogAppender extends AppenderSkeleton {

    public String getConversionPattern() {
        return conversionPattern;
    }

    public void setConversionPattern(String conversionPattern) {
        this.conversionPattern = conversionPattern;
    }

    public String conversionPattern;

    protected static StateItemStackNamed state = JdtxStateContainer.state;

    protected void append(LoggingEvent event) {
        // Обновим JdtxLogContainer
        PatternLayout layout = new PatternLayout(conversionPattern);
        String value = layout.format(event);
        value = value.trim();

        //
        String key = event.getProperty("serviceName");
        if (key == null) {
            key = "default";
        }

        //
        JdtxLogContainer.setLogValue(key, value);


        // Обновим JdtxStateContainer.state
        String text = event.getRenderedMessage().trim();
        state.get().setValue("log.name", event.categoryName);
        state.get().setValue("log.serviceName", key);
        state.get().setValue("log.datetime", new DateTime(event.getTimeStamp()));
        state.get().setValue("log.text", text);
    }

    public void close() {

    }

    public boolean requiresLayout() {
        return false;
    }

}
