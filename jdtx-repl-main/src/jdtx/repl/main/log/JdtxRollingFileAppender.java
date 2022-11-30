package jdtx.repl.main.log;

import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Обычный RollingFileAppender, но с возможностью отфильтровать по MDC.
 * <p>
 * Заинтересованный код вызывает:
 * MDC.put("serviceName", "srvMR")
 * <p>
 * В конфигурации логера задать нужный serviceName:
 * log4j.appender.JDTX_FILE_SRVMR.serviceName=srvMR
 * <p>
 * Тогда в лог будут попадать только те события, у которых MDC.serviceName установлен в нужное значение.
 */
public class JdtxRollingFileAppender extends RollingFileAppender {

    String serviceName;

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public void append(LoggingEvent event) {
        String key = event.getProperty("serviceName");
        if (key != null && key.equals(serviceName)) {
            super.append(event);
        }
    }

}
