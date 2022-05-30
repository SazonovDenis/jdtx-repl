package jdtx.repl.main.task;

import jdtx.repl.main.api.*;
import jdtx.repl.main.log.*;
import jdtx.repl.main.api.mailer.*;
import org.apache.commons.logging.*;

import java.util.*;

/**
 * Логирование на сервер
 */
public class JdxTaskLogHttp extends JdxTaskCustom {

    //
    IMailer mailer;

    //
    public JdxTaskLogHttp(JdxReplWs ws) {
        mailer = ws.getMailer();
        log = LogFactory.getLog("jdtx.JdxTaskLogHttp");
    }

    //
    public void doTask() throws Exception {
        try {
            String logValue = JdtxLogAppender.getLogValue();
            Map data = new HashMap();
            data.put("logValue", logValue);
            mailer.setData(data, "log.log", "from");
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

}
