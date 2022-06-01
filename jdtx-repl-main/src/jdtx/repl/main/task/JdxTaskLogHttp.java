package jdtx.repl.main.task;

import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.log.*;
import org.apache.commons.logging.*;

import java.util.*;

/**
 * Логирование на сервер
 */
public class JdxTaskLogHttp extends JdxTaskCustom {

    //
    IMailer mailer;

    //
    public JdxTaskLogHttp(IMailer mailer) {
        this.mailer = mailer;
        log = LogFactory.getLog("jdtx.JdxTaskLogHttp");
    }

    //
    public void doTask() throws Exception {
        Map<String, String> logValues = JdtxLogStorage.getLogValues();
        //
        Map data = new HashMap();
        data.put("logValues", logValues);
        //
        try {
            mailer.setData(data, "log.log", null);
        } catch (Exception e) {
            log.warn(e.getMessage());
        }
    }

}
