package jdtx.repl.main.task;

import jandcode.utils.error.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

import java.util.*;

public abstract class JdxTaskCustom {


    //
    Log log;

    JdxErrorCollector errorCollector;


    public JdxTaskCustom() {
        errorCollector = new JdxErrorCollector();
    }

    void logInfo(String info) {
        log.info(info);
    }


    private void logError(Exception e, String info) {
        if (info != null) {
            info = info + ": ";
        } else {
            info = "";
        }
        log.error(info + Ut.getExceptionMessage(e));
        log.error(Ut.getStackTrace(e));
    }


    void logError(Exception e) {
        logError(e, null);
    }

    void collectError(String info, Exception e) {
        errorCollector.collectError(info, e);
    }

    void sendErrors(IMailer mailer, String name) {
        try {
            if (mailer == null) {
                throw new XError("mailer == null");
            }

            //
            Map data = null;
            if (errorCollector.getErrors().size() != 0) {
                data = new HashMap();
                data.put("errors", errorCollector.getErrors());
            }

            //
            mailer.setData(data, name, null);
        } catch (Exception e) {
            logError(e, "Отправка ошибок");
        }
    }


}
