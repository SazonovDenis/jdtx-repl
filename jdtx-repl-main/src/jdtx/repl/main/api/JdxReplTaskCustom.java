package jdtx.repl.main.api;

import jandcode.utils.error.*;
import jdtx.repl.main.action.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

import java.util.*;

public class JdxReplTaskCustom {

    //
    List errors = new ArrayList();

    //
    Log log;

    //
    void collectError(String err, Exception e) {
        Map info = new HashMap();
        info.put("operation", err);
        info.put("error", Ut.getExceptionMessage(e));
        errors.add(info);
    }


    void logInfo(String info) {
        log.info(info);
        //getLogger().push("state", info);
    }


    private void logError(Exception e, String info) {
        if (info != null) {
            info = info + ": ";
        } else {
            info = "";
        }
        //getLogger().push("state", info + Ut.getExceptionMessage(e));
        log.error(info + Ut.getExceptionMessage(e));
        log.error(Ut.getStackTrace(e));
    }


    void logError(Exception e) {
        logError(e, null);
    }

/*
    public BgTasksLogger getLogger() {
        return logger;
    }
*/

    void sendErrors(IMailer mailer, String name) {
        try {
            if (mailer == null) {
                throw new XError("mailer == null");
            }

            //
            Map data = null;
            if (errors.size() != 0) {
                data = new HashMap();
                data.put("errors", errors);
            }

            //
            mailer.setData(data, name, null);
        } catch (Exception e) {
            logError(e, "Отправка ошибок");
        }
    }


}
