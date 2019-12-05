package jdtx.repl.main.task;

import jandcode.bgtasks.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

import java.util.*;

/**
 * BgTask - предок
 */
public class BgTaskCustom extends BgTask {


    //
    static Log log = LogFactory.getLog("jdtx.BgTask");

    List errors = new ArrayList();


    void collectError(String err, Exception e) {
        Map info = new HashMap();
        info.put("operation", err);
        info.put("error", Ut.getExceptionMessage(e));
        errors.add(info);
    }


    void logInfo(String info) {
        log.info(info);
        getLogger().put("state", info);
    }


    void logError(Exception e, String info) {
        if (info != null) {
            info = info + ": ";
        } else {
            info = "";
        }
        getLogger().put("state", info + Ut.getExceptionMessage(e));
        log.error(info + Ut.getExceptionMessage(e));
        log.error(Ut.getStackTrace(e));
    }


    void logError(Exception e) {
        logError(e, null);
    }


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
