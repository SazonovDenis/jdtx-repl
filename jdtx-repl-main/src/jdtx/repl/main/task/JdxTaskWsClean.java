package jdtx.repl.main.task;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.util.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;

/**
 * Выполняет шаги "очистка старых реплик" для рабочей станции.
 */
public class JdxTaskWsClean extends JdxTaskCustom {

    //
    JdxReplWs ws;

    //
    public JdxTaskWsClean(JdxReplWs ws) {
        this.ws = ws;
        ws.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxTaskWsCrean");
    }

    //
    public void doTask() throws Exception {
        MDC.put("serviceName", "ws");

        //
        log.info("Рабочая станция, очистка старых реплик");
        try {
            ws.init();
        } catch (Exception e) {
            if (UtJdxErrors.errorIs_failedDatabaseLock(e)) {
                log.error("ws.init: " + e.getMessage());
                return;
            }
            throw e;
        }
        logInfo("Рабочая станция, wsId: " + ws.getWsId());


        // Проверка версии приложения (без обновления)
        ws.checkAppUpdate();


        //
        log.info("Выполнение очистки старых реплик");
        try {
            ws.wsCleanupRepl();
        } catch (Exception e) {
            logError(e);
            collectError("ws.wsCleanupRepl", e);
        }


        //
        logInfo("Отправка ошибок");
        IMailer mailer = ws.getMailer();
        sendErrors(mailer, "ws.errors");


        //
        logInfo("Рабочая станция, очистка старых реплик завершена");
        log.info("----------");

        //
        MDC.remove("serviceName");
    }

}
