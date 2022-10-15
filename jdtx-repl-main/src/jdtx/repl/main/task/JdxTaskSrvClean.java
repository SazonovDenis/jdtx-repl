package jdtx.repl.main.task;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;

/**
 * Выполняет шаги "очистка старых реплик" для сервера.
 */
public class JdxTaskSrvClean extends JdxTaskCustom {

    //
    JdxReplSrv srv;

    //
    public JdxTaskSrvClean(JdxReplSrv srv) {
        this.srv = srv;
        srv.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxTaskSrvClean");
    }

    //
    public void doTask() throws Exception {
        MDC.put("serviceName", "srv");

        //
        log.info("Сервер, очистка старых реплик");
        srv.init();


        // Проверка версии приложения (без обновления)
        srv.checkAppUpdate();


        //
        log.info("Выполнение очистки старых реплик");
        try {
            srv.srvCleanupRepl();
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvCleanupRepl", e);
        }


        //
        IMailer mailer = srv.getSelfMailer();
        logInfo("Отправка ошибок");
        sendErrors(mailer, "srv.errors");


        //
        log.info("Сервер, очистка старых реплик завершена");
        log.info("----------");

        //
        MDC.remove("serviceName");
    }

}
