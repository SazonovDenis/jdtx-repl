package jdtx.repl.main.task;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;

/**
 * Выполняет шаги "удаление старых реплик" для сервера.
 */
public class JdxTaskSrvCleanupRepl extends JdxTaskCustom {

    //
    JdxReplSrv srv;

    //
    public JdxTaskSrvCleanupRepl(JdxReplSrv srv) {
        this.srv = srv;
        srv.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxTaskSrvCleanupRepl");
    }

    //
    public void doTask() throws Exception {
        MDC.put("serviceName", "srvCl");

        //
        log.info("Сервер, удаление старых реплик");
        srv.init();


        // Проверка версии приложения (без обновления)
        srv.checkAppUpdate();


        //
        log.info("Выполнение удаления старых реплик");
        try {
            srv.srvCleanupReplWs(Long.MAX_VALUE);
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvCleanupRepl", e);
        }


        //
        IMailer mailer = srv.getSelfMailer();
        logInfo("Отправка ошибок");
        sendErrors(mailer, "srv.errors");


        //
        log.info("Сервер, удаление старых реплик завершено");
        log.info("----------");

        //
        MDC.remove("serviceName");
    }

}
