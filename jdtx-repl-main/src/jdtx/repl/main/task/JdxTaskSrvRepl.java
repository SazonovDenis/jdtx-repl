package jdtx.repl.main.task;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import org.apache.commons.logging.*;

/**
 * Выполняет шаги "обработка реплик" для сервера.
 */
public class JdxTaskSrvRepl extends JdxTaskCustom {

    //
    JdxReplSrv srv;

    //
    public JdxTaskSrvRepl(JdxReplSrv srv) {
        this.srv = srv;
        srv.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxReplTaskSrv");
    }

    //
    public void doTask() throws Exception {
        //
        log.info("Сервер, обработка реплик");
        srv.init();


        // Проверка версии приложения (без обновления)
        srv.checkAppUpdate();


        //
        log.info("Предварительные шаги");
        try {
            srv.srvHandleRoutineTaskOut();
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvHandleRoutineTask", e);
        }


        //
        log.info("Формирование общей очереди");
        try {
            srv.srvHandleCommonQue();
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvHandleCommonQue", e);
        }


        //
        log.info("Тиражирование реплик");
        try {
            srv.srvReplicasDispatch();
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvReplicasDispatch", e);
        }


        //
        IMailer mailer = srv.getSelfMailer();
        logInfo("Отправка ошибок");
        sendErrors(mailer, "srv.errors");


        //
        log.info("Сервер, обработка реплик завершена");
    }

}
