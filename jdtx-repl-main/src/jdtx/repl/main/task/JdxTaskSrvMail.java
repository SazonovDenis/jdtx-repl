package jdtx.repl.main.task;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import org.apache.commons.logging.*;

/**
 * Выполняет шаги "чтение и отправка почты" для сервера.
 */
public class JdxTaskSrvMail extends JdxTaskCustom {

    //
    JdxReplSrv srv;

    //
    public JdxTaskSrvMail(JdxReplSrv srv) {
        this.srv = srv;
        srv.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxReplTaskSrv");
    }

    //
    public void doTask() throws Exception {
        //
        log.info("Сервер, рассылка");
        srv.init();


        // Проверка версии приложения (без обновления)
        srv.checkAppUpdate();


        //
        log.info("Сервер, чтение входящих очередей");
        try {
            srv.srvHandleQueIn();
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvHandleQueIn", e);
        }


        //
        log.info("Сервер, рассылка исходящих реплик");
        try {
            srv.srvReplicasSend();
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvReplicasSend", e);
        }


        //
        IMailer mailer = srv.getSelfMailer();
        logInfo("Отправка ошибок");
        sendErrors(mailer, "srv.errors.mail");


        //
        log.info("Сервер, рассылка завершена");
    }

}
