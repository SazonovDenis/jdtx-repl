package jdtx.repl.main.task;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;

/**
 * Выполняет шаги "рассылка по расписанию" для сервера.
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
        log.info("Сервер, рассылка по расписанию");
        srv.init();

        //
        MDC.put("serviceName", "srvM");


        // Проверка версии приложения (без обновления)
        srv.checkAppUpdate();


        //
        log.info("Чтение входящих очередей");
        try {
            srv.srvHandleQueIn();
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvHandleQueIn", e);
        }


        //
        log.info("Рассылка исходящих очередей");
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
        log.info("Рассылка по расписанию завершена");
    }

}
