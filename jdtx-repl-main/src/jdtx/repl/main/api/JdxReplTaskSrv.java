package jdtx.repl.main.api;

import jdtx.repl.main.api.mailer.*;
import org.apache.commons.logging.*;

/**
 * Выполняет шаги "сеанс обработки реплик" для сервера.
 */
public class JdxReplTaskSrv extends JdxReplTaskCustom {

    //
    JdxReplSrv srv;

    //
    public JdxReplTaskSrv(JdxReplSrv srv) {
        this.srv = srv;
        log = LogFactory.getLog("jdtx.JdxReplTaskSrv");
    }

    //
    public void doReplSesssion() throws Exception {
        //
        log.info("Сервер, настройка");
        srv.init();

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
            srv.srvDispatchReplicas();
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvDispatchReplicas", e);
        }

        //
        IMailer mailer = srv.getMailer();
        logInfo("Отправка ошибок");
        sendErrors(mailer, "srv.errors");

        //
        log.info("Сервер завершен");
    }

}
