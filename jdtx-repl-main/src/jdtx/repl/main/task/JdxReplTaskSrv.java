package jdtx.repl.main.task;

import jdtx.repl.main.api.*;
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
        srv.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxReplTaskSrv");
    }

    //
    public void doTask() throws Exception {
        //
        log.info("Сервер");
        srv.init();


        // Проверка версии приложения
        srv.checkAppUpdate();


        //
        log.info("Сервер, предварительные шаги");
        try {
            srv.srvHandleRoutineTask();
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvHandleRoutineTask", e);
        }


        //
        log.info("Чтение входящих очередей");
        try {
            srv.srvHandleQueIn();
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvHandleCommonQue", e);
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
        log.info("Рассылка реплик");
        try {
            srv.srvReplicasSendMail();
        } catch (Exception e) {
            logError(e);
            collectError("srv.srvReplicasSendMail", e);
        }


        //
        IMailer mailer = srv.getSelfMailer();
        logInfo("Отправка ошибок");
        sendErrors(mailer, "srv.errors");


        //
        log.info("Сервер завершен");
    }

}
