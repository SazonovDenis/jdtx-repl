package jdtx.repl.main.task;

import com.jdtx.state.StateItemStackNamed;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.log.JdtxStateContainer;
import org.apache.commons.logging.*;
import org.apache.log4j.*;

/**
 * Выполняет шаги "рассылка по расписанию" для сервера.
 */
public class JdxTaskSrvMail extends JdxTaskCustom {

    //
    JdxReplSrv srv;

    //
    protected static StateItemStackNamed state = JdtxStateContainer.state;

    //
    public JdxTaskSrvMail(JdxReplSrv srv) {
        this.srv = srv;
        srv.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxReplTaskSrv");
    }

    //
    public void doTask() throws Exception {
        MDC.put("serviceName", "srvM");

        state.start("srvM");
        try {
            //
            log.info("Сервер, рассылка по расписанию");
            srv.init();


            // Проверка версии приложения (без обновления)
            srv.checkAppUpdate();


            //
            log.info("Предварительные шаги");
            try {
                srv.srvHandleRoutineTaskIn();
            } catch (Exception e) {
                logError(e);
                collectError("srv.srvHandleRoutineTaskIn", e);
            }


            //
            log.info("Чтение входящих очередей");
            try {
                srv.srvReplicasReceive();
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
            log.info("Сервер, рассылка по расписанию завершена");
            log.info("----------");

            //
            MDC.remove("serviceName");

        } finally {
            state.stop();
        }
    }

}
