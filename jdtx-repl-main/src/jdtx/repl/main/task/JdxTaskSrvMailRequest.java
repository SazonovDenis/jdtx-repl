package jdtx.repl.main.task;

import com.jdtx.state.StateItemStackNamed;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.log.JdtxStateContainer;
import org.apache.commons.logging.*;
import org.apache.log4j.*;

/**
 * Выполняет шаг "рассылка по требованию" для сервера.
 */
public class JdxTaskSrvMailRequest extends JdxTaskCustom {

    //
    JdxReplSrv srv;

    //
    protected static StateItemStackNamed state = JdtxStateContainer.state;

    //
    public JdxTaskSrvMailRequest(JdxReplSrv srv) {
        this.srv = srv;
        srv.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxTaskSrvMailRequest");
    }

    //
    public void doTask() throws Exception {
        MDC.put("serviceName", "srvMR");

        state.start("srvMR");
        try {
            //
            log.info("Сервер, рассылка по требованию");
            srv.init();

            // Проверка версии приложения (без обновления)
            srv.checkAppUpdate();


            //
            log.info("Выполнение рассылки по требованию");
            try {
                srv.replicasSend_Requied();
            } catch (Exception e) {
                logError(e);
                collectError("srv.MailRequest", e);
            }


            //
            logInfo("Отправка ошибок");
            IMailer mailer = srv.getSelfMailer();
            sendErrors(mailer, "srv.errors.mailRequest");


            //
            log.info("Сервер, рассылка по требованию завершена");
            log.info("----------");

            //
            MDC.remove("serviceName");

        } finally {
            state.stop();
        }
    }

}
