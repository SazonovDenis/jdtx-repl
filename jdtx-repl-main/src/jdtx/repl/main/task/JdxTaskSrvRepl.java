package jdtx.repl.main.task;

import com.jdtx.state.StateItemStackNamed;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.log.JdtxStateContainer;
import org.apache.commons.logging.*;
import org.apache.log4j.*;

/**
 * Выполняет шаги "обработка реплик" для сервера.
 */
public class JdxTaskSrvRepl extends JdxTaskCustom {

    //
    JdxReplSrv srv;

    //
    protected static StateItemStackNamed state = JdtxStateContainer.state;

    //
    public JdxTaskSrvRepl(JdxReplSrv srv) {
        this.srv = srv;
        srv.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxReplTaskSrv");
    }

    //
    public void doTask() throws Exception {
        MDC.put("serviceName", "srv");

        state.start("srv");
        try {
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
            try {
                if (errorCollector.getErrors().size() == 0) {
                    log.info("Очистка почтовых ящиков");
                    srv.srvCleanupMailInBox();
                } else {
                    log.info("Очистка почтовых ящиков пропущена");
                }
            } catch (Exception e) {
                logError(e);
                collectError("srv.srvCleanupMailInBox", e);
            }


            //
            IMailer mailer = srv.getSelfMailer();
            logInfo("Отправка ошибок");
            sendErrors(mailer, "srv.errors");


            //
            log.info("Сервер, обработка реплик завершена");
            log.info("----------");

            //
            MDC.remove("serviceName");

        } finally {
            state.stop();
        }
    }

}
