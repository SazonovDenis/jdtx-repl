package jdtx.repl.main.task;

import com.jdtx.state.StateItemStackNamed;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.util.*;
import jdtx.repl.main.log.JdtxStateContainer;
import org.apache.commons.logging.*;
import org.apache.log4j.*;

/**
 * Выполняет шаг "рассылка по требованию" для рабочей станции.
 */
public class JdxTaskWsMailRequest extends JdxTaskCustom {

    //
    JdxReplWs ws;

    //
    protected static StateItemStackNamed state = JdtxStateContainer.state;

    //
    public JdxTaskWsMailRequest(JdxReplWs ws) {
        this.ws = ws;
        ws.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxTaskWsMailRequest");
    }

    //
    public void doTask() throws Exception {
        MDC.put("serviceName", "wsMR");

        state.start("wsMR");
        try {
            //
            logInfo("Рабочая станция, рассылка по требованию");
            try {
                ws.init();
            } catch (Exception e) {
                if (UtJdxErrors.errorIs_failedDatabaseLock(e)) {
                    log.error("ws.init: " + e.getMessage());
                    return;
                }
                throw e;
            }
            logInfo("Рабочая станция, wsId: " + ws.getWsId());


            // Проверка версии приложения (без обновления)
            ws.checkAppUpdate();


            //
            log.info("Выполнение рассылки по требованию");
            try {
                ws.replicasSend_Required();
            } catch (Exception e) {
                logError(e);
                collectError("ws.MailRequest", e);
            }


            //
            logInfo("Отправка ошибок");
            IMailer mailer = ws.getMailer();
            sendErrors(mailer, "ws.errors.mailRequest");


            //
            log.info("Рабочая станция, рассылка по требованию завершена");
            log.info("----------");

            //
            MDC.remove("serviceName");

        } finally {
            state.stop();
        }
    }

}
