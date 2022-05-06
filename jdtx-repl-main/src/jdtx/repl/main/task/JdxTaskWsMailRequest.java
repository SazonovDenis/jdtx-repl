package jdtx.repl.main.task;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import org.apache.commons.logging.*;

/**
 * Выполняет шаг "рассылка писем по запросу" для для рабочей станции.
 */
public class JdxTaskWsMailRequest extends JdxTaskCustom {

    //
    JdxReplWs ws;

    //
    public JdxTaskWsMailRequest(JdxReplWs ws) {
        this.ws = ws;
        ws.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxTaskWsMailRequest");
    }

    //
    public void doTask() throws Exception {
        //
        logInfo("Рабочая станция");
        ws.init();
        logInfo("Рабочая станция, wsId: " + ws.getWsId());


        // Проверка версии приложения
        ws.checkAppUpdate();


        //
        log.info("Рассылка по требованию");
        try {
            ws.replicasSend_Required();
        } catch (Exception e) {
            logError(e);
            collectError("ws.MailRequest", e);
        }


        //
        logInfo("Отправка ошибок");
        IMailer mailer = ws.getMailer();
        sendErrors(mailer, "ws.errors");


        //
        log.info("Рабочая станция, рассылка по требованию завершена");
    }

}
