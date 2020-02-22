package jdtx.repl.main.api;

import jdtx.repl.main.api.mailer.*;
import org.apache.commons.logging.*;

/**
 * Выполняет шаги "сеанс репликации" для рабочей станции.
 */
public class JdxReplTaskWs extends JdxReplTaskCustom {

    //
    JdxReplWs ws;
    //public BgTasksLogger logger;

    //
    public JdxReplTaskWs(JdxReplWs ws) {
        this.ws = ws;
        log = LogFactory.getLog("jdtx.JdxReplTaskWs");
    }

    //
    public void doReplSesssion() throws Exception {
        //
        logInfo("Рабочая станция, настройка");
        ws.init();
        logInfo("Рабочая станция, wsId: " + ws.getWsId());

        //
        logInfo("Отслеживаем и обрабатываем свои изменения");
        try {
            ws.handleSelfAudit();
        } catch (Exception e) {
            logError(e);
            collectError("ws.handleSelfAudit", e);
        }

        //
        logInfo("Отправляем свои реплики");
        try {
            ws.send();
        } catch (Exception e) {
            logError(e);
            collectError("ws.send", e);
        }

        //
        logInfo("Забираем входящие реплики");
        try {
            ws.receive();
        } catch (Exception e) {
            logError(e);
            collectError("ws.receive", e);
        }

        //
        logInfo("Применяем входящие реплики");
        try {
            ws.handleQueIn();
        } catch (Exception e) {
            logError(e);
            collectError("ws.handleQueIn", e);
        }

        //
        logInfo("Отправка ошибок");
        IMailer mailer = ws.getMailer();
        sendErrors(mailer, "ws.errors");

        //
        logInfo("Рабочая станция завершена");
    }

}
