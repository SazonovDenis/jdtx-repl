package jdtx.repl.main.task;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;
import org.apache.commons.logging.*;

/**
 * Выполняет шаги "сеанс репликации" для рабочей станции.
 */
public class JdxTaskWsRepl extends JdxTaskCustom {

    //
    JdxReplWs ws;

    //
    public JdxTaskWsRepl(JdxReplWs ws) {
        this.ws = ws;
        ws.errorCollector = errorCollector;
        log = LogFactory.getLog("jdtx.JdxReplTaskWs");
    }

    //
    public void doTask() throws Exception {
        //
        logInfo("Рабочая станция");
        ws.init();
        logInfo("Рабочая станция, wsId: " + ws.getWsId());

        // Проверка версии приложения, обновление при необходимости
        ws.doAppUpdate();

        //
        logInfo("Проверяем аварийную ситуацию");
        try {
            ws.repairAfterBackupRestore(false, false);
        } catch (Exception e) {
            logError(e);
            collectError("ws.repairAfterBackupRestore", e);

            //
            logInfo("Отправка ошибок");
            IMailer mailer = ws.getMailer();
            sendErrors(mailer, "ws.errors");

            //
            logInfo("Рабочая станция прервана");
            return;
        }

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
            ws.replicasSend();
        } catch (Exception e) {
            logError(e);
            collectError("ws.send", e);
        }

        //
        logInfo("Забираем входящие реплики");
        try {
            ws.replicasReceive();
        } catch (Exception e) {
            logError(e);
            collectError("ws.receive", e);
        }

        //
        logInfo("Применяем входящие реплики");
        try {
            ws.handleAllQueIn();
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