package jdtx.repl.main.task;

import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.mailer.*;

/**
 * BgTask - рабочая станция
 */
public class WsBgTask extends BgTaskCustom {


    //
    public boolean runImmediate;


    //
    public void run() throws Exception {
        runImmediate = false;

        //
        try {
            errors.clear();
            step_ws();
        } catch (Exception e) {
            logError(e, "Рабочая станция");
        }
    }


    //
    void step_ws() throws Exception {
        logInfo("Рабочая станция");

        //
        ModelService app = getApp().service(ModelService.class);
        Db db = app.getModel().getDb();
        db.connect();

        //
        try {
            logInfo("Рабочая станция, настройка");
            JdxReplWs ws = new JdxReplWs(db);
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
        } finally {
            db.disconnect();
        }
    }


}
