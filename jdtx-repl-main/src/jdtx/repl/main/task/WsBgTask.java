package jdtx.repl.main.task;

import jandcode.bgtasks.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

/**
 */
public class WsBgTask extends BgTask {


    //
    protected static Log log = LogFactory.getLog("bgTask");


    //
    public String getCfgFileName() {
        return cfgFileName;
    }

    public void setCfgFileName(String cfgFileName) {
        this.cfgFileName = cfgFileName;
    }

    String cfgFileName;

    public boolean runImmediate;


    //
    public void run() throws Exception {
        runImmediate = false;

        try {
            step_ws();
        } catch (Exception e) {
            logError(e, "Рабочая станция: ");
        }
    }


    //
    void step_ws() throws Exception {
        logInfo("Рабочая станция, настройка: " + cfgFileName);

        //
        ModelService app = getApp().service(ModelService.class);
        Db db = app.getModel().getDb();
        db.connect();

        //
        try {
            JdxReplWs ws = new JdxReplWs(db);
            ws.init(cfgFileName);
            logInfo("Рабочая станция, wsId: " + ws.getWsId());

            //
            logInfo("Отслеживаем и обрабатываем свои изменения");
            try {
                ws.handleSelfAudit();
            } catch (Exception e) {
                logError(e);
            }

            //
            logInfo("Отправляем свои реплики");
            try {
                ws.send();
            } catch (Exception e) {
                logError(e);
            }

            //
            logInfo("Забираем входящие реплики");
            try {
                ws.receive();
            } catch (Exception e) {
                logError(e);
            }

            //
            logInfo("Применяем входящие реплики");
            try {
                ws.handleQueIn();
            } catch (Exception e) {
                logError(e);
            }

            //
            logInfo("Рабочая станция завершена");
        } finally {
            db.disconnect();
        }
    }


    void logInfo(String info) {
        log.info(info);
        getLogger().put("state", info);
    }

    void logError(Exception e, String info) {
        getLogger().put("state", info + Ut.getExceptionMessage(e));
        log.error(info + Ut.getExceptionMessage(e));
        log.error(Ut.getStackTrace(e));
    }

    void logError(Exception e) {
        logError(e, "");
    }

}
