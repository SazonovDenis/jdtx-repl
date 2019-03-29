package jdtx.repl.main.task;

import jandcode.bgtasks.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
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
            logError("Рабочая станция: " + extractMsg(e));
            e.printStackTrace();
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
                logError(extractMsg(e));
            }

            //
            logInfo("Отправляем свои реплики");
            try {
                ws.send();
            } catch (Exception e) {
                logError(extractMsg(e));
            }

            //
            logInfo("Забираем входящие реплики");
            try {
                ws.receive();
            } catch (Exception e) {
                logError(extractMsg(e));
            }

            //
            logInfo("Применяем входящие реплики");
            try {
                ws.handleQueIn();
            } catch (Exception e) {
                logError(extractMsg(e));
            }

            //
            logInfo("Рабочая станция завершена");
        } finally {
            db.disconnect();
        }
    }

    String extractMsg(Exception e) {
        String msg;
        if (e.getCause() != null) {
            msg = e.getCause().getMessage();
        } else {
            msg = e.getMessage();
        }
        return msg;
    }

    void logInfo(String s) {
        log.info(s);
        getLogger().put("state", s);
    }

    void logError(String s) {
        log.error(s);
        getLogger().put("state", s);
    }

}
