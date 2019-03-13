package jdtx.repl.main.task;

import jandcode.bgtasks.BgTask;
import jandcode.dbm.ModelService;
import jandcode.dbm.db.Db;
import jdtx.repl.main.api.JdxReplWs;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
            String msg;
            if (e.getCause() != null) {
                msg = e.getCause().getMessage();
            } else {
                msg = e.getMessage();
            }
            getLogger().put("state", "Рабочая станция: " + msg);
            log.error("Рабочая станция: " + msg);
            e.printStackTrace();
        }
    }


    //
    void step_ws() throws Exception {
        logInfo("Рабочая станция: " + cfgFileName);

        //
        ModelService app = getApp().service(ModelService.class);
        Db db = app.getModel().getDb();
        db.connect();

        //
        try {
            logInfo("Рабочая станция, настройка");
            JdxReplWs ws = new JdxReplWs(db);
            ws.init(cfgFileName);

            //
            logInfo("Отслеживаем и обрабатываем свои изменения");
            ws.handleSelfAudit();

            //
            logInfo("Отправляем свои изменения");
            ws.send();

            //
            logInfo("Забираем входящие реплики");
            ws.receive();

            //
            logInfo("Применяем входящие реплики");
            ws.handleQueIn();

            //
            logInfo("Рабочая станция завершена");
        } finally {
            db.disconnect();
        }
    }

    void logInfo(String s) {
        log.info(s);
        getLogger().put("state", s);
    }

}
