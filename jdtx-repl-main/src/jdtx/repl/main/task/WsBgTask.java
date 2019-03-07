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
    protected static Log log = LogFactory.getLog("BgTask");


    //
    public String getCfgFileName() {
        return cfgFileName;
    }

    public void setCfgFileName(String cfgFileName) {
        this.cfgFileName = cfgFileName;
    }

    String cfgFileName;


    //
    public void run() throws Exception {
        try {
            step_ws();
        } catch (Exception e) {
            String msg;
            if (e.getCause() != null) {
                msg = e.getCause().getMessage();
            } else {
                msg = e.getMessage();
            }
            log.error("Рабочая станция: " + msg);
            e.printStackTrace();
        }
    }


    //
    void step_ws() throws Exception {
        log.info("Рабочая станция: " + cfgFileName);

        //
        ModelService app = getApp().service(ModelService.class);
        Db db = app.getModel().getDb();
        db.connect();

        //
        try {
            //
            log.info("Рабочая станция, настройка");
            JdxReplWs ws = new JdxReplWs(db);
            ws.init(cfgFileName);

            //
            log.info("Отслеживаем и обрабатываем свои изменения");
            ws.handleSelfAudit();

            //
            log.info("Отправляем свои изменения");
            ws.send();

            //
            log.info("Забираем входящие реплики");
            ws.receive();

            //
            log.info("Применяем входящие реплики");
            ws.handleQueIn();

            //
            log.info("Рабочая станция завершена");
        } finally {
            db.disconnect();
        }
    }


}
