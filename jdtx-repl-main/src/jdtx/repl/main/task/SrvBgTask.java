package jdtx.repl.main.task;

import jandcode.bgtasks.BgTask;
import jandcode.dbm.ModelService;
import jandcode.dbm.db.Db;
import jdtx.repl.main.api.JdxReplSrv;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 */
public class SrvBgTask extends BgTask {


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


    //
    public void run() throws Exception {
        try {
            step_server();
        } catch (Exception e) {
            String msg = Ut.getExceptionMessage(e);
            log.error("Сервер: " + msg);
            e.printStackTrace();
        }
    }


    //
    void step_server() throws Exception {
        log.info("Сервер: " + cfgFileName);

        //
        ModelService app = getApp().service(ModelService.class);
        Db db = app.getModel().getDb();
        db.connect();

        //
        try {
            //
            log.info("Сервер, настройка");
            JdxReplSrv srv = new JdxReplSrv(db);
            srv.init();

            //
            log.info("Формирование общей очереди");
            srv.srvHandleCommonQue();

            //
            log.info("Тиражирование реплик");
            srv.srvDispatchReplicas();

            //
            log.info("Сервер завершен");
        } finally {
            db.disconnect();
        }
    }


}
