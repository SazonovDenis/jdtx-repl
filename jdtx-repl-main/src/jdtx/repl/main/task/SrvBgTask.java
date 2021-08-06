package jdtx.repl.main.task;

import jandcode.bgtasks.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

/**
 * BgTask - сервер
 */
public class SrvBgTask extends BgTask {

    //
    private static Log log = LogFactory.getLog("jdtx.SrvBgTask");

    //
    public void run() throws Exception {
        try {
            log.info("Сервер");
            step_server();
        } catch (Exception e) {
            log.error(Ut.getExceptionMessage(e));
            log.error(Ut.getStackTrace(e));
        }
    }


    //
    private void step_server() throws Exception {
        ModelService app = getApp().service(ModelService.class);
        Db db = app.getModel().getDb();
        db.connect();

        //
        try {
            JdxReplSrv srv = new JdxReplSrv(db);
            //
            JdxReplTaskSrv replTask = new JdxReplTaskSrv(srv);
            //
            replTask.doReplSession();
        } finally {
            db.disconnect();
        }
    }


}
