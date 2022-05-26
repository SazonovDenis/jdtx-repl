package jdtx.repl.main.task;

import jandcode.bgtasks.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

/**
 * Фоновая задача: сервер, рассылка по требованию
 */
public class BgTaskSrvMailRequest extends BgTask {

    //
    private static Log log = LogFactory.getLog("jdtx.BgTaskSrvMailRequest");

    //
    public void run() throws Exception {
        try {
            log.info("Сервер, рассылка по требованию");
            step();
        } catch (Exception e) {
            log.error(Ut.getExceptionMessage(e));
            log.error(Ut.getStackTrace(e));
        }
    }


    //
    private void step() throws Exception {
        ModelService app = getApp().service(ModelService.class);
        Db db = app.getModel().getDb();
        db.connect();

        //
        try {
            JdxReplSrv srv = new JdxReplSrv(db);
            //
            JdxTaskSrvMailRequest task = new JdxTaskSrvMailRequest(srv);
            //
            task.doTask();
        } finally {
            db.disconnect();
        }
    }


}
