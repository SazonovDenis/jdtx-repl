package jdtx.repl.main.task;

import jandcode.bgtasks.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

/**
 * Task - сервер, рассылка писем
 */
public class BgTaskSrvMail extends BgTask {

    //
    private static Log log = LogFactory.getLog("jdtx.BgTaskSrvMail");

    //
    public void run() throws Exception {
        try {
            log.info("Сервер");
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
            JdxTaskSrvRepl replTask = new JdxTaskSrvRepl(srv);
            //
            replTask.doTask();
        } finally {
            db.disconnect();
        }
    }


}
