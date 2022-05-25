package jdtx.repl.main.task;

import jandcode.bgtasks.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

/**
 * Task, рабочая станция, рассылка писем по запросу
 */
public class BgTaskWsMailRequest extends BgTask {

    //
    private static Log log = LogFactory.getLog("jdtx.BgTaskWsMailRequest");

    //
    public void run() throws Exception {
        try {
            log.info("Рабочая станция, рассылка по требованию");
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
            JdxReplWs ws = new JdxReplWs(db);
            //
            JdxTaskWsMailRequest mailTask = new JdxTaskWsMailRequest(ws);
            //
            mailTask.doTask();
        } finally {
            db.disconnect();
        }
    }


}