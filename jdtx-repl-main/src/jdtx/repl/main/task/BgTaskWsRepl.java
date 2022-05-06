package jdtx.repl.main.task;

import jandcode.bgtasks.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

/**
 * BgTask - рабочая станция
 */
public class BgTaskWsRepl extends BgTask {


    private static Log log = LogFactory.getLog("jdtx.BgTaskWsRepl");

    //
    public boolean runImmediate;


    //
    public void run() throws Exception {
        runImmediate = false; // todo: моэет это лучше в jdtx.repl.main.task.JdxTaskPriorityChoicer.choiceNextTask ?

        //
        try {
            log.info("Рабочая станция");
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
            JdxTaskWsRepl replTask = new JdxTaskWsRepl(ws);
            //
            replTask.doTask();
        } finally {
            db.disconnect();
        }
    }


}
