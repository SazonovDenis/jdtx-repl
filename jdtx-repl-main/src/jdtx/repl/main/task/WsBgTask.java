package jdtx.repl.main.task;

import jandcode.bgtasks.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import org.apache.commons.logging.*;

/**
 * BgTask - рабочая станция
 */
public class WsBgTask extends BgTask {


    private static Log log = LogFactory.getLog("jdtx.WsBgTask");

    //
    public boolean runImmediate;


    //
    public void run() throws Exception {
        runImmediate = false; // todo: моэет это лучше в jdtx.repl.main.task.JdxTaskPriorityChoicer.choiceNextTask ?

        //
        try {
            log.info("Рабочая станция");
            step_ws();
        } catch (Exception e) {
            log.error(e);
        }
    }


    //
    private void step_ws() throws Exception {
        ModelService app = getApp().service(ModelService.class);
        Db db = app.getModel().getDb();
        db.connect();

        //
        try {
            JdxReplWs ws = new JdxReplWs(db);
            //
            JdxReplTaskWs replTask = new JdxReplTaskWs(ws);
            //
            replTask.doReplSession();
        } finally {
            db.disconnect();
        }
    }


}
