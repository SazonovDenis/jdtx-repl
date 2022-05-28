package jdtx.repl.main.task;

import jandcode.bgtasks.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

/**
 * Фоновая задача: логирование на сервер
 */
public class BgTaskLogHttp extends BgTask {

    //
    private static Log log = LogFactory.getLog("jdtx.BgTaskSrvMail");

    //
    private JdxTaskLogHttp taskLogHttp = null;

    //
    public void run() throws Exception {
        try {
            // Мейлер
            JdxTaskLogHttp task = getTaskLogHttp();
            task.doTask();
        } catch (Exception e) {
            e.printStackTrace();
            log.error(Ut.getExceptionMessage(e));
            log.error(Ut.getStackTrace(e));
        }
    }


    private JdxTaskLogHttp getTaskLogHttp() throws Exception {
        if (taskLogHttp == null) {
            ModelService app = getApp().service(ModelService.class);
            Db db = app.getModel().getDb();
            db.connect();

            //
            try {
                JdxReplWs ws = new JdxReplWs(db);
                ws.init();
                taskLogHttp = new JdxTaskLogHttp(ws);
            } finally {
                db.disconnect();
            }
        }
        return taskLogHttp;
    }


}
