package jdtx.repl.main.task;

import jandcode.bgtasks.*;
import jandcode.dbm.*;
import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.log.*;
import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.ut.*;
import org.apache.commons.logging.*;

import java.util.*;

/**
 * Фоновая задача: логирование на сервер
 */
public class BgTaskLogHttp extends BgTask {

    //
    private static Log log = LogFactory.getLog("jdtx.BgTaskSrvMail");

    //
    private IMailer mailer = null;

    //
    public void run() throws Exception {
        try {
            // Мейлер
            IMailer mailer = getMailer();
            // logValue
            String logValue = JdtxLogAppender.getLogValue();
            Map data = new HashMap();
            data.put("logValue", logValue);
            mailer.setData(data, "log.log", "from");
        } catch (Exception e) {
            e.printStackTrace();
            log.error(Ut.getExceptionMessage(e));
            log.error(Ut.getStackTrace(e));
        }
    }


    private IMailer getMailer() throws Exception {
        if (mailer == null) {
            ModelService app = getApp().service(ModelService.class);
            Db db = app.getModel().getDb();
            db.connect();

            //
            try {
                JdxReplWs ws = new JdxReplWs(db);
                ws.init();
                mailer = ws.getMailer();
            } finally {
                db.disconnect();
            }
        }
        return mailer;
    }


}
