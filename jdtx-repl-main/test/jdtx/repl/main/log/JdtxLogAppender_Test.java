package jdtx.repl.main.log;

import jdtx.repl.main.api.*;
import jdtx.repl.main.task.*;
import org.apache.commons.logging.*;
import org.junit.*;

public class JdtxLogAppender_Test extends DbPrepareEtalon_Test {

    protected static Log log = LogFactory.getLog("JdtxLogAppender_Test");

    public void setUp() throws Exception {
        rootDir = "../ext/";
        super.setUp();
        db.connect();
    }

    @Test
    public void test_0() throws Exception {
        logOn();

        int x = 1;
        while (x < 10) {
            log.error("Event ERROR");
            log.warn("Event WARN");
            log.info("Event INFO");
            //
            x = x + 1;
            //
            Thread.sleep(1000);
        }
    }

    @Test
    public void test_BgTaskLogHttp_loop() throws Exception {
        logOn();

        //
        JdxReplWs ws = new JdxReplWs(db);
        ws.init();
        JdxTaskLogHttp replTask = new JdxTaskLogHttp(ws.getMailer());

        //
        int x = 1;
        while (x < 100) {
            replTask.doTask();
            System.out.println("replTask.doTask");
            //
            x = x + 1;
            //
            Thread.sleep(1000);
        }
    }

}
