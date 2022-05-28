package jdtx.repl.main.api.log;

import jandcode.app.test.*;
import jandcode.utils.*;
import jdtx.repl.main.task.*;
import org.apache.commons.logging.*;
import org.junit.*;

public class JdtxLogAppender_Test extends AppTestCase {

    protected static Log log = LogFactory.getLog("JdtxLogAppender_Test");

    public void setUp() throws Exception {
        super.setUp();
        if (UtFile.exists("../_log.properties")) {
            UtLog.loadProperties("../_log.properties") ;
            UtLog.logOn() ;
        } else if (UtFile.exists("../log.properties")) {
            UtLog.loadProperties("../log.properties") ;
            UtLog.logOn();
        } else {
            throw new Exception("Файл log.properties или _log.properties не найден");
        }
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
    public void test_BgTaskLogHttp() throws Exception {
        logOn();

        BgTaskLogHttp taskLogHttp = new BgTaskLogHttp();
        taskLogHttp.setApp(app.getApp());

        int x = 1;
        while (x < 100) {
            taskLogHttp.run();
            //
            x = x + 1;
            //
            Thread.sleep(1000);
        }
    }

}
