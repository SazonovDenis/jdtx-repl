package jdtx.repl.main.log;

import jandcode.app.test.*;
import jandcode.utils.*;
import jandcode.utils.error.*;
import org.apache.commons.logging.*;
import org.apache.log4j.*;
import org.junit.*;

public class Mdc_Test extends AppTestCase {

    protected static Log log = LogFactory.getLog("jdtx.Mdc_Test");

    public void setUp() throws Exception {
        super.setUp();
        if (UtFile.exists("../_log.properties")) {
            UtLog.loadProperties("../_log.properties");
            System.out.println("../_log.properties");
        } else if (UtFile.exists("../log.properties")) {
            UtLog.loadProperties("../log.properties");
            System.out.println("../log.properties");
        } else {
            throw new XError("Файл log.properties или _log.properties не найден, логирование отключено");
        }
        logOn();
    }

    @Test
    public void test_0() throws Exception {
        MDC.put("serviceName", "ws");

        int x = 1;
        while (x < 10E10) {
            for (int i = 0; i < 100; i++) {
                log.error("Event ERROR");
                log.warn("Event WARN");
                log.info("Event INFO");
                //
                x = x + 1;
                //
                Thread.sleep(1);
            }
            log.info("Event INFO ======");

            Thread.sleep(500);
        }
    }

    @Test
    public void test_1() throws Exception {
        MDC.put("serviceName", "srv");

        int x = 1;
        while (x < 10E10) {
            for (int i = 0; i < 100; i++) {
                log.error("Event ERROR");
                log.warn("Event WARN");
                log.info("Event INFO");
                //
                x = x + 1;
                //
                Thread.sleep(1);
            }
            log.info("Event INFO ======");

            Thread.sleep(510);
        }
    }

}
