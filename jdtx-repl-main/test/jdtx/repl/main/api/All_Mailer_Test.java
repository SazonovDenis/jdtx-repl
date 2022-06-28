package jdtx.repl.main.api;

import jandcode.app.test.*;
import org.junit.*;

/**
 * Предрелизные проверки - все должны проходить
 */
public class All_Mailer_Test extends AppTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    @Test
    public void test_NoDeleteIfNotAll() throws Exception {
        JdxReplWsSrv_Mailer_Delete_Test test = new JdxReplWsSrv_Mailer_Delete_Test();
        test.setUp();
        test.test_NoDeleteIfNotAll();
    }

    @Test
    public void test_doRequired() throws Exception {
        JdxReplWsSrv_Mailer_Required_Test test = new JdxReplWsSrv_Mailer_Required_Test();
        test.setUp();
        test.test_doRequired();
    }

}
