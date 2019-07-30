package jdtx.repl.main.ut;

import jandcode.app.test.*;
import jdtx.repl.main.ut.*;
import org.junit.*;

public class UtReplService_Test extends AppTestCase {


    @Test
    public void test_start() throws Exception {
        UtReplService.start();
    }

    @Test
    public void test_list() throws Exception {
        UtReplService.list();
    }

    @Test
    public void test_stop() throws Exception {
        UtReplService.stop();
    }

    @Test
    public void test_install() throws Exception {
        UtReplService.install();
    }

    @Test
    public void test_remove() throws Exception {
        UtReplService.remove();
    }

}
