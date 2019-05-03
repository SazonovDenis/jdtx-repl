package jdtx.repl.main;

import jandcode.app.test.*;
import jdtx.repl.main.ext.*;
import org.junit.*;

public class ProcessListTest extends AppTestCase {


    @Test
    public void test_start() throws Exception {
        ProcessList.start();
    }

    @Test
    public void test_list() throws Exception {
        ProcessList.list();
    }

    @Test
    public void test_stop() throws Exception {
        ProcessList.stop();
    }

    @Test
    public void test_install() throws Exception {
        ProcessList.install();
    }

}
