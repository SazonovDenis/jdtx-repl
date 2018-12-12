package jdtx.repl.main;

import jandcode.app.test.*;
import org.junit.*;

public class ModuleTest extends AppTestCase {

    {
        logSetUp = true;
    }

    @Test
    public void test1() throws Exception {
        app.saveAppRt();
    }
}
