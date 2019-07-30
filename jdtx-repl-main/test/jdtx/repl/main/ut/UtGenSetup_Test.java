package jdtx.repl.main.ut;

import jandcode.app.test.*;
import org.junit.*;

/**
 */
public class UtGenSetup_Test extends AppTestCase {

    @Test
    public void test_gen() throws Exception {
        UtGenSetup utGenSetup = new UtGenSetup();
        utGenSetup.app = app.getApp();

        String fileName = "res:jdtx/repl/main/ut/UtGenSetup.ws_list.txt";
        utGenSetup.gen(fileName, "temp/");
    }

}
