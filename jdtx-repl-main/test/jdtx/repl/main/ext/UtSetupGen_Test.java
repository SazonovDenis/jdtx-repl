package jdtx.repl.main.ext;

import jandcode.app.test.*;
import org.junit.*;

/**
 */
public class UtSetupGen_Test extends AppTestCase {

    @Test
    public void test_rndHexStr() throws Exception {
        RandomString rnd = new RandomString();
        for (int i = 0; i < 20; i++) {
            String guid = rnd.nextHexStr(16);
            System.out.println(guid);
        }
    }

    @Test
    public void test_gen() throws Exception {
        UtSetup utSetup = new UtSetup();
        utSetup.app = app.getApp();

        String fileName = "D:\\t\\jantas\\ws_list.txt";
        utSetup.gen(fileName, "temp/");
    }

}
