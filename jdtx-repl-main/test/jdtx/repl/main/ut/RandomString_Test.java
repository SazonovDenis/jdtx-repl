package jdtx.repl.main.ut;

import jandcode.app.test.*;
import org.junit.*;

/**
 */
public class RandomString_Test extends AppTestCase {

    @Test
    public void test_rndHexStr() throws Exception {
        RandomString rnd = new RandomString();
        for (int i = 0; i < 20; i++) {
            String guid = rnd.nextHexStr(16);
            System.out.println(guid);
        }
    }

}
