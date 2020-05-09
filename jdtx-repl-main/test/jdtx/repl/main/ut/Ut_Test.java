package jdtx.repl.main.ut;

import jandcode.app.test.*;
import org.junit.*;

/**
 */
public class Ut_Test extends AppTestCase {

    @Test
    public void test_tryParse() throws Exception {
        assertEquals(0, Ut.tryParseInteger(null));
        assertEquals(0, Ut.tryParseInteger(""));
        assertEquals(0, Ut.tryParseInteger("qwe"));
        assertEquals(0, Ut.tryParseInteger("1.2"));
        assertEquals(0, Ut.tryParseInteger("1,2"));
        assertEquals(0, Ut.tryParseInteger("100qwe"));
        assertEquals(0, Ut.tryParseInteger("1e3"));
        //
        assertEquals(0, Ut.tryParseInteger("0"));
        assertEquals(1, Ut.tryParseInteger("1"));
        assertEquals(-1, Ut.tryParseInteger("-1"));
        assertEquals(100, Ut.tryParseInteger("100"));
    }

}
