package jdtx.repl.main.api.decoder;

import jdtx.repl.main.api.*;
import org.json.simple.*;
import org.junit.*;

/**
 *
 */
// todo: тест сломался
public class RefDecoderCheck_Test extends ReplDatabaseStruct_Test {


    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_checkValid() throws Exception {
        String jsonFileName = "test/jdtx/repl/main/api/decoder/decode_strategy.json";
        JSONObject cfgDecode = UtRepl.loadAndValidateJsonFile(jsonFileName);
        RefDecodeStrategy strategy = new RefDecodeStrategy();
        strategy.init(cfgDecode);

        //
        System.out.println("file: " + jsonFileName);
        UtDecodeStrategy.checkValid(strategy, struct);

        //
        System.out.println();
        //
        assertEquals(false, strategy.needDecode("Lic".toUpperCase(), 1, 0L));
        assertEquals(true, strategy.needDecode("Lic".toUpperCase(), 1, 10L));
        assertEquals(true, strategy.needDecode("Lic".toUpperCase(), 1, 1000L));
        assertEquals(true, strategy.needDecode("Lic".toUpperCase(), 1, 1001L));
        //
        assertEquals(false, strategy.needDecode("Lic".toLowerCase(), 1, 0L));
        assertEquals(true, strategy.needDecode("Lic".toLowerCase(), 1, 10L));
        assertEquals(true, strategy.needDecode("Lic".toLowerCase(), 1, 1000L));
        assertEquals(true, strategy.needDecode("Lic".toLowerCase(), 1, 1001L));

        //
        assertEquals(false, strategy.needDecode("CommentTip".toUpperCase(), 1, 0L));
        assertEquals(false, strategy.needDecode("CommentTip".toUpperCase(), 1, 10L));
        assertEquals(true, strategy.needDecode("CommentTip".toUpperCase(), 1, 1000L));
        assertEquals(true, strategy.needDecode("CommentTip".toUpperCase(), 1, 1001L));
        //
        assertEquals(false, strategy.needDecode("CommentTip".toLowerCase(), 1, 0L));
        assertEquals(false, strategy.needDecode("CommentTip".toLowerCase(), 1, 10L));
        assertEquals(true, strategy.needDecode("CommentTip".toLowerCase(), 1, 1000L));
        assertEquals(true, strategy.needDecode("CommentTip".toLowerCase(), 1, 1001L));

        //
        assertEquals(false, strategy.needDecode("DataTip".toUpperCase(), 1, 0L));
        assertEquals(false, strategy.needDecode("DataTip".toUpperCase(), 1, 10L));
        assertEquals(false, strategy.needDecode("DataTip".toUpperCase(), 1, 1000L));
        assertEquals(false, strategy.needDecode("DataTip".toUpperCase(), 1, 1001L));
        //
        assertEquals(false, strategy.needDecode("DataTip".toLowerCase(), 1, 0L));
        assertEquals(false, strategy.needDecode("DataTip".toLowerCase(), 1, 10L));
        assertEquals(false, strategy.needDecode("DataTip".toLowerCase(), 1, 1000L));
        assertEquals(false, strategy.needDecode("DataTip".toLowerCase(), 1, 1001L));

        //
        System.out.println(strategy.needDecode("DataTip_Qaz", 1, 0L));
        System.out.println(strategy.needDecode("DataTip_Qaz", 1, 10L));
        System.out.println(strategy.needDecode("DataTip_Qaz", 1, 1000L));
        System.out.println(strategy.needDecode("DataTip_Qaz", 1, 1001L));
    }

}