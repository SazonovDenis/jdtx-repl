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
        checkValid(jsonFileName);
    }

    @Test
    public void test_checkValid_install() throws Exception {
        String jsonFileName = "../install/cfg/decode_strategy_194.json";
        checkValid(jsonFileName);
    }

    void checkValid(String jsonFileName) throws Exception {
        JSONObject cfgDecode = UtRepl.loadAndValidateJsonFile(jsonFileName);
        RefDecodeStrategy.initInstance(cfgDecode);

        //
        RefDecodeStrategy strategy = RefDecodeStrategy.getInstance();
        System.out.println("file: " + jsonFileName);
        UtDecodeStrategy.checkValid(strategy, struct);

        //
        System.out.println();
        //
        assertEquals(false, strategy.needDecodeOwn("Lic".toUpperCase(), 0L));
        assertEquals(true, strategy.needDecodeOwn("Lic".toUpperCase(), 10L));
        assertEquals(true, strategy.needDecodeOwn("Lic".toUpperCase(), 1000L));
        assertEquals(true, strategy.needDecodeOwn("Lic".toUpperCase(), 1001L));
        //
        assertEquals(false, strategy.needDecodeOwn("Lic".toLowerCase(), 0L));
        assertEquals(true, strategy.needDecodeOwn("Lic".toLowerCase(), 10L));
        assertEquals(true, strategy.needDecodeOwn("Lic".toLowerCase(), 1000L));
        assertEquals(true, strategy.needDecodeOwn("Lic".toLowerCase(), 1001L));

        //
        assertEquals(false, strategy.needDecodeOwn("CommentTip".toUpperCase(), 0L));
        assertEquals(false, strategy.needDecodeOwn("CommentTip".toUpperCase(), 10L));
        assertEquals(true, strategy.needDecodeOwn("CommentTip".toUpperCase(), 1000L));
        assertEquals(true, strategy.needDecodeOwn("CommentTip".toUpperCase(), 1001L));
        //
        assertEquals(false, strategy.needDecodeOwn("CommentTip".toLowerCase(), 0L));
        assertEquals(false, strategy.needDecodeOwn("CommentTip".toLowerCase(), 10L));
        assertEquals(true, strategy.needDecodeOwn("CommentTip".toLowerCase(), 1000L));
        assertEquals(true, strategy.needDecodeOwn("CommentTip".toLowerCase(), 1001L));

        //
        assertEquals(false, strategy.needDecodeOwn("DataTip".toUpperCase(), 0L));
        assertEquals(false, strategy.needDecodeOwn("DataTip".toUpperCase(), 10L));
        assertEquals(false, strategy.needDecodeOwn("DataTip".toUpperCase(), 1000L));
        assertEquals(false, strategy.needDecodeOwn("DataTip".toUpperCase(), 1001L));
        //
        assertEquals(false, strategy.needDecodeOwn("DataTip".toLowerCase(), 0L));
        assertEquals(false, strategy.needDecodeOwn("DataTip".toLowerCase(), 10L));
        assertEquals(false, strategy.needDecodeOwn("DataTip".toLowerCase(), 1000L));
        assertEquals(false, strategy.needDecodeOwn("DataTip".toLowerCase(), 1001L));

        //
        System.out.println(strategy.needDecodeOwn("DataTip_Qaz", 0L));
        System.out.println(strategy.needDecodeOwn("DataTip_Qaz", 10L));
        System.out.println(strategy.needDecodeOwn("DataTip_Qaz", 1000L));
        System.out.println(strategy.needDecodeOwn("DataTip_Qaz", 1001L));
    }

}