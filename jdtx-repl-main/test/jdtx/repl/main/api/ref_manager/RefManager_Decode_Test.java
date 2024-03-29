package jdtx.repl.main.api.ref_manager;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.util.*;
import org.json.simple.*;
import org.junit.*;

import java.util.*;

/**
 *
 */
public class RefManager_Decode_Test extends ReplDatabaseStruct_Test {

    // Стратегии перекодировки каждой таблицы
    JSONObject cfgDecode;

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        //
        super.setUp();
        //
        cfgDecode = UtRepl.loadAndValidateJsonFile("test/etalon/decode_strategy.json");
    }

    @Test
    public void test_DecoderSlot() throws Exception {
        Map<RefDecoderSlot, Long> wsToSlot = new HashMap<>();

        //
        RefDecoderSlot sl_1 = new RefDecoderSlot();
        sl_1.ws_id = 2;
        sl_1.ws_slot_no = 0;
        //
        wsToSlot.put(sl_1, 999L);

        //
        RefDecoderSlot sl_2 = new RefDecoderSlot();
        sl_2.ws_id = 2;
        sl_2.ws_slot_no = 0;

        // Ищем наш слот
        Long own_slot_no_1 = wsToSlot.get(sl_1);
        System.out.println(own_slot_no_1);
        //
        Long own_slot_no_2 = wsToSlot.get(sl_2);
        System.out.println(own_slot_no_2);
    }

    @Test
    public void test_RefManagerDecode() throws Exception {
        RefManager_Decode d1 = extSrv.getApp().service(RefManager_Decode.class);
        System.out.println(d1.getClass());
    }

/*
    @Test
    public void test_RefManagerDecode_1() throws Exception {
        db.execSql("delete from " + UtJdx.SYS_TABLE_PREFIX + "decode");

        //
        long wsId = 1;
        long wsId_d1 = 1;
        long wsId_d2 = 2;

        // ---
        RefManager_Decode d1 = extSrv.getApp().service(RefManager_Decode.class);
        d1.db = db;
        d1.init(wsId, cfgDecode);
        d1.SLOT_SIZE = 4;

        //
        for (long id = 0; id < 10; id++) {
            long id_own = d1.get_id_local("LIC", new JdxRef(wsId_d1, id));
            System.out.println(wsId_d1 + ":" + id + " -> " + id_own);
        }

        //
        System.out.println();
        for (long id = 5; id < 15; id++) {
            long id_own = d1.get_id_local("LIC", new JdxRef(wsId_d1, id));
            System.out.println(wsId_d1 + ":" + id + " -> " + id_own);
        }

        System.out.println();
        System.out.println("d1.wsToSlotList: " + d1.wsToSlotList);
        System.out.println("d1.slotToWsList: " + d1.slotToWsList);
        System.out.println();


        // ---
        RefManager_Decode d2 = new RefManager_Decode();
        d2.db = db;
        d2.init(wsId, cfgDecode);
        d2.SLOT_SIZE = 4;

        //
        for (long id = 0; id < 10; id++) {
            long id_own = d2.get_id_local("LIC", new JdxRef(wsId_d2, id));
            System.out.println(wsId_d2 + ":" + id + " -> " + id_own);
        }

        //
        System.out.println();
        for (long id = 100; id < 110; id++) {
            long id_own = d2.get_id_local("LIC", new JdxRef(wsId_d2, id));
            System.out.println(wsId_d2 + ":" + id + " -> " + id_own);
        }

        System.out.println();
        System.out.println("d2.wsToSlotList: " + d2.wsToSlotList);
        System.out.println("d2.slotToWsList: " + d2.slotToWsList);
        System.out.println();


        // ---
        d1 = new RefManager_Decode();
        d1.db = db;
        d1.init(wsId, cfgDecode);
        d1.SLOT_SIZE = 4;

        //
        System.out.println();
        for (long id = 10; id < 25; id++) {
            long id_own = d1.get_id_local("LIC", new JdxRef(wsId_d1, id));
            System.out.println(wsId_d1 + ":" + id + " -> " + id_own);
        }

        System.out.println();
        System.out.println("d1.wsToSlotList: " + d1.wsToSlotList);
        System.out.println("d1.slotToWsList: " + d1.slotToWsList);
        System.out.println();


        // ---
        d2 = new RefManager_Decode();
        d2.db = db;
        d2.init(wsId, cfgDecode);
        d2.SLOT_SIZE = 4;

        //
        assertEquals(0, d1.get_id_local("LIC", new JdxRef(wsId_d1, 0)));
        assertEquals(1, d1.get_id_local("LIC", new JdxRef(wsId_d1, 1)));
        assertEquals(2, d1.get_id_local("LIC", new JdxRef(wsId_d1, 2)));
        assertEquals(3, d1.get_id_local("LIC", new JdxRef(wsId_d1, 3)));
        assertEquals(4, d1.get_id_local("LIC", new JdxRef(wsId_d1, 4)));
        assertEquals(5, d1.get_id_local("LIC", new JdxRef(wsId_d1, 5)));
        assertEquals(6, d1.get_id_local("LIC", new JdxRef(wsId_d1, 6)));
        assertEquals(7, d1.get_id_local("LIC", new JdxRef(wsId_d1, 7)));
        assertEquals(8, d1.get_id_local("LIC", new JdxRef(wsId_d1, 8)));
        assertEquals(9, d1.get_id_local("LIC", new JdxRef(wsId_d1, 9)));
        //
        assertEquals(6, d1.get_id_local("LIC", new JdxRef(wsId_d1, 6)));
        assertEquals(7, d1.get_id_local("LIC", new JdxRef(wsId_d1, 7)));
        assertEquals(8, d1.get_id_local("LIC", new JdxRef(wsId_d1, 8)));
        assertEquals(9, d1.get_id_local("LIC", new JdxRef(wsId_d1, 9)));
        assertEquals(10, d1.get_id_local("LIC", new JdxRef(wsId_d1, 10)));
        assertEquals(11, d1.get_id_local("LIC", new JdxRef(wsId_d1, 11)));
        assertEquals(12, d1.get_id_local("LIC", new JdxRef(wsId_d1, 12)));
        assertEquals(13, d1.get_id_local("LIC", new JdxRef(wsId_d1, 13)));
        assertEquals(14, d1.get_id_local("LIC", new JdxRef(wsId_d1, 14)));
        assertEquals(15, d1.get_id_local("LIC", new JdxRef(wsId_d1, 15)));
        //
        assertEquals(0, d1.get_id_local("LIC", new JdxRef(wsId_d2, 0)));
        assertEquals(2 + 4 * 101, d1.get_id_local("LIC", new JdxRef(wsId_d2, 2)));
        assertEquals(3 + 4 * 101, d1.get_id_local("LIC", new JdxRef(wsId_d2, 3)));
        assertEquals(0 + 4 * 102, d1.get_id_local("LIC", new JdxRef(wsId_d2, 4)));
        assertEquals(1 + 4 * 102, d1.get_id_local("LIC", new JdxRef(wsId_d2, 5)));
        assertEquals(2 + 4 * 102, d1.get_id_local("LIC", new JdxRef(wsId_d2, 6)));
        assertEquals(3 + 4 * 102, d1.get_id_local("LIC", new JdxRef(wsId_d2, 7)));
        assertEquals(0 + 4 * 103, d1.get_id_local("LIC", new JdxRef(wsId_d2, 8)));
        assertEquals(1 + 4 * 103, d1.get_id_local("LIC", new JdxRef(wsId_d2, 9)));
        //
        assertEquals(1 + 4 * 104, d1.get_id_local("LIC", new JdxRef(wsId_d2, 101)));
        assertEquals(2 + 4 * 104, d1.get_id_local("LIC", new JdxRef(wsId_d2, 102)));
        assertEquals(3 + 4 * 104, d1.get_id_local("LIC", new JdxRef(wsId_d2, 103)));
        assertEquals(0 + 4 * 105, d1.get_id_local("LIC", new JdxRef(wsId_d2, 104)));
        assertEquals(1 + 4 * 105, d1.get_id_local("LIC", new JdxRef(wsId_d2, 105)));
        assertEquals(2 + 4 * 105, d1.get_id_local("LIC", new JdxRef(wsId_d2, 106)));
        assertEquals(3 + 4 * 105, d1.get_id_local("LIC", new JdxRef(wsId_d2, 107)));
        assertEquals(0 + 4 * 106, d1.get_id_local("LIC", new JdxRef(wsId_d2, 108)));
        assertEquals(1 + 4 * 106, d1.get_id_local("LIC", new JdxRef(wsId_d2, 109)));
        //
        assertEquals(11, d1.get_id_local("LIC", new JdxRef(wsId_d1, 11)));
        assertEquals(12, d1.get_id_local("LIC", new JdxRef(wsId_d1, 12)));
        assertEquals(13, d1.get_id_local("LIC", new JdxRef(wsId_d1, 13)));
        assertEquals(14, d1.get_id_local("LIC", new JdxRef(wsId_d1, 14)));
        assertEquals(15, d1.get_id_local("LIC", new JdxRef(wsId_d1, 15)));
        assertEquals(16, d1.get_id_local("LIC", new JdxRef(wsId_d1, 16)));
        assertEquals(17, d1.get_id_local("LIC", new JdxRef(wsId_d1, 17)));
        assertEquals(18, d1.get_id_local("LIC", new JdxRef(wsId_d1, 18)));
        assertEquals(19, d1.get_id_local("LIC", new JdxRef(wsId_d1, 19)));
        assertEquals(20, d1.get_id_local("LIC", new JdxRef(wsId_d1, 20)));
        assertEquals(21, d1.get_id_local("LIC", new JdxRef(wsId_d1, 21)));
        assertEquals(22, d1.get_id_local("LIC", new JdxRef(wsId_d1, 22)));
        assertEquals(23, d1.get_id_local("LIC", new JdxRef(wsId_d1, 23)));
        assertEquals(24, d1.get_id_local("LIC", new JdxRef(wsId_d1, 24)));
    }

    @Test
    public void test_RefManagerDecode_2() throws Exception {
        long wsId = 1;
        long wsId_d1 = 1;
        long wsId_d2 = 2;

        // ---

        RefManager_Decode decoder = new RefManager_Decode();
        decoder.db = db;
        decoder.init(wsId, cfgDecode);
        decoder.SLOT_SIZE = 4;

        for (long id = 10; id < 25; id++) {
            long id_own = decoder.get_id_local("LIC", new JdxRef(wsId_d1, id));
            //
            JdxRef ref = decoder.get_ref("LIC", id_own);
            System.out.println(id_own + " -> " + ref.toString());
            //
            assertEquals(id, ref.value);
            assertEquals(wsId_d1, ref.ws_id);
        }

        for (long id = 100; id < 110; id++) {
            long id_own = decoder.get_id_local("LIC", new JdxRef(wsId_d2, id));
            //
            JdxRef ref = decoder.get_ref("LIC", id_own);
            System.out.println(id_own + " -> " + ref.toString());
            //
            assertEquals(id, ref.value);
            assertEquals(wsId_d2, ref.ws_id);
        }

        for (long id = 80; id < 120; id++) {
            long id_own = decoder.get_id_local("LIC", new JdxRef(wsId_d1, id));
            //
            JdxRef ref = decoder.get_ref("LIC", id_own);
            System.out.println(id_own + " -> " + ref.toString());
            //
            assertEquals(id, ref.value);
            assertEquals(wsId_d1, ref.ws_id);
        }

        for (long id = 10; id < 25; id++) {
            long id_own = decoder.get_id_local("LIC", new JdxRef(wsId_d2, id));
            //
            JdxRef ref = decoder.get_ref("LIC", id_own);
            System.out.println(id_own + " -> " + ref.toString());
            //
            assertEquals(id, ref.value);
            assertEquals(wsId_d2, ref.ws_id);
        }

        //
        long id_own = decoder.get_id_local("LIC", new JdxRef(wsId_d1, 0));
        JdxRef ref = decoder.get_ref("LIC", id_own);
        System.out.println(id_own + " -> " + ref.toString());
        assertEquals(0, ref.value);
        assertEquals(true, ref.isEmptyWs());

        //
        id_own = decoder.get_id_local("LIC", new JdxRef(wsId_d2, 0));
        ref = decoder.get_ref("LIC", id_own);
        System.out.println(id_own + " -> " + ref.toString());
        assertEquals(0, ref.value);
        assertEquals(true, ref.isEmptyWs());
    }
*/

    @Test
    public void test_checkValid() throws Exception {
        String jsonFileName = "test/jdtx/repl/main/api/ref_manager/decode_strategy.json";
        checkValid(jsonFileName);
    }

    @Test
    public void test_checkValid_install() throws Exception {
        String jsonFileName = "../install/cfg/decode_strategy_194.json";
        checkValid(jsonFileName);
    }

    void checkValid(String jsonFileName) throws Exception {
        JSONObject cfgDecode = UtRepl.loadAndValidateJsonFile(jsonFileName);
        RefDecodeStrategy decodeStrategy = new RefDecodeStrategy();
        decodeStrategy.init(cfgDecode);

        //
        System.out.println("file: " + jsonFileName);
        UtDecodeStrategy.checkValid(decodeStrategy, struct);

        //
        System.out.println();
        //
        assertEquals(false, decodeStrategy.needDecodeOwn("Lic".toUpperCase(), 0L));
        assertEquals(true, decodeStrategy.needDecodeOwn("Lic".toUpperCase(), 10L));
        assertEquals(true, decodeStrategy.needDecodeOwn("Lic".toUpperCase(), 1000L));
        assertEquals(true, decodeStrategy.needDecodeOwn("Lic".toUpperCase(), 1001L));
        //
        assertEquals(false, decodeStrategy.needDecodeOwn("Lic".toLowerCase(), 0L));
        assertEquals(true, decodeStrategy.needDecodeOwn("Lic".toLowerCase(), 10L));
        assertEquals(true, decodeStrategy.needDecodeOwn("Lic".toLowerCase(), 1000L));
        assertEquals(true, decodeStrategy.needDecodeOwn("Lic".toLowerCase(), 1001L));

        //
        assertEquals(false, decodeStrategy.needDecodeOwn("CommentTip".toUpperCase(), 0L));
        assertEquals(false, decodeStrategy.needDecodeOwn("CommentTip".toUpperCase(), 10L));
        assertEquals(true, decodeStrategy.needDecodeOwn("CommentTip".toUpperCase(), 1000L));
        assertEquals(true, decodeStrategy.needDecodeOwn("CommentTip".toUpperCase(), 1001L));
        //
        assertEquals(false, decodeStrategy.needDecodeOwn("CommentTip".toLowerCase(), 0L));
        assertEquals(false, decodeStrategy.needDecodeOwn("CommentTip".toLowerCase(), 10L));
        assertEquals(true, decodeStrategy.needDecodeOwn("CommentTip".toLowerCase(), 1000L));
        assertEquals(true, decodeStrategy.needDecodeOwn("CommentTip".toLowerCase(), 1001L));

        //
        assertEquals(false, decodeStrategy.needDecodeOwn("DataTip".toUpperCase(), 0L));
        assertEquals(false, decodeStrategy.needDecodeOwn("DataTip".toUpperCase(), 10L));
        assertEquals(false, decodeStrategy.needDecodeOwn("DataTip".toUpperCase(), 1000L));
        assertEquals(false, decodeStrategy.needDecodeOwn("DataTip".toUpperCase(), 1001L));
        //
        assertEquals(false, decodeStrategy.needDecodeOwn("DataTip".toLowerCase(), 0L));
        assertEquals(false, decodeStrategy.needDecodeOwn("DataTip".toLowerCase(), 10L));
        assertEquals(false, decodeStrategy.needDecodeOwn("DataTip".toLowerCase(), 1000L));
        assertEquals(false, decodeStrategy.needDecodeOwn("DataTip".toLowerCase(), 1001L));

        //
        System.out.println(decodeStrategy.needDecodeOwn("DataTip_Qaz", 0L));
        System.out.println(decodeStrategy.needDecodeOwn("DataTip_Qaz", 10L));
        System.out.println(decodeStrategy.needDecodeOwn("DataTip_Qaz", 1000L));
        System.out.println(decodeStrategy.needDecodeOwn("DataTip_Qaz", 1001L));
    }

}