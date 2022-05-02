package jdtx.repl.main.api.decoder;

import jandcode.dbm.test.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.util.*;
import org.json.simple.*;
import org.junit.*;

import java.util.*;

/**
 *
 */
public class RefDecoder_Test extends DbmTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        // Стратегии перекодировки каждой таблицы
        JSONObject cfgDecode = UtRepl.loadAndValidateJsonFile("test/etalon/decode_strategy.json");
        RefDecodeStrategy.initInstance(cfgDecode);
    }

    @Test
    public void test_x() throws Exception {
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
    public void test_1() throws Exception {
        dbm.getDb().execSql("delete from " + UtJdx.SYS_TABLE_PREFIX + "decode");

        //
        long wsId = 1;
        long wsId_d1 = 1;
        long wsId_d2 = 2;

        // ---
        RefDecoder d1 = new RefDecoder(dbm.getDb(), wsId);
        d1.SLOT_SIZE = 4;

        //
        for (long id = 0; id < 10; id++) {
            long id_own = d1.get_id_local("LIC", new JdxRef(wsId_d1, id));
            System.out.println(wsId_d1 + ":" + id + " -> " + id_own);
        }

        //
        System.out.println("");
        for (long id = 5; id < 15; id++) {
            long id_own = d1.get_id_local("LIC", new JdxRef(wsId_d1, id));
            System.out.println(wsId_d1 + ":" + id + " -> " + id_own);
        }

        System.out.println("");
        System.out.println("d1.wsToSlotList: " + d1.wsToSlotList);
        System.out.println("d1.slotToWsList: " + d1.slotToWsList);
        System.out.println("");


        // ---
        RefDecoder d2 = new RefDecoder(dbm.getDb(), wsId);
        d2.SLOT_SIZE = 4;

        //
        for (long id = 0; id < 10; id++) {
            long id_own = d2.get_id_local("LIC", new JdxRef(wsId_d2, id));
            System.out.println(wsId_d2 + ":" + id + " -> " + id_own);
        }

        //
        System.out.println("");
        for (long id = 100; id < 110; id++) {
            long id_own = d2.get_id_local("LIC", new JdxRef(wsId_d2, id));
            System.out.println(wsId_d2 + ":" + id + " -> " + id_own);
        }

        System.out.println("");
        System.out.println("d2.wsToSlotList: " + d2.wsToSlotList);
        System.out.println("d2.slotToWsList: " + d2.slotToWsList);
        System.out.println("");


        // ---
        d1 = new RefDecoder(dbm.getDb(), wsId);
        d1.SLOT_SIZE = 4;

        //
        System.out.println("");
        for (long id = 10; id < 25; id++) {
            long id_own = d1.get_id_local("LIC", new JdxRef(wsId_d1, id));
            System.out.println(wsId_d1 + ":" + id + " -> " + id_own);
        }

        System.out.println("");
        System.out.println("d1.wsToSlotList: " + d1.wsToSlotList);
        System.out.println("d1.slotToWsList: " + d1.slotToWsList);
        System.out.println("");


        // ---
        d2 = new RefDecoder(dbm.getDb(), wsId);
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
    public void test_2() throws Exception {
        long wsId = 1;
        long wsId_d1 = 1;
        long wsId_d2 = 2;

        // ---
        RefDecoder decoder = new RefDecoder(dbm.getDb(), wsId);
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
        assertEquals(decoder.self_ws_id, ref.ws_id);

        //
        id_own = decoder.get_id_local("LIC", new JdxRef(wsId_d2, 0));
        ref = decoder.get_ref("LIC", id_own);
        System.out.println(id_own + " -> " + ref.toString());
        assertEquals(0, ref.value);
        assertEquals(decoder.self_ws_id, ref.ws_id);
    }


}