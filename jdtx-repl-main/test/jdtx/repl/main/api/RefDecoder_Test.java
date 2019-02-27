package jdtx.repl.main.api;

import jandcode.dbm.test.*;
import org.junit.*;

/**
 */
public class RefDecoder_Test extends DbmTestCase {


    @Test
    public void test_1() throws Exception {
        long wsId = 1;
        long wsId_d1 = 1;
        long wsId_d2 = 2;

        // ---
        RefDecoder d1 = new RefDecoder(dbm.getDb(), wsId);
        d1.SLOT_SIZE = 4;

        //
        for (long id = 1; id < 10; id++) {
            long id_own = d1.get_id_own(id, wsId_d1, "LIC");
            System.out.println(wsId_d1 + ":" + id + " -> " + id_own);
        }

        //
        System.out.println("");
        for (long id = 5; id < 15; id++) {
            long id_own = d1.get_id_own(id, wsId_d1, "LIC");
            System.out.println(wsId_d1 + ":" + id + " -> " + id_own);
        }

        System.out.println("");
        System.out.println("d1.allSlots: " + d1.allSlots);
        System.out.println("");


        // ---
        RefDecoder d2 = new RefDecoder(dbm.getDb(), wsId);
        d2.SLOT_SIZE = 4;

        //
        for (long id = 1; id < 10; id++) {
            long id_own = d2.get_id_own(id, wsId_d2, "LIC");
            System.out.println(wsId_d2 + ":" + id + " -> " + id_own);
        }

        //
        System.out.println("");
        for (long id = 100; id < 110; id++) {
            long id_own = d2.get_id_own(id, wsId_d2, "LIC");
            System.out.println(wsId_d2 + ":" + id + " -> " + id_own);
        }

        System.out.println("");
        System.out.println("d2.allSlots: " + d2.allSlots);
        System.out.println("");


        // ---
        d1 = new RefDecoder(dbm.getDb(), wsId);
        d1.SLOT_SIZE = 4;

        //
        System.out.println("");
        for (long id = 10; id < 25; id++) {
            long id_own = d1.get_id_own(id, wsId_d1, "LIC");
            System.out.println(wsId_d1 + ":" + id + " -> " + id_own);
        }

        System.out.println("");
        System.out.println("d1.allSlots: " + d1.allSlots);
        System.out.println("");


        // ---
        d2 = new RefDecoder(dbm.getDb(), wsId);
        d2.SLOT_SIZE = 4;

        //
        assertEquals(1, d1.get_id_own(1, wsId_d1, "LIC"));
        assertEquals(2, d1.get_id_own(2, wsId_d1, "LIC"));
        assertEquals(3, d1.get_id_own(3, wsId_d1, "LIC"));
        assertEquals(4, d1.get_id_own(4, wsId_d1, "LIC"));
        assertEquals(5, d1.get_id_own(5, wsId_d1, "LIC"));
        assertEquals(6, d1.get_id_own(6, wsId_d1, "LIC"));
        assertEquals(7, d1.get_id_own(7, wsId_d1, "LIC"));
        assertEquals(8, d1.get_id_own(8, wsId_d1, "LIC"));
        assertEquals(9, d1.get_id_own(9, wsId_d1, "LIC"));
        //
        assertEquals(6, d1.get_id_own(6, wsId_d1, "LIC"));
        assertEquals(7, d1.get_id_own(7, wsId_d1, "LIC"));
        assertEquals(8, d1.get_id_own(8, wsId_d1, "LIC"));
        assertEquals(9, d1.get_id_own(9, wsId_d1, "LIC"));
        assertEquals(10, d1.get_id_own(10, wsId_d1, "LIC"));
        assertEquals(11, d1.get_id_own(11, wsId_d1, "LIC"));
        assertEquals(12, d1.get_id_own(12, wsId_d1, "LIC"));
        assertEquals(13, d1.get_id_own(13, wsId_d1, "LIC"));
        assertEquals(14, d1.get_id_own(14, wsId_d1, "LIC"));
        assertEquals(15, d1.get_id_own(15, wsId_d1, "LIC"));
        //
        assertEquals(2 + 4 * 101, d1.get_id_own(2, wsId_d2, "LIC"));
        assertEquals(3 + 4 * 101, d1.get_id_own(3, wsId_d2, "LIC"));
        assertEquals(0 + 4 * 102, d1.get_id_own(4, wsId_d2, "LIC"));
        assertEquals(1 + 4 * 102, d1.get_id_own(5, wsId_d2, "LIC"));
        assertEquals(2 + 4 * 102, d1.get_id_own(6, wsId_d2, "LIC"));
        assertEquals(3 + 4 * 102, d1.get_id_own(7, wsId_d2, "LIC"));
        assertEquals(0 + 4 * 103, d1.get_id_own(8, wsId_d2, "LIC"));
        assertEquals(1 + 4 * 103, d1.get_id_own(9, wsId_d2, "LIC"));
        //
        assertEquals(1 + 4 * 104, d1.get_id_own(101, wsId_d2, "LIC"));
        assertEquals(2 + 4 * 104, d1.get_id_own(102, wsId_d2, "LIC"));
        assertEquals(3 + 4 * 104, d1.get_id_own(103, wsId_d2, "LIC"));
        assertEquals(0 + 4 * 105, d1.get_id_own(104, wsId_d2, "LIC"));
        assertEquals(1 + 4 * 105, d1.get_id_own(105, wsId_d2, "LIC"));
        assertEquals(2 + 4 * 105, d1.get_id_own(106, wsId_d2, "LIC"));
        assertEquals(3 + 4 * 105, d1.get_id_own(107, wsId_d2, "LIC"));
        assertEquals(0 + 4 * 106, d1.get_id_own(108, wsId_d2, "LIC"));
        assertEquals(1 + 4 * 106, d1.get_id_own(109, wsId_d2, "LIC"));
        //
        assertEquals(11, d1.get_id_own(11, wsId_d1, "LIC"));
        assertEquals(12, d1.get_id_own(12, wsId_d1, "LIC"));
        assertEquals(13, d1.get_id_own(13, wsId_d1, "LIC"));
        assertEquals(14, d1.get_id_own(14, wsId_d1, "LIC"));
        assertEquals(15, d1.get_id_own(15, wsId_d1, "LIC"));
        assertEquals(16, d1.get_id_own(16, wsId_d1, "LIC"));
        assertEquals(17, d1.get_id_own(17, wsId_d1, "LIC"));
        assertEquals(18, d1.get_id_own(18, wsId_d1, "LIC"));
        assertEquals(19, d1.get_id_own(19, wsId_d1, "LIC"));
        assertEquals(20, d1.get_id_own(20, wsId_d1, "LIC"));
        assertEquals(21, d1.get_id_own(21, wsId_d1, "LIC"));
        assertEquals(22, d1.get_id_own(22, wsId_d1, "LIC"));
        assertEquals(23, d1.get_id_own(23, wsId_d1, "LIC"));
        assertEquals(24, d1.get_id_own(24, wsId_d1, "LIC"));
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
            long id_own = decoder.get_id_own(id, wsId_d1, "LIC");
            //System.out.println(wsId_d1 + ":" + id + " -> " + id_own);
            //
            JdxRef ref = decoder.get_ref(id_own, "LIC");
            System.out.println(id_own + "->" + ref.ws_id + ":" + ref.id);
            //
            assertEquals(id, ref.id);
            assertEquals(wsId_d1, ref.ws_id);
        }

        for (long id = 100; id < 110; id++) {
            long id_own = decoder.get_id_own(id, wsId_d2, "LIC");
            //System.out.println(wsId_d2 + ":" + id + " -> " + id_own);
            //
            JdxRef ref = decoder.get_ref(id_own, "LIC");
            System.out.println(id_own + "->" + ref.ws_id + ":" + ref.id);
            //
            assertEquals(id, ref.id);
            assertEquals(wsId_d2, ref.ws_id);
        }

        for (long id = 80; id < 120; id++) {
            long id_own = decoder.get_id_own(id, wsId_d1, "LIC");
            //System.out.println(wsId_d1 + ":" + id + " -> " + id_own);
            //
            JdxRef ref = decoder.get_ref(id_own, "LIC");
            System.out.println(id_own + "->" + ref.ws_id + ":" + ref.id);
            //
            assertEquals(id, ref.id);
            assertEquals(wsId_d1, ref.ws_id);
        }

        for (long id = 10; id < 25; id++) {
            long id_own = decoder.get_id_own(id, wsId_d2, "LIC");
            //System.out.println(wsId_d2 + ":" + id + " -> " + id_own);
            //
            JdxRef ref = decoder.get_ref(id_own, "LIC");
            System.out.println(id_own + "->" + ref.ws_id + ":" + ref.id);
            //
            assertEquals(id, ref.id);
            assertEquals(wsId_d2, ref.ws_id);
        }

    }


}