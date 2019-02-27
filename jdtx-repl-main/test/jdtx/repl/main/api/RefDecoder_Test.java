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
            System.out.println(wsId_d1 + ":" + id + " -> " + d1.ws_id + ":" + id_own);
        }

        //
        System.out.println("");
        for (long id = 5; id < 15; id++) {
            long id_own = d1.get_id_own(id, wsId_d1, "LIC");
            System.out.println(wsId_d1 + ":" + id + " -> " + d1.ws_id + ":" + id_own);
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
            System.out.println(wsId_d2 + ":" + id + " -> " + d2.ws_id + ":" + id_own);
        }

        //
        System.out.println("");
        for (long id = 100; id < 110; id++) {
            long id_own = d2.get_id_own(id, wsId_d2, "LIC");
            System.out.println(wsId_d2 + ":" + id + " -> " + d2.ws_id + ":" + id_own);
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
            System.out.println(wsId_d1 + ":" + id + " -> " + d1.ws_id + ":" + id_own);
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
        //
        assertEquals(18, d1.get_id_own(2, wsId_d2, "LIC"));
        assertEquals(19, d1.get_id_own(3, wsId_d2, "LIC"));
        assertEquals(20, d1.get_id_own(4, wsId_d2, "LIC"));
        assertEquals(21, d1.get_id_own(5, wsId_d2, "LIC"));
        assertEquals(22, d1.get_id_own(6, wsId_d2, "LIC"));
        assertEquals(23, d1.get_id_own(7, wsId_d2, "LIC"));
        assertEquals(24, d1.get_id_own(8, wsId_d2, "LIC"));
        assertEquals(25, d1.get_id_own(9, wsId_d2, "LIC"));
        //
        assertEquals(29, d1.get_id_own(101, wsId_d2, "LIC"));
        assertEquals(30, d1.get_id_own(102, wsId_d2, "LIC"));
        assertEquals(31, d1.get_id_own(103, wsId_d2, "LIC"));
        assertEquals(32, d1.get_id_own(104, wsId_d2, "LIC"));
        assertEquals(33, d1.get_id_own(105, wsId_d2, "LIC"));
        assertEquals(34, d1.get_id_own(106, wsId_d2, "LIC"));
        assertEquals(35, d1.get_id_own(107, wsId_d2, "LIC"));
        assertEquals(36, d1.get_id_own(108, wsId_d2, "LIC"));
        assertEquals(37, d1.get_id_own(109, wsId_d2, "LIC"));
        //
        assertEquals(11, d1.get_id_own(11, wsId_d1, "LIC"));
        assertEquals(12, d1.get_id_own(12, wsId_d1, "LIC"));
        assertEquals(13, d1.get_id_own(13, wsId_d1, "LIC"));
        assertEquals(14, d1.get_id_own(14, wsId_d1, "LIC"));
        assertEquals(15, d1.get_id_own(15, wsId_d1, "LIC"));
        assertEquals(40, d1.get_id_own(16, wsId_d1, "LIC"));
        assertEquals(41, d1.get_id_own(17, wsId_d1, "LIC"));
        assertEquals(42, d1.get_id_own(18, wsId_d1, "LIC"));
        assertEquals(43, d1.get_id_own(19, wsId_d1, "LIC"));
        assertEquals(44, d1.get_id_own(20, wsId_d1, "LIC"));
        assertEquals(45, d1.get_id_own(21, wsId_d1, "LIC"));
        assertEquals(46, d1.get_id_own(22, wsId_d1, "LIC"));
        assertEquals(47, d1.get_id_own(23, wsId_d1, "LIC"));
        assertEquals(48, d1.get_id_own(24, wsId_d1, "LIC"));


    }

}