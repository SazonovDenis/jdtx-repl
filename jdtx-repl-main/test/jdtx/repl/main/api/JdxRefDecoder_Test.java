package jdtx.repl.main.api;

import jandcode.dbm.test.*;
import org.junit.*;

/**
 */
public class JdxRefDecoder_Test extends DbmTestCase {


    @Test
    public void test_1() throws Exception {
        // ---
        JdxRefDecoder d1 = new JdxRefDecoder(dbm.getDb(), 1);
        d1.SLOT_SIZE = 4;

        //
        for (long id = 1; id < 10; id++) {
            long id_own = d1.getOrCreate_id_own(id, "LIC");
            System.out.println("id.1 = " + id + " -> " + id_own + "");
        }

        //
        System.out.println("");
        for (long id = 5; id < 15; id++) {
            long id_own = d1.getOrCreate_id_own(id, "LIC");
            System.out.println("id.1 = " + id + " -> " + id_own + "");
        }

        System.out.println("");
        System.out.println(d1.tablesDecodeSlots);


        // ---
        JdxRefDecoder d2 = new JdxRefDecoder(dbm.getDb(), 2);
        d2.SLOT_SIZE = 4;

        //
        for (long id = 1; id < 10; id++) {
            long id_own = d2.getOrCreate_id_own(id, "LIC");
            System.out.println("id.2 = " + id + " -> " + id_own + "");
        }

        //
        System.out.println("");
        for (long id = 100; id < 110; id++) {
            long id_own = d2.getOrCreate_id_own(id, "LIC");
            System.out.println("id.2 = " + id + " -> " + id_own + "");
        }

        System.out.println("");
        System.out.println(d2.tablesDecodeSlots);


        // ---
        d1 = new JdxRefDecoder(dbm.getDb(), 1);
        d1.SLOT_SIZE = 4;

        //
        System.out.println("");
        for (long id = 10; id < 25; id++) {
            long id_own = d1.getOrCreate_id_own(id, "LIC");
            System.out.println("id.1 = " + id + " -> " + id_own + "");
        }

        System.out.println("");
        System.out.println(d1.tablesDecodeSlots);


        // ---
        d2 = new JdxRefDecoder(dbm.getDb(), 2);
        d2.SLOT_SIZE = 4;

        //
        assertEquals(d1.getOrCreate_id_own(1, "LIC"), 1);
        assertEquals(d1.getOrCreate_id_own(2, "LIC"), 2);
        assertEquals(d1.getOrCreate_id_own(3, "LIC"), 3);
        assertEquals(d1.getOrCreate_id_own(4, "LIC"), 4);
        assertEquals(d1.getOrCreate_id_own(5, "LIC"), 5);
        assertEquals(d1.getOrCreate_id_own(6, "LIC"), 6);
        assertEquals(d1.getOrCreate_id_own(7, "LIC"), 7);
        assertEquals(d1.getOrCreate_id_own(8, "LIC"), 8);
        assertEquals(d1.getOrCreate_id_own(9, "LIC"), 9);
        //
        assertEquals(d1.getOrCreate_id_own(6, "LIC"), 6);
        assertEquals(d1.getOrCreate_id_own(7, "LIC"), 7);
        assertEquals(d1.getOrCreate_id_own(8, "LIC"), 8);
        assertEquals(d1.getOrCreate_id_own(9, "LIC"), 9);
        assertEquals(d1.getOrCreate_id_own(10, "LIC"), 10);
        assertEquals(d1.getOrCreate_id_own(11, "LIC"), 11);
        assertEquals(d1.getOrCreate_id_own(12, "LIC"), 12);
        assertEquals(d1.getOrCreate_id_own(13, "LIC"), 13);
        assertEquals(d1.getOrCreate_id_own(14, "LIC"), 14);
        //
        assertEquals(d2.getOrCreate_id_own(2, "LIC"), 18);
        assertEquals(d2.getOrCreate_id_own(3, "LIC"), 19);
        assertEquals(d2.getOrCreate_id_own(4, "LIC"), 20);
        assertEquals(d2.getOrCreate_id_own(5, "LIC"), 21);
        assertEquals(d2.getOrCreate_id_own(6, "LIC"), 22);
        assertEquals(d2.getOrCreate_id_own(7, "LIC"), 23);
        assertEquals(d2.getOrCreate_id_own(8, "LIC"), 24);
        assertEquals(d2.getOrCreate_id_own(9, "LIC"), 25);
        //
        assertEquals(d2.getOrCreate_id_own(101, "LIC"), 29);
        assertEquals(d2.getOrCreate_id_own(102, "LIC"), 30);
        assertEquals(d2.getOrCreate_id_own(103, "LIC"), 31);
        assertEquals(d2.getOrCreate_id_own(104, "LIC"), 32);
        assertEquals(d2.getOrCreate_id_own(105, "LIC"), 33);
        assertEquals(d2.getOrCreate_id_own(106, "LIC"), 34);
        assertEquals(d2.getOrCreate_id_own(107, "LIC"), 35);
        assertEquals(d2.getOrCreate_id_own(108, "LIC"), 36);
        assertEquals(d2.getOrCreate_id_own(109, "LIC"), 37);
        //
        assertEquals(d1.getOrCreate_id_own(11, "LIC"), 11);
        assertEquals(d1.getOrCreate_id_own(12, "LIC"), 12);
        assertEquals(d1.getOrCreate_id_own(13, "LIC"), 13);
        assertEquals(d1.getOrCreate_id_own(14, "LIC"), 14);
        assertEquals(d1.getOrCreate_id_own(15, "LIC"), 15);
        assertEquals(d1.getOrCreate_id_own(16, "LIC"), 40);
        assertEquals(d1.getOrCreate_id_own(17, "LIC"), 41);
        assertEquals(d1.getOrCreate_id_own(18, "LIC"), 42);
        assertEquals(d1.getOrCreate_id_own(19, "LIC"), 43);
        assertEquals(d1.getOrCreate_id_own(20, "LIC"), 44);
        assertEquals(d1.getOrCreate_id_own(21, "LIC"), 45);
        assertEquals(d1.getOrCreate_id_own(22, "LIC"), 46);
        assertEquals(d1.getOrCreate_id_own(23, "LIC"), 47);
        assertEquals(d1.getOrCreate_id_own(24, "LIC"), 48);


    }

}