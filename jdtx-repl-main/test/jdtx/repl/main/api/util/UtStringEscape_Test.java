package jdtx.repl.main.api.util;

import groovy.json.*;
import jandcode.utils.test.*;
import org.junit.*;

/**
 *
 */
public class UtStringEscape_Test extends UtilsTestCase {

    public static String sSr1 = "qqq\tttt\baaa\0zzz\ruuu\nsss\\too\\boo\\roo\\zoo";
    public static String sSr2 = "qwerty|Иванов ВАСЯ|ә, ғ, қ, ң, ө, ұ, ү, h";
    public static String sSr3 = "qqq\"\"zoo''hhh\"'xxx'\"fff| the \"SUN\" the 'sun'";

    public static String[] sArr = {sSr1, sSr2, sSr3};

    @Test
    public void test_escape() throws Exception {
        String sEs1 = StringEscapeUtils.escapeJava(sSr1);
        String sEs2 = StringEscapeUtils.escapeJava(sSr2);
        String sEs3 = StringEscapeUtils.escapeJava(sSr3);
        String sUn1 = StringEscapeUtils.unescapeJava(sEs1);
        String sUn2 = StringEscapeUtils.unescapeJava(sEs2);
        String sUn3 = StringEscapeUtils.unescapeJava(sEs3);

        System.out.println(sSr1);
        System.out.println(sEs1);
        System.out.println(sUn1);
        System.out.println("---");
        System.out.println(sSr2);
        System.out.println(sEs2);
        System.out.println(sUn3);
        System.out.println("---");
        System.out.println(sSr3);
        System.out.println(sEs3);
        System.out.println(sUn3);

        assertEquals(sSr1, sUn1);
        assertEquals(sSr2, sUn2);
        assertEquals(sSr3, sUn3);
    }

    @Test
    public void test_escape_Jdx() throws Exception {
        String sEs1 = UtStringEscape.escapeJava(sSr1);
        String sEs2 = UtStringEscape.escapeJava(sSr2);
        String sEs3 = UtStringEscape.escapeJava(sSr3);
        String sUn1 = StringEscapeUtils.unescapeJava(sEs1);
        String sUn2 = StringEscapeUtils.unescapeJava(sEs2);
        String sUn3 = StringEscapeUtils.unescapeJava(sEs3);

        System.out.println(sSr1);
        System.out.println(sEs1);
        System.out.println(sUn1);
        System.out.println("---");
        System.out.println(sSr2);
        System.out.println(sEs2);
        System.out.println(sUn3);
        System.out.println("---");
        System.out.println(sSr3);
        System.out.println(sEs3);
        System.out.println(sUn3);

        assertEquals(sSr1, sUn1);
        assertEquals(sSr2, sUn2);
        assertEquals(sSr3, sUn3);
    }

}
