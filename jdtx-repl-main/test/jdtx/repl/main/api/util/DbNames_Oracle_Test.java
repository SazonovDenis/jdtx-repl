package jdtx.repl.main.api.util;

import jandcode.utils.test.*;
import org.junit.*;

public class DbNames_Oracle_Test extends UtilsTestCase {


    @Test
    public void getShortName() {
        IDbNames manager = new DbNames_Oracle();

        String s10 = "1234567890";
        String s20 = "01234567890123456789";
        String s30 = "012345678901234567890123456789";
        String s34 = "012345678901234567890123456789asdf";

        //
        String prefix = "Z_T_";
        String suffix = "_UT";

        //
        assertEquals(s10, manager.getShortName(s10, 3));

        assertEquals(s20, manager.getShortName(s20, 3));

        assertNotSame(s30, manager.getShortName(s30, 3));

        assertNotSame(s30, manager.getShortName(s30, 1));

        assertEquals(s30, manager.getShortName(s30, 0));

        assertNotSame(s34, manager.getShortName(s34, 0));

        assertEquals(s10.length(), manager.getShortName(s10, 3).length());

        assertEquals(s20.length(), manager.getShortName(s20, 3).length());

        assertEquals(27, manager.getShortName(s30, 3).length());

        assertEquals(27, manager.getShortName(s34, 3).length());

        assertEquals(30, manager.getShortName(s34, 0).length());

        //
        assertEquals(prefix+s10 , manager.getShortName(s10, prefix));

        assertEquals(prefix+s20 , manager.getShortName(s20, prefix));

        assertNotSame(prefix+s30 , manager.getShortName(s30, prefix));

        assertNotSame("-"+s30 , manager.getShortName(s30, "-"));

        assertEquals(s30, manager.getShortName(s30, ""));

        assertNotSame(s34, manager.getShortName(s34, ""));

        assertEquals(s10.length() + prefix.length(), manager.getShortName(s10, prefix).length());

        assertEquals(s20.length() + prefix.length(), manager.getShortName(s20, prefix).length());

        assertEquals(30, manager.getShortName(s30, prefix).length());

        assertEquals(30, manager.getShortName(s34, prefix).length());

        //
        assertEquals(prefix + s10 + suffix, manager.getShortName(s10, prefix, suffix));

        assertEquals(prefix + s20 + suffix, manager.getShortName(s20, prefix, suffix));

        assertNotSame(prefix + s30 + suffix, manager.getShortName(s30, prefix, suffix));

        assertNotSame("+" + s30 + "-", manager.getShortName(s30, "+", "-"));

        assertEquals("" + s20, manager.getShortName(s20, "", ""));

        assertEquals(s30, manager.getShortName(s30, "", ""));

        assertNotSame(s34, manager.getShortName(s34, "", ""));

        assertEquals(prefix.length() + s10.length() + suffix.length(), manager.getShortName(s10, prefix, suffix).length());

        assertEquals(prefix.length() + s20.length() + suffix.length(), manager.getShortName(s20, prefix, suffix).length());

        assertEquals(30, manager.getShortName(s30, prefix, suffix).length());

        assertEquals(30, manager.getShortName(s34, prefix, suffix).length());

    }

}

