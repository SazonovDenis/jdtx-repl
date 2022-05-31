package jdtx.repl.main.api;

import jandcode.utils.error.*;
import jdtx.repl.main.api.util.*;
import org.joda.time.*;
import org.junit.*;

/**
 *
 */
public class JdxReplWs_repairLockFile_Test extends DbPrepareEtalon_Test {


    @Test
    public void test_repairLockFile() throws Exception {
        db2.connect();
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();

        //
        ws.repairLockFileDelete();
        assertEquals(false, ws.repairLockFile().exists());

        //
        System.out.println("repairLockFile Create");

        //
        ws.repairLockFileCreate();
        assertEquals(true, ws.repairLockFile().exists());

        //
        System.out.println("repairLockFileRead: " + ws.repairLockFileRead());
        System.out.println("repairLockFileGiud: " + ws.repairLockFileGiud());
        assertEquals(true, ws.repairLockFileRead() != null);
        assertEquals(true, ws.repairLockFileGiud() != null);
        assertEquals(true, ws.repairLockFileRead().contains(new DateTime().toString("YYYY-MM-dd")));
        assertEquals(true, ws.repairLockFileRead().contains(ws.repairLockFileGiud()));

        //
        try {
            ws.repairLockFileCreate();
            throw new XError("Should fail");
        } catch (Exception e) {
            if (!UtJdxErrors.collectExceptionText(e).contains("already exists")) {
                throw e;
            }
        }

        //
        System.out.println("repairLockFile Delete");
        ws.repairLockFileDelete();

        //
        assertEquals(false, ws.repairLockFile().exists());
        System.out.println("repairLockFileRead: " + ws.repairLockFileRead());
        System.out.println("repairLockFileGiud: " + ws.repairLockFileGiud());
        assertEquals(true, ws.repairLockFileRead() == null);
        assertEquals(true, ws.repairLockFileGiud() == null);
    }


}
