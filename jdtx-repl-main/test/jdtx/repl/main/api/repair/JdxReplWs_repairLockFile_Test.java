package jdtx.repl.main.api.repair;

import jandcode.utils.error.*;
import jdtx.repl.main.api.*;
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
        JdxRepairLockFileManager repairLockFileManager = new JdxRepairLockFileManager(ws.getDataRoot());

        //
        repairLockFileManager.repairLockFileDelete();
        assertEquals(false, repairLockFileManager.getRepairLockFile().exists());

        //
        System.out.println("repairLockFile Create");

        //
        repairLockFileManager.repairLockFileCreate();
        assertEquals(true, repairLockFileManager.getRepairLockFile().exists());

        //
        System.out.println("repairLockFileRead: " + repairLockFileManager.repairLockFileRead());
        System.out.println("repairLockFileGuid: " + repairLockFileManager.repairLockFileGuid());
        assertEquals(true, repairLockFileManager.repairLockFileRead() != null);
        assertEquals(true, repairLockFileManager.repairLockFileGuid() != null);
        assertEquals(true, repairLockFileManager.repairLockFileRead().contains(new DateTime().toString("YYYY-MM-dd")));
        assertEquals(true, repairLockFileManager.repairLockFileRead().contains(repairLockFileManager.repairLockFileGuid()));

        //
        try {
            repairLockFileManager.repairLockFileCreate();
            throw new XError("Should fail");
        } catch (Exception e) {
            if (!UtJdxErrors.collectExceptionText(e).contains("already exists")) {
                throw e;
            }
        }

        //
        System.out.println("repairLockFile Delete");
        repairLockFileManager.repairLockFileDelete();

        //
        assertEquals(false, repairLockFileManager.getRepairLockFile().exists());
        System.out.println("repairLockFileRead: " + repairLockFileManager.repairLockFileRead());
        System.out.println("repairLockFileGuid: " + repairLockFileManager.repairLockFileGuid());
        assertEquals(true, repairLockFileManager.repairLockFileRead() == null);
        assertEquals(true, repairLockFileManager.repairLockFileGuid() == null);
    }


}
