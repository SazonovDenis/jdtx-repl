package jdtx.repl.main.api.repair;

import jandcode.utils.*;
import jandcode.utils.error.*;
import jdtx.repl.main.api.util.*;
import junit.framework.*;
import org.joda.time.*;
import org.junit.Test;

/**
 *
 */
public class JdxReplWs_repairLockFile_Test extends TestCase {


    @Test
    public void test_repairLockFile() throws Exception {
        JdxRepairLockFileManager repairLockFileManager = new JdxRepairLockFileManager("temp/");

        //
        repairLockFileManager.repairLockFileDelete();
        assertEquals(false, repairLockFileManager.getRepairLockFile().exists());

        //
        System.out.println("repairLockFile Create");

        //
        repairLockFileManager.repairLockFileCreate(null);
        assertEquals(true, repairLockFileManager.getRepairLockFile().exists());

        //
        System.out.println("repairLockFileStr: " + repairLockFileManager.repairLockFileStr());
        System.out.println("repairLockFileGuid: " + repairLockFileManager.repairLockFileGuid());
        assertEquals(true, repairLockFileManager.repairLockFileStr() != null);
        assertEquals(true, repairLockFileManager.repairLockFileGuid() != null);
        assertEquals(true, repairLockFileManager.repairLockFileStr().contains(new DateTime().toString("YYYY-MM-dd")));
        assertEquals(true, repairLockFileManager.repairLockFileStr().contains(repairLockFileManager.repairLockFileGuid()));

        //
        try {
            repairLockFileManager.repairLockFileCreate(null);
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
        System.out.println("repairLockFileStr: " + repairLockFileManager.repairLockFileStr());
        System.out.println("repairLockFileGuid: " + repairLockFileManager.repairLockFileGuid());
        assertEquals(true, repairLockFileManager.repairLockFileStr() == null);
        assertEquals(true, repairLockFileManager.repairLockFileGuid() == null);
    }


    @Test
    public void test_repairLockFile_Params() throws Exception {
        JdxRepairLockFileManager repairLockFileManager = new JdxRepairLockFileManager("temp/");

        //
        repairLockFileManager.repairLockFileDelete();
        assertEquals(false, repairLockFileManager.getRepairLockFile().exists());

        //
        System.out.println("repairLockFile Create");

        //
        repairLockFileManager.repairLockFileCreate(UtCnv.toMap("xx", 1234, "yy", true, "zz", "qaz"));
        assertEquals(true, repairLockFileManager.getRepairLockFile().exists());

        //
        System.out.println("repairLockFileStr: " + repairLockFileManager.repairLockFileStr());
        System.out.println("repairLockFileGuid: " + repairLockFileManager.repairLockFileGuid());
        System.out.println("repairLockFileMap: " + repairLockFileManager.repairLockFileMap());
        assertEquals(true, repairLockFileManager.repairLockFileStr() != null);
        assertEquals(true, repairLockFileManager.repairLockFileGuid() != null);
        assertEquals(true, repairLockFileManager.repairLockFileStr().contains(new DateTime().toString("YYYY-MM-dd")));
        assertEquals(true, repairLockFileManager.repairLockFileStr().contains(repairLockFileManager.repairLockFileGuid()));
        assertEquals(true, repairLockFileManager.repairLockFileMap().containsKey("xx"));
        assertEquals(true, repairLockFileManager.repairLockFileMap().containsKey("yy"));
        assertEquals(true, repairLockFileManager.repairLockFileMap().containsKey("zz"));
        assertEquals("1234", repairLockFileManager.repairLockFileMap().get("xx"));
        assertEquals("true", repairLockFileManager.repairLockFileMap().get("yy"));
        assertEquals("qaz", repairLockFileManager.repairLockFileMap().get("zz"));
        assertEquals(false, repairLockFileManager.repairLockFileMap().containsKey("aa"));
        assertEquals(null, repairLockFileManager.repairLockFileMap().get("aa"));


        //
        System.out.println("repairLockFile Delete");
        repairLockFileManager.repairLockFileDelete();

        //
        assertEquals(false, repairLockFileManager.getRepairLockFile().exists());
        System.out.println("repairLockFileStr: " + repairLockFileManager.repairLockFileStr());
        System.out.println("repairLockFileGuid: " + repairLockFileManager.repairLockFileGuid());
        System.out.println("repairLockFileMap: " + repairLockFileManager.repairLockFileMap());
        assertEquals(true, repairLockFileManager.repairLockFileStr() == null);
        assertEquals(true, repairLockFileManager.repairLockFileGuid() == null);
        assertEquals(true, repairLockFileManager.repairLockFileMap() == null);
    }


}
