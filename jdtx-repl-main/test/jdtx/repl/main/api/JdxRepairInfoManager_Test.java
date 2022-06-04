package jdtx.repl.main.api;

import jdtx.repl.main.api.mailer.*;
import jdtx.repl.main.api.repair.*;
import jdtx.repl.main.ut.*;
import org.junit.*;

/**
 *
 */
public class JdxRepairInfoManager_Test extends DbPrepareEtalon_Test {


    @Override
    public void setUp() throws Exception {
        super.setUp();
        db2.connect();
    }

    @Test
    public void setDataRepairInfo() throws Exception {
        JdxReplWs ws = new JdxReplWs(db2);
        ws.init();
        IMailer mailer = ws.getMailer();

        //
        System.out.println();

        //
        JdxRepairInfoManager repairInfoManager = new JdxRepairInfoManager(mailer);

        //
        String allowedRepairGuid = repairInfoManager.getAllowedRepairGuid();
        System.out.println("allowedRepairGuid: " + allowedRepairGuid);

        //
        System.out.println();


        //
        String guid_0 = (new RandomString(12345)).nextHexStr(16);
        String guid_1 = (new RandomString(67890)).nextHexStr(16);
        assertEquals(false, guid_1.equalsIgnoreCase(guid_0));

        //
        repairInfoManager.setRepairAllowed(guid_0);

        //
        allowedRepairGuid = repairInfoManager.getAllowedRepairGuid();
        System.out.println("allowedRepairGuid: " + allowedRepairGuid);
        assertEquals("allowedRepairGuid", guid_0, allowedRepairGuid);


        //
        repairInfoManager.setNoRepair();

        //
        allowedRepairGuid = repairInfoManager.getAllowedRepairGuid();
        System.out.println("allowedRepairGuid: " + allowedRepairGuid);
        assertEquals("allowedRepairGuid", null, allowedRepairGuid);


        //
        repairInfoManager.setRepairAllowed(guid_1);

        //
        allowedRepairGuid = repairInfoManager.getAllowedRepairGuid();
        System.out.println("allowedRepairGuid: " + allowedRepairGuid);
        assertEquals("allowedRepairGuid", guid_1, allowedRepairGuid);

        //
        System.out.println();
    }

}
