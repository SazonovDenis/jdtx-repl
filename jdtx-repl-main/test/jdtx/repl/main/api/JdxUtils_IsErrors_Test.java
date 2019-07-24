package jdtx.repl.main.api;

import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

public class JdxUtils_IsErrors_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test_createAudit_twice() throws Exception {
        UtDbObjectManager objectManager = new UtDbObjectManager(db, struct);
        IJdxTable table = struct.getTable("AppUpdate");
        //
        System.out.println("----------");
        System.out.println("objectManager.dropAudit");
        objectManager.dropAudit(table.getName());
        //
        System.out.println("----------");
        System.out.println("objectManager.createAudit");
        objectManager.createAuditTable(table);
        objectManager.createAuditTriggers(table);
        //
        System.out.println("----------");
        System.out.println("objectManager.createAudit");
        objectManager.createAuditTable(table);
        objectManager.createAuditTriggers(table);
        //
        System.out.println("----------");
        System.out.println("objectManager.dropAudit");
        objectManager.dropAudit(table.getName());
        //
        System.out.println("----------");
        System.out.println("objectManager.dropAudit");
        objectManager.dropAudit(table.getName());
    }

}
