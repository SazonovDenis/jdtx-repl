package jdtx.repl.main.api;

import jdtx.repl.main.api.audit.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

public class UtJdx_IsErrors_Test extends ReplDatabaseStruct_Test {

    @Test
    public void test_createAudit_twice() throws Exception {
        UtDbObjectManager objectManager = new UtDbObjectManager(db);
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

    @Test
    public void test_ForeignKeyViolation_Info() {
        Exception e = new Exception("violation of FOREIGN KEY constraint \"FK_LIC_ULZ\" on table \"LIC\"");
        JdxForeignKeyViolationException ee = new JdxForeignKeyViolationException(e);
        //
        IJdxTable thisTable = UtJdx.get_ForeignKeyViolation_tableInfo(ee, struct);
        IJdxForeignKey foreignKey = UtJdx.get_ForeignKeyViolation_refInfo(ee, struct);
        IJdxField refField = foreignKey.getField();
        IJdxTable refTable = refField.getRefTable();
        //
        String thisTableName = thisTable.getName();
        String thisTableRefFieldName = refField.getName();
        //
        String refTableName = refTable.getName();
        String refTableFieldName = foreignKey.getTableField().getName();
        //
        System.out.println("Foreign key: " + thisTableName + "." + thisTableRefFieldName + " -> " + refTableName + "." + refTableFieldName);

    }
}
