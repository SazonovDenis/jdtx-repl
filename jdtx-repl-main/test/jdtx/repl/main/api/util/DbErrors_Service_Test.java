package jdtx.repl.main.api.util;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.audit.*;
import jdtx.repl.main.api.jdx_db_object.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

public class DbErrors_Service_Test extends ReplDatabaseStruct_Test {

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_ForeignKeyViolation_Info() {
        System.out.println(UtJdx.getDbInfoStr(db));
        IDbErrors dbErrors = db.service(DbErrorsService.class);
        //
        Exception e = new Exception("violation of FOREIGN KEY constraint \"FK_LIC_ULZ\" on table \"LIC\"");
        JdxForeignKeyViolationException ee = new JdxForeignKeyViolationException(e);
        //
        IJdxTable thisTable = dbErrors.get_ForeignKeyViolation_tableInfo(ee, struct);
        IJdxForeignKey foreignKey = dbErrors.get_ForeignKeyViolation_refInfo(ee, struct);
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
