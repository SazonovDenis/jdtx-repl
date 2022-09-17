package jdtx.repl.main.api.jdx_db_object;

import jandcode.dbm.db.*;
import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.junit.*;

public class DbObjectManagerService_Test extends ReplDatabaseStruct_Test {

    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_createAudit_Firebird() throws Exception {
        test_createAudit_twice(db, struct);
    }

    @Test
    public void test_createAudit_Oracle() throws Exception {
        test_createAudit_twice(db_one, struct_one);
    }

    void test_createAudit_twice(Db db, IJdxDbStruct struct) throws Exception {
        System.out.println(UtJdx.getDbInfoStr(db));
        IDbObjectManager dbObjectManager = db.service(DbObjectManager.class);

        //
        dbObjectManager.dropReplBase();
        dbObjectManager.createReplBase(1, "-");

        //
        IJdxTable table = struct.getTables().get(0);
        System.out.println("table: " + table.getName());
        //
        System.out.println("----------");
        System.out.println("dbObjectManager.dropAudit");
        dbObjectManager.dropAudit(table.getName());
        //
        System.out.println("----------");
        System.out.println("dbObjectManager.createAudit");
        dbObjectManager.createAudit(table);
        //
        System.out.println("----------");
        System.out.println("dbObjectManager.createAudit");
        dbObjectManager.createAudit(table);
        //
        System.out.println("----------");
        System.out.println("dbObjectManager.dropAudit");
        dbObjectManager.dropAudit(table.getName());
        //
        System.out.println("----------");
        System.out.println("dbObjectManager.dropAudit");
        dbObjectManager.dropAudit(table.getName());
    }

}
