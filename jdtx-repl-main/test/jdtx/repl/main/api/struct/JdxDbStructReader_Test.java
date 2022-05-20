package jdtx.repl.main.api.struct;

import jdtx.repl.main.api.*;
import org.junit.*;

/**
 *
 */
public class JdxDbStructReader_Test extends DbPrepareEtalon_Test {


    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
        db.connect();
    }

    @Test
    public void test_compareCreateDrop() throws Exception {
        System.out.println("db: " + db.getDbSource().getDatabase());

        //
        try {
            db.execSql("drop table TEST_TAB");
        } catch (Exception e) {

        }

        //
        db.execSql("create table TEST_TAB (ID integer not null, NAME0 varchar(50), NAME1 varchar(50) not null, BLOB0 blob, BLOB1 blob not null)");

        //
        JdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db);
        IJdxDbStruct struct = dbStructReader.readDbStruct();

        //
        IJdxTable jdxTable = struct.getTable("TEST_TAB");

        assertEquals(true, jdxTable != null);

        assertEquals(5, jdxTable.getFields().size());

        assertEquals(true, jdxTable.getField("ID") != null);
        assertEquals(true, jdxTable.getField("NAME0") != null);
        assertEquals(true, jdxTable.getField("NAME1") != null);
        assertEquals(true, jdxTable.getField("BLOB0") != null);
        assertEquals(true, jdxTable.getField("BLOB1") != null);

        assertEquals(false, jdxTable.getField("ID").isNullable());
        assertEquals(true, jdxTable.getField("NAME0").isNullable());
        assertEquals(false, jdxTable.getField("NAME1").isNullable());
        assertEquals(true, jdxTable.getField("BLOB0").isNullable());
        assertEquals(false, jdxTable.getField("BLOB1").isNullable());

        //
        db.execSql("drop table TEST_TAB");
    }

}
