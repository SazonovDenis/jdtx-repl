package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 * Создание/удаление репликационных структур
 */
public class UtRepl_ReplInit_Test extends ReplDatabase_Test {


    @Test
    public void test_db() throws Exception {
        // db
        DataStore st = db.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st);
        // db1
        DataStore st1 = db1.loadSql("select id, orgName, dbLabel from dbInfo");
        UtData.outTable(st1);
    }

    @Test
    public void test_Drop() throws Exception {
        // db
        UtRepl utr = new UtRepl(db);
        utr.dropReplication();
        // db1
        UtRepl utr1 = new UtRepl(db1);
        utr1.dropReplication();
    }

    @Test
    public void test_Create() throws Exception {
        // db
        UtRepl utr = new UtRepl(db);
        utr.dropReplication();
        utr.createReplication();
        // db1
        UtRepl utr1 = new UtRepl(db1);
        utr1.dropReplication();
        utr1.createReplication();
    }

    @Test
    public void test_compareCreateDrop() throws Exception {
        UtRepl utr = new UtRepl(db);

        //
        JdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        UtDbStruct_RW struct_rw = new UtDbStruct_RW();

        //
        IJdxDbStruct struct_1 = reader.readDbStruct(false);
        struct_rw.write(struct_1, "temp/dbStruct_1.xml");

        //
        utr.createReplication();
        //
        utr.dropReplication();

        //
        IJdxDbStruct struct_2 = reader.readDbStruct(false);
        struct_rw.write(struct_2, "temp/dbStruct_2.xml");

        // Проверим совпадение
        utTest.compareStruct(struct_1, struct_2);
    }


}
