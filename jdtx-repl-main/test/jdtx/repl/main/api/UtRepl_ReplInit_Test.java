package jdtx.repl.main.api;

import jandcode.dbm.data.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 * Создание/удаление репликационных структур
 */
public class UtRepl_ReplInit_Test extends Repl_TwoDatabase_Test {


    @Test
    public void test_db() throws Exception {
        DataStore st = dbm.getDb().loadSql("select id, orgName from dbInfo");
        dbm.outTable(st);
    }

    @Test
    public void test_Drop() throws Exception {
        UtRepl utr = new UtRepl(dbm.getDb());
        utr.dropReplication();
    }

    @Test
    public void test_Create() throws Exception {
        UtRepl utr = new UtRepl(dbm.getDb());
        utr.dropReplication();
        utr.createReplication();
    }

    @Test
    public void test_CreateDrop() throws Exception {
        UtRepl utr = new UtRepl(dbm.getDb());

        //
        JdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(dbm.getDb());
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
        utt.compareStruct(struct_1, struct_2);
    }


}
