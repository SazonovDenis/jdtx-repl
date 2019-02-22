package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 * Создание/удаление репликационных структур
 */
public class UtRepl_ReplInit_Test extends ReplDatabase_Test {


    @Test
    public void test_DropReplication() throws Exception {
        // db
        UtRepl utr = new UtRepl(db, 1);
        utr.dropReplication();
        // db1
        UtRepl utr1 = new UtRepl(db2, 2);
        utr1.dropReplication();
        // db2
        UtRepl utr2 = new UtRepl(db3, 3);
        utr2.dropReplication();
    }

    @Test
    public void test_CreateReplication() throws Exception {
        // db
        UtRepl utr = new UtRepl(db, 1);
        utr.dropReplication();
        utr.createReplication();
        // db1
        UtRepl utr1 = new UtRepl(db2, 2);
        utr1.dropReplication();
        utr1.createReplication();
        // db2
        UtRepl utr2 = new UtRepl(db3, 3);
        utr2.dropReplication();
        utr2.createReplication();
    }

    @Test
    /**
     * Проверим неизменность структуры
     * после создания и последующего удаления репликационных структур
     */
    public void test_compareCreateDrop() throws Exception {
        UtRepl utr = new UtRepl(db, 1);

        //
        JdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        UtDbStruct_RW struct_rw = new UtDbStruct_RW();

        //
        IJdxDbStruct struct_1 = reader.readDbStruct(false);
        struct_rw.write(struct_1, "../_test-data/dbStruct_1.xml");

        //
        utr.createReplication();
        //
        utr.dropReplication();

        //
        IJdxDbStruct struct_2 = reader.readDbStruct(false);
        struct_rw.write(struct_2, "../_test-data/dbStruct_2.xml");

        // Проверим совпадение
        UtTest utTest = new UtTest(db);
        utTest.compareStruct(struct_1, struct_2);
    }


}
