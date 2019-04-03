package jdtx.repl.main.api.jdx_db_object;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 * Создание/удаление репликационных структур
 */
public class UtDbObject_Test extends ReplDatabase_Test {


    @Test
    /**
     * Проверим неизменность структуры
     * после создания и последующего удаления репликационных структур
     */
    public void test_compareCreateDrop() throws Exception {
        JdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db);
        IJdxDbStruct struct = dbStructReader.readDbStruct();
        UtRepl utRepl = new UtRepl(db, struct);
        //
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();

        //
        IJdxDbStruct struct_1 = dbStructReader.readDbStruct(false);
        struct_rw.write(struct_1, "../_test-data/dbStruct_1.xml");

        //
        utRepl.createReplication(1, "");
        //
        utRepl.dropReplication();

        //
        IJdxDbStruct struct_2 = dbStructReader.readDbStruct(false);
        struct_rw.write(struct_2, "../_test-data/dbStruct_2.xml");

        // Проверим совпадение
        UtTest utTest = new UtTest(db);
        utTest.compareStruct(struct_1, struct_2);
    }


}
