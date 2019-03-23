package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 * Создание/удаление репликационных структур
 */
public class UtRepl_ReplInit_Test extends ReplDatabase_Test {


    @Test
    /**
     * Проверим неизменность структуры
     * после создания и последующего удаления репликационных структур
     */
    public void test_compareCreateDrop() throws Exception {
        UtRepl utr = new UtRepl(db);

        //
        JdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        UtDbStruct_RW struct_rw = new UtDbStruct_RW();

        //
        IJdxDbStruct struct_1 = reader.readDbStruct(false);
        struct_rw.write(struct_1, "../_test-data/dbStruct_1.xml");

        //
        utr.createReplication(1, "");
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
