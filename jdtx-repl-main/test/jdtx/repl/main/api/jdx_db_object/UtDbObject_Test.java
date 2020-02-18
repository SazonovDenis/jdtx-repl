package jdtx.repl.main.api.jdx_db_object;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

public class UtDbObject_Test extends Database_Test {


    @Test
    /**
     * Проверим неизменность структуры
     * после создания и последующего удаления репликационных структур
     */
    public void test_compareCreateDrop() throws Exception {
        JdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db1);
        IJdxDbStruct struct = dbStructReader.readDbStruct();
        UtRepl utRepl = new UtRepl(db1, struct);
        //
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();

        //
        IJdxDbStruct struct_1 = dbStructReader.readDbStruct(false);
        struct_rw.toFile(struct_1, "../_test-data/dbStruct_1.xml");

        //
        utRepl.createReplication(1, "");
        //
        utRepl.dropReplication();

        //
        IJdxDbStruct struct_2 = dbStructReader.readDbStruct(false);
        struct_rw.toFile(struct_2, "../_test-data/dbStruct_2.xml");

        // Проверим совпадение
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct_1, struct_2));
    }


}
