package jdtx.repl.main.api.jdx_db_object;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import jdtx.repl.main.api.util.*;
import org.junit.*;

public class UtDbObject_Test extends Database_Test {

    @Test
    public void test_db() throws Exception {
        System.out.println(UtJdx.getDbInfoStr(db1));

        //
        JdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db1);
        IJdxDbStruct struct = dbStructReader.readDbStruct();
        System.out.println("Таблиц в базе: " + struct.getTables().size());

        //
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        struct_rw.toFile(struct, "../_test-data/dbStruct.xml");
    }

    @Test
    public void test_dropReplication() throws Exception {
        System.out.println(UtJdx.getDbInfoStr(db1));

        //
        JdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db1);
        IJdxDbStruct struct = dbStructReader.readDbStruct();
        System.out.println("Таблиц в базе: " + struct.getTables().size());

        //
        UtRepl utRepl = new UtRepl(db1, struct);

        //
        System.out.println("dropReplication");
        utRepl.dropReplication();
    }

    @Test
    public void test_createReplication() throws Exception {
        System.out.println(UtJdx.getDbInfoStr(db1));

        //
        JdxDbStructReader dbStructReader = new JdxDbStructReader();
        dbStructReader.setDb(db1);
        IJdxDbStruct struct = dbStructReader.readDbStruct();
        System.out.println("Таблиц в базе: " + struct.getTables().size());

        //
        UtRepl utRepl = new UtRepl(db1, struct);

        //
        System.out.println("dropReplication");
        utRepl.dropReplication();

        //
        System.out.println("createReplication");
        utRepl.createReplication(1, "test");
    }

    /**
     * Проверим неизменность структуры
     * после создания и последующего удаления репликационных структур
     */
    @Test
    public void test_compareCreateDrop() throws Exception {
        System.out.println(UtJdx.getDbInfoStr(db1));

        //
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
