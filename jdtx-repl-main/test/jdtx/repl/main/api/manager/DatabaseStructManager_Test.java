package jdtx.repl.main.api.manager;

import jdtx.repl.main.api.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;

/**
 */
public class DatabaseStructManager_Test extends ReplDatabaseStruct_Test {


    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_dbStruct_SaveLoad() throws Exception {
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        DatabaseStructManager databaseStructManager = new DatabaseStructManager(db);


        // Сохраняем структуру в БД
        databaseStructManager.setDbStructAllowed(struct);

        // Читаем структуру из БД
        IJdxDbStruct structLoad = databaseStructManager.getDbStructAllowed();

        // Сравниваем
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structLoad));

        // Сохраняем структуру (через файл) в БД
        File file = new File("../_test-data/dbStruct.xml");
        struct_rw.toFile(struct, file.getPath());
        //
        IJdxDbStruct struct = struct_rw.read(file.getPath());
        //
        databaseStructManager.setDbStructAllowed(struct);

        // Читаем структуру из БД
        IJdxDbStruct structLoad1 = databaseStructManager.getDbStructAllowed();

        // Сравниваем
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structLoad1));
    }


}
