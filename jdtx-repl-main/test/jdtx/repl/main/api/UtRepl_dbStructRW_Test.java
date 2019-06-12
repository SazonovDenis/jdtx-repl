package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;

/**
 */
public class UtRepl_dbStructRW_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_dbStruct_SaveLoad() throws Exception {
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        UtDbStruct_DbRW dbStructRW = new UtDbStruct_DbRW(db);


        // Сохраняем структуру в БД
        dbStructRW.dbStructSaveAllowed(struct);

        // Читаем структуру из БД
        IJdxDbStruct structLoad = dbStructRW.getDbStructAllowed();

        // Сравниваем
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structLoad));

        // Сохраняем структуру (через файл) в БД
        File file = new File("../_test-data/dbStruct.xml");
        struct_rw.saveToFile(struct, file.getPath());
        //
        IJdxDbStruct struct = struct_rw.read(file.getPath());
        //
        dbStructRW.dbStructSaveAllowed(struct);

        // Читаем структуру из БД
        IJdxDbStruct structLoad1 = dbStructRW.getDbStructAllowed();

        // Сравниваем
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structLoad1));
    }


}
