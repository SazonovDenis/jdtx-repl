package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;

/**
 */
public class UtDbStructApprover_XmlRW_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_dbStruct_SaveLoad() throws Exception {
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        UtDbStructApprover dbStructRW = new UtDbStructApprover(db);


        // Сохраняем структуру в БД
        dbStructRW.setDbStructAllowed(struct);

        // Читаем структуру из БД
        IJdxDbStruct structLoad = dbStructRW.getDbStructAllowed();

        // Сравниваем
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structLoad));

        // Сохраняем структуру (через файл) в БД
        File file = new File("../_test-data/dbStruct.xml");
        struct_rw.toFile(struct, file.getPath());
        //
        IJdxDbStruct struct = struct_rw.read(file.getPath());
        //
        dbStructRW.setDbStructAllowed(struct);

        // Читаем структуру из БД
        IJdxDbStruct structLoad1 = dbStructRW.getDbStructAllowed();

        // Сравниваем
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structLoad1));
    }


}
