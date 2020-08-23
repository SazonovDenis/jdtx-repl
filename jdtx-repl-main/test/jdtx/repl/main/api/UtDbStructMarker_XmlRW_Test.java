package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;

/**
 */
public class UtDbStructMarker_XmlRW_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_dbStruct_SaveLoad() throws Exception {
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        UtDbStructMarker utDbStructMarker = new UtDbStructMarker(db);


        // Сохраняем структуру в БД
        utDbStructMarker.setDbStructAllowed(struct);

        // Читаем структуру из БД
        IJdxDbStruct structLoad = utDbStructMarker.getDbStructAllowed();

        // Сравниваем
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structLoad));

        // Сохраняем структуру (через файл) в БД
        File file = new File("../_test-data/dbStruct.xml");
        struct_rw.toFile(struct, file.getPath());
        //
        IJdxDbStruct struct = struct_rw.read(file.getPath());
        //
        utDbStructMarker.setDbStructAllowed(struct);

        // Читаем структуру из БД
        IJdxDbStruct structLoad1 = utDbStructMarker.getDbStructAllowed();

        // Сравниваем
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structLoad1));
    }


}
