package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;

/**
 */
public class UtRepl_dbStructRW_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_dbStruct_SaveLoad() throws Exception {
        IJdxDbStruct structNoDiff = new JdxDbStruct();
        IJdxDbStruct structDiff1 = new JdxDbStruct();
        IJdxDbStruct structDiff2 = new JdxDbStruct();

        //
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        UtRepl utRepl = new UtRepl(db, struct);


        // Сохраняем структуру в БД
        utRepl.dbStructSave(struct);

        // Читаем структуру из БД
        IJdxDbStruct structLoad = utRepl.dbStructLoad();

        // Сравниваем
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structLoad, structNoDiff, structDiff1, structDiff2));


        // Сохраняем структуру (через файл) в БД
        File file = new File("../_test-data/dbStruct.xml");
        struct_rw.saveToFile(struct, file.getPath());
        utRepl.dbStructSave(file);

        // Читаем структуру из БД
        IJdxDbStruct structLoad1 = utRepl.dbStructLoad();

        // Сравниваем
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structLoad1, structNoDiff, structDiff1, structDiff2));
    }


}
