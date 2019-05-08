package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

/**
 */
public class UtDbStruct_XmlRW_Test extends ReplDatabaseStruct_Test {


    @Test
    public void test_XmlRW() throws Exception {
        // структура подключенной БД - в файл
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        struct_rw.saveToFile(struct, "../_test-data/dbStruct.xml");

        // проверим, что структура подключенной БД нетривиальна
        assertEquals(true, struct.getTables().size() > 5);
        assertEquals(true, struct.getTables().get(0).getFields().size() > 2);
        assertEquals(true, struct.getTables().get(1).getFields().size() > 2);

        // прочитаем из файла
        IJdxDbStruct structXml = struct_rw.read("../_test-data/dbStruct.xml");

        // проверим совпадение
        UtTest utTest = new UtTest(db);
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structXml));
    }

    @Test
    // проверим совпадение struct, struct2
    public void test_diff() throws Exception {
        IJdxDbStruct structNoDiff = new JdxDbStruct();
        IJdxDbStruct structDiff1 = new JdxDbStruct();
        IJdxDbStruct structDiff2 = new JdxDbStruct();
        UtDbComparer.dbStructIsEqual(struct, struct2, structNoDiff, structDiff1, structDiff2);
        //
        System.out.println(structNoDiff.getTables().size());
        System.out.println(structDiff1.getTables().size());
        System.out.println(structDiff2.getTables().size());
        //
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        struct_rw.saveToFile(structNoDiff, "../_test-data/structNoDiff.xml");
        struct_rw.saveToFile(structDiff1, "../_test-data/structDiff1.xml");
        struct_rw.saveToFile(structDiff2, "../_test-data/structDiff2.xml");
    }


}
