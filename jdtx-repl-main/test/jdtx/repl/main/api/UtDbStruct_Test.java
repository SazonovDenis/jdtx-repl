package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

public class UtDbStruct_Test extends ReplDatabase_Test {

    @Test
    public void test_1() throws Exception {
        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        IJdxDbStruct struct = reader.readDbStruct();

        // структура подключенной БД - в файл
        UtDbStruct_RW struct_rw = new UtDbStruct_RW();
        struct_rw.write(struct, "../_test-data/dbStruct.xml");

        // проверим, что структура подключенной БД нетривиальна
        assertEquals(true, struct.getTables().size() > 5);
        assertEquals(true, struct.getTables().get(0).getFields().size() > 2);
        assertEquals(true, struct.getTables().get(1).getFields().size() > 2);

        // прочитаем из файла
        IJdxDbStruct structXml = struct_rw.read("../_test-data/dbStruct.xml");

        // проверим совпадение
        UtTest utTest = new UtTest(db);
        utTest.compareStruct(struct, structXml);
    }

}
