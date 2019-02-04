package jdtx.repl.main.api;

import jandcode.dbm.test.*;
import jandcode.utils.*;
import jdtx.repl.main.api.struct.*;
import org.junit.*;

public class UtDbStruct_Test extends DbmTestCase {

    public void setUp() throws Exception {
        super.setUp();
        UtFile.mkdirs("temp");
    }

    @Test
    public void test_1() throws Exception {
        // чтение структуры
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(dbm.getDb());
        IJdxDbStruct struct = reader.readDbStruct();

        // структура подключенной БД - в файл
        UtDbStruct_RW struct_rw = new UtDbStruct_RW();
        struct_rw.write(struct, "temp/dbStruct.xml");

        // проверим, что структура подключенной БД нетривиальна
        assertEquals(true, struct.getTables().size() > 5);
        assertEquals(true, struct.getTables().get(0).getFields().size() > 2);
        assertEquals(true, struct.getTables().get(1).getFields().size() > 2);

        // прочитаем из файла
        IJdxDbStruct structXml = struct_rw.read("temp/dbStruct.xml");

        // проверим совпадение
        (new UtStructTest()).compareStruct(struct, structXml);
    }

}
