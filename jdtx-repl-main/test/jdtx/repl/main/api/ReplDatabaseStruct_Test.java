package jdtx.repl.main.api;

import jdtx.repl.main.api.struct.*;
import org.junit.*;

import java.io.*;

/**
 */
public class ReplDatabaseStruct_Test extends ReplDatabase_Test {

    // Структуры
    IJdxDbStruct struct;
    IJdxDbStruct struct2;
    IJdxDbStruct struct3;

    public void setUp() throws Exception {
        //
        super.setUp();

        // Структуры db и db1
        IJdxDbStructReader reader = new JdxDbStructReader();
        reader.setDb(db);
        struct = reader.readDbStruct();
        reader.setDb(db2);
        struct2 = reader.readDbStruct();
        reader.setDb(db3);
        struct3 = reader.readDbStruct();
    }

    @Test
    public void test_XmlRW() throws Exception {
        // структура подключенной БД - в файл
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        struct_rw.write(struct, "../_test-data/dbStruct.xml");

        // проверим, что структура подключенной БД нетривиальна
        assertEquals(true, struct.getTables().size() > 5);
        assertEquals(true, struct.getTables().get(0).getFields().size() > 2);
        assertEquals(true, struct.getTables().get(1).getFields().size() > 2);

        // прочитаем из файла
        IJdxDbStruct structXml = struct_rw.read("../_test-data/dbStruct.xml");

        // проверим совпадение
        UtTest utTest = new UtTest(db);
        assertEquals(true, DbComparer.dbStructIsEqual(struct, structXml));
    }

    @Test
    // проверим совпадение struct, struct2
    public void test_diff() throws Exception {
        IJdxDbStruct structNoDiff = new JdxDbStruct();
        IJdxDbStruct structDiff1 = new JdxDbStruct();
        IJdxDbStruct structDiff2 = new JdxDbStruct();
        DbComparer.dbStructIsEqual(struct, struct2, structNoDiff, structDiff1, structDiff2);
        //
        System.out.println(structNoDiff.getTables().size());
        System.out.println(structDiff1.getTables().size());
        System.out.println(structDiff2.getTables().size());
        //
        UtDbStruct_XmlRW struct_rw = new UtDbStruct_XmlRW();
        struct_rw.write(structNoDiff, "../_test-data/structNoDiff.xml");
        struct_rw.write(structDiff1, "../_test-data/structDiff1.xml");
        struct_rw.write(structDiff2, "../_test-data/structDiff2.xml");
    }

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
        assertEquals(true, DbComparer.dbStructIsEqual(struct, structLoad, structNoDiff, structDiff1, structDiff2));


        // Сохраняем структуру (через файл) в БД
        File file = new File("../_test-data/dbStruct.xml");
        struct_rw.write(struct, file.getPath());
        utRepl.dbStructSave(file);

        // Читаем структуру из БД
        IJdxDbStruct structLoad1 = utRepl.dbStructLoad();

        // Сравниваем
        assertEquals(true, DbComparer.dbStructIsEqual(struct, structLoad1, structNoDiff, structDiff1, structDiff2));
    }


}
