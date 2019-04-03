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
    public void test_1() throws Exception {
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

    @Test
    public void test_2() throws Exception {
/*
        // структура подключенной БД - в файл
        UtDbStruct_RW struct_rw = new UtDbStruct_RW();
        struct_rw.write(struct, "../_test-data/dbStruct.xml");

        // прочитаем из файла
        IJdxDbStruct structXml = struct_rw.read("../_test-data/dbStruct.xml");

*/
        // проверим совпадение
        IJdxDbStruct structDiff0 = new JdxDbStruct();
        IJdxDbStruct structDiff1 = new JdxDbStruct();
        IJdxDbStruct structDiff2 = new JdxDbStruct();
        DbComparer.dbStructIsEqual(struct, struct2, structDiff0, structDiff1, structDiff2);
        //
        System.out.println(structDiff0.getTables().size());
        System.out.println(structDiff1.getTables().size());
        System.out.println(structDiff2.getTables().size());
        //
        UtDbStruct_RW struct_rw = new UtDbStruct_RW();
        //struct_rw.write(structDiff0, "../_test-data/structDiff0.xml");
        struct_rw.write(structDiff1, "../_test-data/structDiff1.xml");
        struct_rw.write(structDiff2, "../_test-data/structDiff2.xml");
    }

    @Test
    public void test_dbStructSave() throws Exception {
        IJdxDbStruct structDiff0 = new JdxDbStruct();
        IJdxDbStruct structDiff1 = new JdxDbStruct();
        IJdxDbStruct structDiff2 = new JdxDbStruct();

        //
        UtDbStruct_RW struct_rw = new UtDbStruct_RW();
        UtRepl ut = new UtRepl(db);


        // Сохраняем структуру в БД
        ut.dbStructSave(struct);

        // Читаем структуру из БД
        IJdxDbStruct structLoad = ut.dbStructLoad();

        // Сравниваем
        assertEquals(true, DbComparer.dbStructIsEqual(struct, structLoad, structDiff0, structDiff1, structDiff2));


        // Сохраняем структуру (через файл) в БД
        File file = new File("../_test-data/dbStruct.xml");
        struct_rw.write(struct, file.getPath());
        ut.dbStructSave(file);

        // Читаем структуру из БД
        IJdxDbStruct structLoad1 = ut.dbStructLoad();

        // Сравниваем
        assertEquals(true, DbComparer.dbStructIsEqual(struct, structLoad1, structDiff0, structDiff1, structDiff2));
    }


}
