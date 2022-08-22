package jdtx.repl.main.api.struct;

import jdtx.repl.main.api.*;
import org.junit.*;

import java.util.*;

/**
 *
 */
public class JdxDbStruct_XmlRW_Test extends ReplDatabaseStruct_Test {


    @Override
    public void setUp() throws Exception {
        rootDir = "../../ext/";
        super.setUp();
    }

    @Test
    public void test_XmlRW() throws Exception {
        // структура подключенной БД - в файл
        JdxDbStruct_XmlRW struct_rw = new JdxDbStruct_XmlRW();
        struct_rw.toFile(struct, "../_test-data/dbStruct.xml");

        // проверим, что структура подключенной БД нетривиальна
        assertEquals(true, struct.getTables().size() > 5);
        assertEquals(true, struct.getTables().get(0).getFields().size() > 2);
        assertEquals(true, struct.getTables().get(1).getFields().size() > 2);

        // прочитаем из файла
        IJdxDbStruct structXml = struct_rw.read("../_test-data/dbStruct.xml");

        // проверим совпадение
        assertEquals(true, UtDbComparer.dbStructIsEqual(struct, structXml));
    }

    @Test
    // проверим совпадение struct, struct2
    public void test_diff() throws Exception {
        List<IJdxTable> tablesAdded = new ArrayList<>();
        List<IJdxTable> tablesRemoved = new ArrayList<>();
        List<IJdxTable> tablesChanged = new ArrayList<>();
        UtDbComparer.getStructDiff(struct, struct2, tablesAdded, tablesRemoved, tablesChanged);
        //
        System.out.println("tablesAdded: " + tablesAdded.size());
        System.out.println("tablesRemoved: " + tablesRemoved.size());
        System.out.println("tablesChanged: " + tablesChanged.size());
    }


}
